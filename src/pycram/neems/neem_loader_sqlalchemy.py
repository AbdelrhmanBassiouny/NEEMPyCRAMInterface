import logging

import pandas as pd
from sqlalchemy import create_engine, Engine, between, and_, func, Table
from sqlalchemy.orm import sessionmaker, aliased, InstrumentedAttribute, Query
from typing_extensions import Optional, List

from .neems_database import *

TaskType = aliased(RdfType)
ParticipantType = aliased(RdfType)
Task = aliased(DulExecutesTask)
HasParticipant = aliased(DulHasParticipant)
HasBaseLink = aliased(UrdfHasBaseLink)
HasTimeInterval = aliased(DulHasTimeInterval)
HasIntervalBegin = aliased(SomaHasIntervalBegin)
HasIntervalEnd = aliased(SomaHasIntervalEnd)
Environment = aliased(NeemsEnvironmentIndex)
Translation = aliased(TransformTranslation)
Rotation = aliased(TransformRotation)
Transform = aliased(TfTransform)


class NeemLoader:
    engine: Engine
    session: 'Session'

    def __init__(self, sql_uri: str):
        self.engine = create_engine(sql_uri)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_task(self, task: str) -> Task:
        """
        Get the task from the database
        :param task: the task name.
        :return: the task.
        """
        return self.session.query(Task).filter(Task.dul_Task_o == task).first()

    @staticmethod
    def get_column_names(table: Table) -> List[str]:
        """
        Get the column names of a table
        :param table: the table.
        :return: the column names.
        """
        return [col.name for col in table.columns]

    def neem_cond(self, col1: InstrumentedAttribute, col2: InstrumentedAttribute, default_neem_id=None):
        if not isinstance(col1, InstrumentedAttribute) and not isinstance(col2, InstrumentedAttribute):
            logging.warning("col1 and col2 should be instances of Column")
            return col1 == col2
        elif not isinstance(col2, InstrumentedAttribute):
            if "neem_id" in self.get_column_names(col1.table) and default_neem_id is not None:
                return (col1 == col2) & (col1.table.columns.neem_id == default_neem_id)
            else:
                return col1 == col2
        elif not isinstance(col1, InstrumentedAttribute):
            if "neem_id" in self.get_column_names(col2.table) and default_neem_id is not None:
                return (col1 == col2) & (col2.table.columns.neem_id == default_neem_id)
            else:
                return col1 == col2

        table1_names = self.get_column_names(col1.table)
        table2_names = self.get_column_names(col2.table)

        if "neem_id" not in table1_names and "neem_id" not in table2_names:
            logging.warning("neem_id not found in the columns")
            return col1 == col2
        elif "neem_id" not in table1_names and default_neem_id is not None:
            return (col1 == col2) & (col2.table.columns.neem_id == default_neem_id)
        elif "neem_id" not in table2_names and default_neem_id is not None:
            return (col1 == col2) & (col1.table.columns.neem_id == default_neem_id)
        else:
            return (col1 == col2) & (col1.table.columns.neem_id == col2.table.columns.neem_id)

    def has_neem_id(self, table: InstrumentedAttribute) -> bool:
        """
        Check if a table has a neem_id column
        :param table: the table.
        :return: True if the table has a neem_id column, False otherwise.
        """
        return "neem_id" in self.get_column_names(table)

    def get_neem_id_conditions(self, tables: List[Table], neem_id: InstrumentedAttribute) -> Query:
        """
        Get the neem_id conditions of a list of tables.
        :param tables: the list of tables.
        :param neem_id: the neem_id column.
        """
        cond = None
        for table in tables:
            if self.has_neem_id(table.ID.table):
                if cond is None:
                    cond = table.neem_id == neem_id
                else:
                    cond = and_(cond, table.neem_id == neem_id)
        return cond

    # noinspection PyTypeChecker
    def get_task_data(self, task: str, use_regex: Optional[bool] = False,
                      order_by: Optional[Column] = TfHeader.stamp) -> pd.DataFrame:
        """
        Get the data of a certain task from the database
        :param task: the task name.
        :param use_regex: whether to use regex to filter the task name or not.
        :param order_by: the column to order by.
        :return: the data of the task.
        """
        from_tables = [TaskType, HasParticipant, ParticipantType, HasBaseLink, HasTimeInterval, HasIntervalBegin,
                       HasIntervalEnd, Tf, TfHeader, Translation, Rotation, Neem, Environment]
        neem_conditions = self.get_neem_id_conditions(from_tables, Task.neem_id)
        join_conditions = [TaskType.s == Task.dul_Task_o,
                           HasParticipant.dul_Event_s == Task.dul_Action_s,
                           ParticipantType.s == HasParticipant.dul_Object_o,
                           HasBaseLink.dul_PhysicalObject_s == HasParticipant.dul_Object_o,
                           HasTimeInterval.dul_Event_s == Task.dul_Action_s,
                           HasIntervalBegin.dul_TimeInterval_s == HasTimeInterval.dul_TimeInterval_o,
                           HasIntervalEnd.dul_TimeInterval_s == HasIntervalBegin.dul_TimeInterval_s,
                           Tf.header == TfHeader.ID,
                           Transform.ID == Tf.ID,
                           Rotation.ID == Transform.rotation,
                           Translation.ID == Transform.translation,
                           Neem._id == Task.neem_id,
                           Environment.neems_ID == Neem.ID,
                           Tf.child_frame_id == func.substring_index(HasBaseLink.urdf_Link_o, ':', -1),
                           TaskType.o.like(f"%{task}%") if use_regex else TaskType.o == task,
                           ParticipantType.o != 'owl:NamedIndividual',
                           between(TfHeader.stamp, HasIntervalBegin.o - 40, HasIntervalEnd.o),
                           *neem_conditions]

        query = self.session.query(
            Task.dul_Task_o,
            TaskType.o,
            Task.neem_id,
            Environment.environment_values,
            HasParticipant.dul_Object_o,
            ParticipantType.o,
            Tf.child_frame_id,
            TfHeader.frame_id,
            HasIntervalBegin.o,
            TfHeader.stamp,
            HasIntervalEnd.o,
            Translation.x,
            Translation.y,
            Translation.z,
            Rotation.x,
            Rotation.y,
            Rotation.z,
            Rotation.w,
            HasBaseLink.urdf_Link_o).distinct().where(and_(*join_conditions)).order_by(order_by)

        df = pd.read_sql(query.statement, self.engine)
        return df

    def get_task_data_using_joins(self, task: str, use_regexp: Optional[bool] = False,
                                  order_by: Optional[Column] = TfHeader.stamp) -> pd.DataFrame:
        """
        Get the data of a certain task from the database using joins
        :param task: the task name.
        :param use_regexp: whether to use regex to filter the task name or not.
        :param order_by: the column to order by.
        :return: the data of the task.
        """
        joins = {TaskType: and_(TaskType.s == Task.dul_Task_o,
                                TaskType.o.like(f"%{task}%") if use_regexp else TaskType.o == task),
                 HasParticipant: HasParticipant.dul_Event_s == Task.dul_Action_s,
                 ParticipantType: and_(ParticipantType.s == HasParticipant.dul_Object_o,
                                       ParticipantType.o != 'owl:NamedIndividual'),
                 HasBaseLink: HasBaseLink.dul_PhysicalObject_s == HasParticipant.dul_Object_o,
                 HasTimeInterval: HasTimeInterval.dul_Event_s == Task.dul_Action_s,
                 HasIntervalBegin: HasIntervalBegin.dul_TimeInterval_s == HasTimeInterval.dul_TimeInterval_o,
                 HasIntervalEnd: HasIntervalEnd.dul_TimeInterval_s == HasIntervalBegin.dul_TimeInterval_s,
                 TfHeader: between(TfHeader.stamp, HasIntervalBegin.o - 40, HasIntervalEnd.o),
                 Tf: and_(Tf.header == TfHeader.ID,
                          Tf.child_frame_id == func.substring_index(HasBaseLink.urdf_Link_o, ':', -1)),
                 Transform: Transform.ID == Tf.ID,
                 Rotation: Rotation.ID == Transform.rotation,
                 Translation: Translation.ID == Transform.translation,
                 Neem: Neem._id == Task.neem_id,
                 Environment: Environment.neems_ID == Neem.ID}

        neem_id_conditions = self.get_neem_id_conditions(list(joins.keys()), Task.neem_id)
        query = self.session.query(
            Task.dul_Task_o,
            TaskType.o,
            Task.neem_id,
            Environment.environment_values,
            HasParticipant.dul_Object_o,
            ParticipantType.o,
            Tf.child_frame_id,
            TfHeader.frame_id,
            HasIntervalBegin.o,
            TfHeader.stamp,
            HasIntervalEnd.o,
            Translation.x,
            Translation.y,
            Translation.z,
            Rotation.x,
            Rotation.y,
            Rotation.z,
            Rotation.w,
            HasBaseLink.urdf_Link_o).distinct()

        for table, on in joins.items():
            query = query.join(table, on)
        query = query.filter(*neem_id_conditions).order_by(order_by)

        df = pd.read_sql(query.statement, self.engine)
        return df
