import logging

import pandas as pd
from sqlalchemy import create_engine, Engine, between, and_, func, Table, BinaryExpression, select, Select, not_
from sqlalchemy.orm import sessionmaker, aliased, InstrumentedAttribute
from typing_extensions import Optional, List, Dict

from .neems_database import *

TaskType = aliased(RdfType)
ParticipantType = aliased(RdfType)


class NeemLoader:
    engine: Engine
    session: 'Session'

    def __init__(self, sql_uri: str):
        self.engine = create_engine(sql_uri)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self.query: Optional[Select] = None
        self.selected_columns: Optional[List[InstrumentedAttribute]] = []
        self.joins: Optional[Dict[Table, BinaryExpression]] = {}
        self.in_filters: Optional[Dict[Column, List]] = {}
        self.remove_filters: Optional[Dict[Column, List]] = {}

    def get_task(self, task: str) -> DulExecutesTask:
        """
        Get the task from the database
        :param task: the task name.
        :return: the task.
        """
        return self.session.query(DulExecutesTask).filter(DulExecutesTask.dul_Task_o == task).first()

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

    def get_neem_id_conditions(self, tables: List[Table], neem_id: InstrumentedAttribute) -> BinaryExpression:
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
        from_tables = [TaskType, DulHasParticipant, ParticipantType, UrdfHasBaseLink, DulHasTimeInterval,
                       SomaHasIntervalBegin,
                       SomaHasIntervalEnd, Tf, TfHeader, TransformTranslation, TransformRotation, Neem,
                       NeemsEnvironmentIndex]
        neem_conditions = self.get_neem_id_conditions(from_tables, DulExecutesTask.neem_id)
        join_conditions = [TaskType.s == DulExecutesTask.dul_Task_o,
                           DulHasParticipant.dul_Event_s == DulExecutesTask.dul_Action_s,
                           ParticipantType.s == DulHasParticipant.dul_Object_o,
                           UrdfHasBaseLink.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                           DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                           SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                           SomaHasIntervalEnd.dul_TimeInterval_s == SomaHasIntervalBegin.dul_TimeInterval_s,
                           Tf.header == TfHeader.ID,
                           TfTransform.ID == Tf.ID,
                           TransformRotation.ID == TfTransform.rotation,
                           TransformTranslation.ID == TfTransform.translation,
                           Neem._id == DulExecutesTask.neem_id,
                           NeemsEnvironmentIndex.neems_ID == Neem.ID,
                           Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1),
                           TaskType.o.like(f"%{task}%") if use_regex else TaskType.o == task,
                           ParticipantType.o != 'owl:NamedIndividual',
                           between(TfHeader.stamp, SomaHasIntervalBegin.o - 40, SomaHasIntervalEnd.o),
                           *neem_conditions]

        query = self.session.query(
            DulExecutesTask.dul_Task_o,
            TaskType.o,
            DulExecutesTask.neem_id,
            NeemsEnvironmentIndex.environment_values,
            DulHasParticipant.dul_Object_o,
            ParticipantType.o,
            Tf.child_frame_id,
            TfHeader.frame_id,
            SomaHasIntervalBegin.o,
            TfHeader.stamp,
            SomaHasIntervalEnd.o,
            TransformTranslation.x,
            TransformTranslation.y,
            TransformTranslation.z,
            TransformRotation.x,
            TransformRotation.y,
            TransformRotation.z,
            TransformRotation.w,
            UrdfHasBaseLink.urdf_Link_o).distinct().where(and_(*join_conditions)).order_by(order_by)

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
        joins = {TaskType: TaskType.s == DulExecutesTask.dul_Task_o,
                 DulHasParticipant: DulHasParticipant.dul_Event_s == DulExecutesTask.dul_Action_s,
                 ParticipantType: ParticipantType.s == DulHasParticipant.dul_Object_o,
                 UrdfHasBaseLink: UrdfHasBaseLink.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                 DulHasTimeInterval: DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                 SomaHasIntervalBegin: SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                 SomaHasIntervalEnd: SomaHasIntervalEnd.dul_TimeInterval_s == SomaHasIntervalBegin.dul_TimeInterval_s,
                 TfHeader: between(TfHeader.stamp, SomaHasIntervalBegin.o - 40, SomaHasIntervalEnd.o),
                 Tf: and_(Tf.header == TfHeader.ID,
                          Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1)),
                 TfTransform: TfTransform.ID == Tf.ID,
                 TransformRotation: TransformRotation.ID == TfTransform.rotation,
                 TransformTranslation: TransformTranslation.ID == TfTransform.translation,
                 Neem: Neem._id == DulExecutesTask.neem_id,
                 NeemsEnvironmentIndex: NeemsEnvironmentIndex.neems_ID == Neem.ID}

        neem_id_conditions = self.get_neem_id_conditions(list(joins.keys()), DulExecutesTask.neem_id)
        filtering_conditions = [TaskType.o.like(f"%{task}%") if use_regexp else TaskType.o == task,
                                ParticipantType.o != 'owl:NamedIndividual']

        query = self.session.query(
            DulExecutesTask.dul_Task_o,
            TaskType.o,
            DulExecutesTask.neem_id,
            NeemsEnvironmentIndex.environment_values,
            DulHasParticipant.dul_Object_o,
            ParticipantType.o,
            Tf.child_frame_id,
            TfHeader.frame_id,
            SomaHasIntervalBegin.o,
            TfHeader.stamp,
            SomaHasIntervalEnd.o,
            TransformTranslation.x,
            TransformTranslation.y,
            TransformTranslation.z,
            TransformRotation.x,
            TransformRotation.y,
            TransformRotation.z,
            TransformRotation.w,
            UrdfHasBaseLink.urdf_Link_o).distinct()

        for table, on in joins.items():
            query = query.join(table, on)
        query = query.filter(*filtering_conditions, *neem_id_conditions).order_by(order_by)

        df = pd.read_sql(query.statement, self.engine)
        return df

    @property
    def statement(self) -> Select:
        """
        Get the query statement.
        :return: the query statement.
        """
        self.query = select(*self.selected_columns)
        for table, on in self.joins.items():
            self.query = self.query.join(table, on)
        return self.filter(self.in_filters, self.remove_filters)

    def join_task_participants(self, participants: Optional[List[str]] = None,
                               remove_participants: Optional[List[str]] = None,
                               select_task: Optional[bool] = False,
                               select_columns: Optional[bool] = True) -> 'NeemLoader':
        """
        Add task participant_types to the query,
        Assumes tasks have been joined/selected already if select_task is False,
         else will select tasks first.
        :param participants: the participant_types.
        :param remove_participants: the participant_types to remove.
        :param select_task: whether to select the task or not.
        :param select_columns: whether to select data from this table or not.
        :return: the modified query.
        """
        if select_task:
            self.selected_columns.append(DulExecutesTask.dul_Task_o.label("task"))
        if select_columns:
            self.selected_columns.append(DulHasParticipant.dul_Object_o.label("participant"))
        if remove_participants is None:
            remove_participants = ["owl:NamedIndividual"]
        else:
            remove_participants.append("owl:NamedIndividual")
        joins = {DulHasParticipant: DulHasParticipant.dul_Event_s == DulExecutesTask.dul_Action_s}
        in_filters = {DulHasParticipant.dul_Object_o: participants,
                   DulHasParticipant.neem_id: [DulExecutesTask.neem_id]}
        remove_filters = {DulHasParticipant.dul_Object_o: remove_participants}
        self._update_joins_metadata(joins, in_filters, remove_filters)
        return self

    def join_participant_types(self, participant_types: Optional[List[str]] = None,
                               remove_participant_types: Optional[List[str]] = None,
                               select_participants: Optional[bool] = False,
                               select_columns: Optional[bool] = True) -> 'NeemLoader':
        """
        Add participant types to the query,
        Assumes participant_types have been joined/selected already if select_participants is False,
         else will select participant_types first.
        :param participant_types: the participant_types to include.
        :param remove_participant_types: the participant_types to remove.
        :param select_participants: whether to select participant_types or not.
        :param select_columns: whether to select data from this table or not.
        :return: the modified query.
        """
        if select_participants:
            self.selected_columns.append(DulHasParticipant.dul_Object_o)
        if select_columns:
            self.selected_columns.append(ParticipantType.o.label("participant_type"))
        if remove_participant_types is None:
            remove_participant_types = ["owl:NamedIndividual"]
        else:
            remove_participant_types.append("owl:NamedIndividual")

        joins = {ParticipantType: ParticipantType.s == DulHasParticipant.dul_Object_o}
        in_filters = {ParticipantType.o: participant_types,
                   ParticipantType.neem_id: [DulHasParticipant.neem_id]}
        remove_filters = {ParticipantType.o: remove_participant_types}
        self._update_joins_metadata(joins, in_filters, remove_filters)
        return self

    def join_task_types(self, tasks: Optional[List[str]] = None,
                        remove_tasks: Optional[List[str]] = None,
                        select_tasks: Optional[bool] = False,
                        select_columns: Optional[bool] = True) -> 'NeemLoader':
        """
        Add task types to the query,
        Assumes tasks have been joined/selected already if select_tasks is False,
         else will select tasks first.
        :param tasks: the tasks to include.
        :param remove_tasks: the tasks to remove.
        :param select_tasks: whether to select the task or not.
        :param select_columns: whether to select data from this table or not.
        :return: the modified query.
        """
        if select_tasks:
            self.selected_columns.append(DulExecutesTask.dul_Task_o.label("task"))
        if select_columns:
            self.selected_columns.append(TaskType.o.label("task_type"))
        if remove_tasks is None:
            remove_tasks = ["owl:NamedIndividual"]
        else:
            remove_tasks.append("owl:NamedIndividual")

        joins = {TaskType: TaskType.s == DulExecutesTask.dul_Task_o}
        in_filters = {TaskType.o: tasks,
                   TaskType.neem_id: [DulExecutesTask.neem_id]}
        remove_filters = {TaskType.o: remove_tasks}
        self._update_joins_metadata(joins, in_filters, remove_filters)
        return self

    def join_participant_base_link(self, select_participants: Optional[bool] = False,
                                   select_columns: Optional[bool] = False) -> 'NeemLoader':
        """
        Add base links of participant_types to the query,
        Assumes participant_types have been joined/selected already if select_participants is False,
         else will select participant_types first.
        :param select_participants: whether to select participant_types or not.
        :param select_columns: whether to select data from this table or not.
        :return: the modified query.
        """
        if select_participants:
            self.selected_columns.append(DulHasParticipant.dul_Object_o)
        if select_columns:
            self.selected_columns.append(UrdfHasBaseLink.urdf_Link_o.label("base_link"))
        joins = {UrdfHasBaseLink: UrdfHasBaseLink.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o}
        in_filters = {UrdfHasBaseLink.neem_id: [DulHasParticipant.neem_id]}
        self._update_joins_metadata(joins, in_filters)
        return self

    def join_task_time_interval(self, select_task: Optional[bool] = False,
                                select_columns: Optional[bool] = False) -> 'NeemLoader':
        """
        Add time interval of tasks to the query,
        Assumes tasks have been joined/queried already if query_task is False,
         else will query task first.
        :param select_task: whether to select tasks or not.
        :param select_columns: whether to select data from this table or not.
        :return: the modified query.
        """
        if select_task:
            self.selected_columns.append(DulExecutesTask.dul_Task_o)
        if select_columns:
            self.selected_columns.append(SomaHasIntervalBegin.o.label("start_time"))
            self.selected_columns.append(SomaHasIntervalEnd.o.label("end_time"))

        joins = {DulHasTimeInterval: DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                 SomaHasIntervalBegin: SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                 SomaHasIntervalEnd: SomaHasIntervalEnd.dul_TimeInterval_s == SomaHasIntervalBegin.dul_TimeInterval_s}

        in_filters = {DulHasTimeInterval.neem_id: [DulExecutesTask.neem_id],
                   SomaHasIntervalBegin.neem_id: [DulExecutesTask.neem_id],
                   SomaHasIntervalEnd.neem_id: [DulExecutesTask.neem_id]}

        self._update_joins_metadata(joins, in_filters)
        return self

    def _update_joins_metadata(self, joins: Dict[Table, BinaryExpression],
                               in_filters: Optional[Dict[Column, List]] = None,
                               remove_filters: Optional[Dict[Column, List]] = None):
        """
        Update the joins' metadata.
        :param joins: the joins.
        :param in_filters: the column values to include.
        :param remove_filters: the column values to remove.
        """
        self.joins.update(joins)
        self.in_filters.update(in_filters)
        self.remove_filters.update(remove_filters)

    def filter(self, in_: Optional[Dict[Column, List]] = None,
               remove: Optional[Dict[Column, List]] = None) -> Select:
        """
        Filter the query.
        :param in_: the column values to include.
        :param remove: the column values to remove.
        :return: the modified query.
        """

        def check_values(values):
            if values is not None:
                if len(values) > 0:
                    if values[0] is not None:
                        return True
            return False

        filters = []
        if in_ is not None:
            filters.extend([col.in_(values) for col, values in in_.items() if check_values(values)])
        if remove is not None:
            filters.extend([not_(col.in_(values)) for col, values in remove.items() if check_values(values)])
        if len(filters) > 0:
            self.query = self.query.filter(*filters)
        return self.query

    def select(self, *entities: InstrumentedAttribute) -> 'NeemLoader':
        """
        Select the columns.
        :param entities: the columns.
        :return: the modified query.
        """
        self.selected_columns.extend(entities)
        return self

    def set_select(self, select_: Optional[Select] = None):
        """
        Select the query.
        :param select_: the query.
        """
        self.query = select_

    def reset(self):
        """
        Reset the query.
        """
        self.query = None
        self.selected_columns = []
        self.joins = {}
        self.in_filters = {}
        self.remove_filters = {}
        self.selected_columns = []
