import logging

import pandas as pd
from sqlalchemy import (create_engine, Engine, between, and_, func, Table, BinaryExpression, select,
                        Select, not_, or_)
from sqlalchemy.orm import sessionmaker, InstrumentedAttribute, Session
from sqlalchemy.testing import in_
from typing_extensions import Optional, List, Dict, Type

from .enums import (column_to_label, ColumnLabel as CL, TaskType, ParticipantType, SubTaskType, SubTask,
                    TaskParameterType, TaskParameterCategory, Agent, AgentType, IsPerformedByType,
                    PerformerBaseLinkName, ParticipantBaseLinkName, PerformerTf, ParticipantTf,
                    PerformerTfTransform, ParticipantTfTransform, PerformerTfHeader, ParticipantTfHeader,
                    PerformerTransformTranslation, PerformerTransformRotation,
                    ParticipantTransformTranslation, ParticipantTransformRotation, ParticipantBaseLink,
                    PerformerBaseLink)
from .neems_database import *
from .query_result import QueryResult


class NeemQuery:
    """
    A class to query the neems database using sqlalchemy.
    """
    engine: Engine
    session: Session
    neem_data_link: Optional[str] = "https://neem-data.informatik.uni-bremen.de/data/"
    mesh_folders: Optional[List[str]] = ['pouring_hands_neem/meshes/',
                                         'kitchen_object_meshes/',
                                         'bielefeld_study_neem/meshes/']

    def __init__(self, sql_uri: str):
        self._select_neem_id: bool = False
        self.engine = create_engine(sql_uri)
        self.session = sessionmaker(bind=self.engine)()
        self.query: Optional[Select] = None
        self.selected_columns: Optional[List[InstrumentedAttribute]] = []
        self.joins: Optional[Dict[Table, BinaryExpression]] = {}
        self.in_filters: Optional[Dict[Column, List]] = {}
        self.remove_filters: Optional[Dict[Column, List]] = {}
        self.outer_joins: Optional[Dict[Table, bool]] = {}
        self.filters: Optional[List[BinaryExpression]] = []
        self._limit: Optional[int] = None
        self._order_by: Optional[Column] = None
        self.select_from_tables: Optional[List[Table]] = []
        self.latest_executed_query: Optional[Select] = None
        self.latest_result: Optional[QueryResult] = None
        self._distinct = False

    @classmethod
    def _get_mesh_links(cls):
        """
        Get the mesh links of the objects.
        """
        return [cls.neem_data_link + folder for folder in cls.mesh_folders]

    def get_task(self, task: str) -> DulExecutesTask:
        """
        Get the task from the database
        :param task: the task name.
        :return: the task.
        """
        # noinspection PyTypeChecker
        return self.session.query(DulExecutesTask).filter(DulExecutesTask.dul_Task_o == task).first()

    @staticmethod
    def get_column_names(table: Table) -> List[str]:
        """
        Get the column names of a table
        :param table: the table.
        :return: the column names.
        """
        # noinspection PyTypeChecker
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
        return self.has_column(table, "neem_id")

    def has_column(self, table: InstrumentedAttribute, column: str) -> bool:
        """
        Check if a table has a column
        :param table: the table.
        :param column: the column name.
        :return: True if the table has the column, False otherwise.
        """
        return column in self.get_column_names(table)

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

    def get_result(self) -> QueryResult:
        """
        Get the data of the query as a QueryResult object using the pandas dataframe.
        """
        if self.query_changed:
            self.latest_result = QueryResult(pd.read_sql_query(self.statement, self.engine))
            self.latest_executed_query = self.query
        return self.latest_result

    def get_result_in_chunks(self, chunk_size: Optional[int] = 500):
        """
        Get the data of the query as a dataframe in chunks.
        """
        with self.engine.connect().execution_options(stream_results=True) as conn:
            for chunk_df in pd.read_sql_query(self.statement, conn, chunksize=chunk_size):
                yield chunk_df

    @property
    def statement(self) -> Select:
        """
        Get the query statement.
        :return: the query statement.
        """
        self.construct_query()
        return self.query

    def construct_query(self) -> Select:
        """
        Get the query.
        :return: the query.
        """
        query = None
        if self._select_neem_id:
            neem_id = self.find_neem_id(look_in_selected=True)
            if neem_id is None:
                logging.error("No neem_id found in the tables")
                raise ValueError("No neem_id found in the tables")
            self.update_selected_columns([neem_id])
        if len(self.selected_columns) > 0:
            selected_columns = []
            for col in self.selected_columns:
                if col in column_to_label.keys():
                    selected_columns.append(col.label(column_to_label[col]))
                elif col.name == "neem_id":
                    selected_columns.append(col.label(CL.neem_id.value))
                else:
                    logging.debug(f"Column {col} not found in the column to label dictionary")
                    selected_columns.append(col.label(col.table.name + '_' + col.name))
            query = select(*selected_columns)
        if len(self.select_from_tables) > 0:
            if query is None:
                query = select(*self.select_from_tables)
            else:
                query = query.select_from(*self.select_from_tables)
        if self._distinct:
            query = query.distinct()
        for table, on in self.joins.items():
            query = query.join(table, on, isouter=self.outer_joins.get(table, False))
        if self._limit is not None:
            query = query.limit(self._limit)
        if self._order_by is not None:
            query = query.order_by(self._order_by)
        self.query = self._filter(query, self.in_filters, self.remove_filters, self.filters)
        return self.query

    def join_all_subtasks_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the subtasks data.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_subtasks(is_outer=is_outer).
         join_subtask_type(is_outer=is_outer))
        return self

    def join_subtasks(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the subtasks table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {DulHasConstituent:
                     and_(DulHasConstituent.dul_Entity_s == DulExecutesTask.dul_Action_s,
                          DulHasConstituent.neem_id == DulExecutesTask.neem_id),
                 SubTask: and_(SubTask.dul_Action_s == DulHasConstituent.dul_Entity_o,
                               SubTask.neem_id == DulExecutesTask.neem_id)}
        outer_join = {DulHasConstituent: is_outer, SubTask: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_subtask_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the subtask type table. Assumes subtasks have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(SubTaskType, SubTask, SubTask.dul_Task_o, is_outer=is_outer)

    def join_all_task_participants_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the participants' data.
        :param is_outer: whether to use outer join or not.
        :return: the NEEMs as a pandas dataframe.
        """
        (self.join_task_participants(is_outer=is_outer).
         join_participant_types(is_outer=is_outer).
         join_participant_base_link(is_outer=is_outer))
        return self

    def join_task_participants(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add task participant_types to the query,
        Assumes tasks have been joined/selected already.
        :return: the modified query.
        """
        joins = {DulHasParticipant: and_(DulHasParticipant.dul_Event_s == DulExecutesTask.dul_Action_s,
                                         DulHasParticipant.neem_id == DulExecutesTask.neem_id)}
        outer_join = {DulHasParticipant: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_participant_types(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add participant types to the query,
        Assumes participant_types have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(ParticipantType, DulHasParticipant, DulHasParticipant.dul_Object_o, is_outer=is_outer)

    def join_task_types(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add task types to the query,
        Assumes tasks have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(TaskType, DulExecutesTask, DulExecutesTask.dul_Task_o, is_outer=is_outer)

    def join_participant_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of participants to the query,
        Assumes DulHasParticipant have been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_base_link(ParticipantBaseLink, DulHasParticipant.dul_Object_o,
                                           DulHasParticipant.neem_id, is_outer)

    def join_performer_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of performers to the query,
        Assumes SomaIsPerformedBy have been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_base_link(PerformerBaseLink, SomaIsPerformedBy.dul_Agent_o,
                                           SomaIsPerformedBy.neem_id, is_outer)

    def join_performer_base_link_name(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add PerformerBaseLinkName to the query,
        Assumes SomaIsPerformedBy has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_base_link_name(PerformerBaseLinkName, SomaIsPerformedBy.dul_Agent_o,
                                                SomaIsPerformedBy.neem_id, is_outer)

    def join_participant_base_link_name(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add ParticipantBaseLinkName to the query,
        Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_base_link_name(ParticipantBaseLinkName, DulHasParticipant.dul_Object_o,
                                                DulHasParticipant.neem_id, is_outer)

    def join_task_time_interval(self) -> 'NeemQuery':
        """
        Add time interval of tasks to the query,
        Assumes tasks have been joined/queried already.
        :return: the modified query.
        """
        joins = {DulHasTimeInterval:
                     and_(DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                          DulHasTimeInterval.neem_id == DulExecutesTask.neem_id),
                 SomaHasIntervalBegin:
                     and_(SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                          SomaHasIntervalBegin.neem_id == DulExecutesTask.neem_id),
                 SomaHasIntervalEnd:
                     and_(SomaHasIntervalEnd.dul_TimeInterval_s == SomaHasIntervalBegin.dul_TimeInterval_s,
                          SomaHasIntervalEnd.neem_id == DulExecutesTask.neem_id)}

        self.update_joins(joins)
        return self

    def join_tf_on_time_interval(self, begin_offset: Optional[float] = -40,
                                 end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin and SomaHasIntervalEnd have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        joins = {TfHeader: between(TfHeader.stamp,
                                   SomaHasIntervalBegin.o + begin_offset,
                                   SomaHasIntervalEnd.o + end_offset),
                 Tf: and_(Tf.header == TfHeader.ID,
                          Tf.neem_id == SomaHasIntervalBegin.neem_id), }
        self.update_joins(joins)
        return self

    def join_performer_tf_on_time_interval(self, begin_offset: Optional[float] = -40,
                                           end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin, SomaHasIntervalEnd, and PerformerBaseLinkName have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self._join_entity_tf_on_time_interval(PerformerTf, PerformerTfHeader, PerformerBaseLinkName.o,
                                                     begin_offset, end_offset)

    def join_participant_tf_on_time_interval(self, begin_offset: Optional[float] = -40,
                                             end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin, SomaHasIntervalEnd, and ParticipantBaseLinkName have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self._join_entity_tf_on_time_interval(ParticipantTf, ParticipantTfHeader, ParticipantBaseLinkName.o,
                                                     begin_offset, end_offset)

    def join_tf_on_tasks(self) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes tasks have been joined/selected already.
        :return: the modified query.
        """
        joins = {Tf: Tf.neem_id == DulExecutesTask.neem_id}
        self.update_joins(joins)
        self.join_tf_header_on_tf()
        return self

    def join_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes UrdfHasBaseLink has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        """
        return self._join_tf_using_child_frame_id(func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1),
                                                  UrdfHasBaseLink.neem_id, is_outer=is_outer)

    def join_tf_on_participant(self, is_outer: Optional[bool] = False):
        """
        Add tf data to the query,
        Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        return self._join_tf_using_child_frame_id(func.substring_index(DulHasParticipant.dul_Object_o, ':', -1),
                                                  DulHasParticipant.neem_id, is_outer=is_outer)

    def join_tf_header_on_tf(self) -> 'NeemQuery':
        """
        Join the TfHeader on the Tf table using the header column in the Tf table.
        Assumes Tf has been joined/selected already.
        :return: The modified query.
        """
        return self._join_entity_tf_header_on_tf(TfHeader, Tf)

    def join_tf_transfrom(self) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes tf has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(TfTransform, Tf, TransformTranslation, TransformRotation)

    def join_performer_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes PerformerBaseLinkName has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link(PerformerTf, PerformerBaseLink, is_outer=is_outer)

    def join_participant_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes ParticipantBaseLinkName has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link(ParticipantTf, ParticipantBaseLink, is_outer=is_outer)

    def join_performer_tf_on_base_link_name(self) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes PerformerBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link_name(PerformerTf, PerformerBaseLinkName)

    def join_participant_tf_on_base_link_name(self) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes ParticipantBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link_name(ParticipantTf, ParticipantBaseLinkName)

    def join_performer_tf_header_on_tf(self) -> 'NeemQuery':
        """
        Join the performer_tf_header on the performer_tf table using the header column in the performer_tf table.
        :return: The modified query.
        """
        return self._join_entity_tf_header_on_tf(PerformerTfHeader, PerformerTf)

    def join_participant_tf_header_on_tf(self) -> 'NeemQuery':
        """
        Join the participant_tf_header on the participant_tf table using the header column in the participant_tf table.
        :return: The modified query.
        """
        return self._join_entity_tf_header_on_tf(ParticipantTfHeader, ParticipantTf)

    def join_performer_tf_transform(self) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes performer tf has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(PerformerTfTransform, PerformerTf, PerformerTransformTranslation,
                                              PerformerTransformRotation)

    def join_participant_tf_transform(self) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes participant tf has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(ParticipantTfTransform, ParticipantTf, ParticipantTransformTranslation,
                                              ParticipantTransformRotation)

    def _join_entity_base_link(self, entity_base_link: Type[UrdfHasBaseLink], entity_name: str,
                               neem_id: str, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of participants to the query,
        Assumes DulHasParticipant have been joined/selected already.
        :param entity_base_link: the entity_base_link to join.
        :param entity_name: the entity_name to compare with entity_base_link object.
        :param neem_id: the neem_id to compare with entity_base_link neem_id.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {entity_base_link: and_(entity_base_link.dul_PhysicalObject_s == entity_name,
                                        entity_base_link.neem_id == neem_id)}
        outer_join = {entity_base_link: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def _join_entity_tf_on_base_link(self, entity_tf: Type[Tf], entity_base_link: Type[UrdfHasBaseLink],
                                     is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add entity (performer, participant, ...etc.) tf data (transform, header, child_frame_id) to the query,
        :param entity_tf: The tf table for the entity.
        :param entity_base_link: The base link table for the entity.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        joins = {entity_tf: and_(entity_tf.child_frame_id == func.substring_index(entity_base_link.urdf_Link_o, ':', -1),
                                 entity_tf.neem_id == entity_base_link.neem_id)}
        outer_join = {entity_tf: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def _join_entity_base_link_name(self, entity_base_link: Type[UrdfHasBaseLink],
                                    entity_col: str, neem_id: str, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base link name to the query,
        Assumes entity has been joined/selected already.
        :param entity_base_link: the base link name table for the entity.
        :param entity_col: the column to compare with the base link name.
        :param neem_id: the neem_id to compare with the base link name.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {entity_base_link: and_(entity_base_link.dul_PhysicalObject_s == entity_col,
                                        entity_base_link.neem_id == neem_id)}
        outer_join = {entity_base_link: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def _join_entity_tf_on_time_interval(self, entity_tf: Type[Tf],
                                         entity_tf_header: Type[TfHeader],
                                         entity_frame_id: str,
                                         begin_offset: Optional[float] = -40,
                                         end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Add entity (performer, participant, ...etc.) tf data (transform, header, child_frame_id) to the query,
        :param entity_tf: The tf table for the entity.
        :param entity_tf_header: The tf header table for the entity.
        :param entity_frame_id: The frame id of the entity.
        :param begin_offset: The time offset from the beginning of the task.
        :param end_offset: The time offset from the end of the task.
        :return: The modified query.
        """
        joins = {entity_tf_header: between(entity_tf_header.stamp, SomaHasIntervalBegin.o + begin_offset,
                                           SomaHasIntervalEnd.o + end_offset),
                 entity_tf: and_(entity_tf.header == entity_tf_header.ID,
                                 entity_tf.child_frame_id == entity_frame_id,
                                 entity_tf.neem_id == SomaHasIntervalBegin.neem_id)}
        self.update_joins(joins)
        return self

    def _join_entity_tf_on_tf_header(self, entity_tf: Type[Tf], entity_tf_header: Type[TfHeader]) -> 'NeemQuery':
        """
        Join the entity_tf table on the entity_tf_header table.
        :param entity_tf: The Tf table of the entity.
        :param entity_tf_header: The Tf header table of the entity.
        :return: The modified query.
        """
        joins = {entity_tf: entity_tf.header == entity_tf_header.ID}
        self.update_joins(joins)
        return self

    def _join_tf_using_child_frame_id(self, child_frame_id: str,
                                      neem_id: str,
                                      is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data to the query,
        :param child_frame_id: The value to compare with the Tf.child_frame_id.
        :param neem_id: The neem_id to compare with Tf.neem_id.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        joins = {Tf: and_(Tf.child_frame_id == child_frame_id,
                          Tf.neem_id == neem_id)}
        outer_join = {Tf: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def _join_entity_tf_on_base_link_name(self, entity_tf: Type[Tf],
                                          entity_base_link: Type[UrdfHasBaseLinkName]) -> 'NeemQuery':
        """
        Add entity tf data (transform, header, child_frame_id) to the query,
        Assumes entity base link name has been joined/selected already.
        :param entity_tf: the tf table for the entity.
        :param entity_base_link: the base link name table for the entity.
        :return: the modified query.
        """
        joins = {entity_tf: and_(entity_tf.child_frame_id == entity_base_link.o,
                                 entity_tf.neem_id == entity_base_link.neem_id)}
        self.update_joins(joins)
        return self

    def _join_entity_tf_header_on_tf(self, entity_tf_header: Type[TfHeader], entity_tf: Type[Tf]) -> 'NeemQuery':
        """
        Join the entity_tf_header on the entity_tf table using the header column in the entity_tf table.
        Assumes entity_tf has been joined/selected already.
        :param entity_tf_header: The tf header table of the entity.
        :param entity_tf: The tf table of the entity.
        :return: The modified query.
        """
        joins = {entity_tf_header: entity_tf_header.ID == entity_tf.header}
        self.update_joins(joins)
        return self

    def _join_entity_tf_transform(self, transform_table: Type[TfTransform],
                                  tf_table: Type[Tf],
                                  transform_translation: Type[TransformTranslation],
                                  transform_rotation: Type[TransformRotation]) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes tf has been joined/selected already.
        :return: the modified query.
        """
        joins = {transform_table: transform_table.ID == tf_table.ID,
                 transform_translation: transform_translation.ID == transform_table.translation,
                 transform_rotation: transform_rotation.ID == transform_table.rotation}
        self.update_joins(joins)
        return self

    def filter_tf_by_base_link(self) -> 'NeemQuery':
        """
        Filter the tf data by base link. Assumes UrdfHasBaseLink has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1))

    def filter_tf_by_base_link_name(self) -> 'NeemQuery':
        """
        Filter the tf data by URDF base link name. Assumes UrdfHasBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == UrdfHasBaseLinkName.o)

    def filter_tf_by_performer_base_link_name(self) -> 'NeemQuery':
        """
        Filter the tf data by performer base link name. Assumes PerformerBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == PerformerBaseLinkName.o)

    def filter_tf_by_participant_base_link_name(self) -> 'NeemQuery':
        """
        Filter the tf data by participant base link name. Assumes ParticipantBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == ParticipantBaseLinkName.o)

    def filter_tf_by_base_link_or_participant(self) -> 'NeemQuery':
        """
        Filter the tf data by base link or participant. Assumes UrdfHasBaseLink and DulHasParticipant have been joined/selected already.
        :return: the modified query.
        """
        return self.filter(or_(Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1),
                               Tf.child_frame_id == func.substring_index(DulHasParticipant.dul_Object_o, ':', -1)))

    def join_neems(self, on: Optional[BinaryExpression] = None) -> 'NeemQuery':
        """
        Join the neem table, if on is None, will join on the neem_id column of any table that has it.
        :param on: the condition to join on.
        :return: the modified query.
        """
        if on is None:
            neem_id = self.find_neem_id()
            if neem_id is None:
                logging.error("The joining condition was not specified for the neems table,"
                              " and no neem_id was found in the already joined/selected tables")
                raise ValueError("No neem_id found in the tables")
            on = Neem._id == neem_id
        joins = {Neem: on}
        self.update_joins(joins)
        return self

    def find_neem_id(self, look_in_selected: Optional[bool] = False) -> Optional[BinaryExpression]:
        """
        Find the neem_id column to join on from the already joined/selected tables.
        :param look_in_selected: whether to look in the selected tables or not.
        :return: the neem_id column.
        """
        neem_id = None
        for table in self.select_from_tables:
            if self.has_neem_id(table.__table__):
                neem_id = table.neem_id
                break
        if neem_id is None:
            for table in self.joins.keys():
                if self.has_neem_id(table.__table__):
                    neem_id = table.neem_id
                    break
        if neem_id is None and look_in_selected:
            for col in self.selected_columns:
                if self.has_neem_id(col.table):
                    neem_id = col.class_.neem_id
                    break
        return neem_id

    def join_neems_environment(self) -> 'NeemQuery':
        """
        Join the neems_environment_index table. Assumes neem has been joined/selected already.
        :return: the modified query.
        """
        joins = {NeemsEnvironmentIndex: NeemsEnvironmentIndex.neems_ID == Neem.ID}
        self.update_joins(joins)
        return self

    def join_all_task_parameter_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the task parameter data.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_task_parameter_category(is_outer=is_outer).
         join_task_parameter(is_outer=is_outer).
         join_task_parameter_type(is_outer=is_outer))
        return self

    def join_task_parameter_category(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {DulHasParameter: and_(DulHasParameter.dul_Concept_s == DulExecutesTask.dul_Task_o,
                                       DulHasParameter.neem_id == DulExecutesTask.neem_id)}
        outer_join = {DulHasParameter: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_task_parameter(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter classification table. Assumes DulHasParameter has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {TaskParameterCategory: and_(TaskParameterCategory.dul_Concept_s == DulHasParameter.dul_Parameter_o,
                                             TaskParameterCategory.neem_id == DulHasParameter.neem_id)}
        outer_join = {TaskParameterCategory: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_task_parameter_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter type table. Assumes TaskParameterCategory has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(TaskParameterType, TaskParameterCategory, TaskParameterCategory.dul_Entity_o,
                              is_outer=is_outer)

    def join_all_agent_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the agent data.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_agent(is_outer=is_outer).
         join_agent_type(is_outer=is_outer))
        return self

    def join_agent(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the agent table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {DulIsTaskOf: and_(DulIsTaskOf.dul_Task_s == DulExecutesTask.dul_Task_o,
                                   DulIsTaskOf.neem_id == DulExecutesTask.neem_id),
                 Agent: and_(Agent.dul_Concept_s == DulIsTaskOf.dul_Role_o,
                             Agent.neem_id == DulExecutesTask.neem_id)}
        outer_join = {DulIsTaskOf: is_outer, Agent: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_agent_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the agent type table. Assumes Agent has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(AgentType, Agent, Agent.dul_Entity_o, is_outer=is_outer)

    def join_task_is_performed_by(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the is performed by table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {SomaIsPerformedBy: and_(SomaIsPerformedBy.dul_Action_s == DulExecutesTask.dul_Action_s,
                                         SomaIsPerformedBy.neem_id == DulExecutesTask.neem_id)}
        outer_join = {SomaIsPerformedBy: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_is_performed_by_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the is performed by type table. Assumes SomaIsPerformedBy has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self.join_type(IsPerformedByType, SomaIsPerformedBy, SomaIsPerformedBy.dul_Agent_o, is_outer=is_outer)

    def join_object_mesh_path(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the object mesh path table. Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_object_shape(is_outer=is_outer).
         join_shape_mesh(is_outer=is_outer).
         join_mesh_path(is_outer=is_outer))
        return self

    def join_object_shape(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the object shape table. Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {SomaHasShape: and_(SomaHasShape.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                                    SomaHasShape.neem_id == DulHasParticipant.neem_id)}
        outer_join = {SomaHasShape: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_shape_mesh(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the shape mesh table. Assumes SomaHasShape has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {DulHasRegion: and_(DulHasRegion.dul_Entity_s == SomaHasShape.soma_Shape_o,
                                    DulHasRegion.neem_id == SomaHasShape.neem_id)}
        outer_join = {DulHasRegion: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_mesh_path(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the mesh path table. Assumes DulHasRegion has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {SomaHasFilePath: and_(SomaHasFilePath.dul_Entity_s == DulHasRegion.dul_Region_o,
                                       SomaHasFilePath.neem_id == DulHasRegion.neem_id)}
        outer_join = {SomaHasFilePath: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_type(self, type_table: Type[RdfType], type_of_table: Table, type_of_column: Column,
                  is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join a type table.
        :param type_table: the type table.
        :param type_of_table: the table to join on.
        :param type_of_column: the column to join on.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {type_table: and_(type_table.s == type_of_column,
                                  type_table.neem_id == type_of_table.neem_id,
                                  type_table.o != "owl:NamedIndividual")}
        outer_join = {type_table: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def limit(self, limit: int) -> 'NeemQuery':
        """
        Limit the query results.
        :param limit: the limit.
        :return: the modified query.
        """
        self._limit = limit
        return self

    def order_by(self, column: Column) -> 'NeemQuery':
        """
        Order the query results. (It's recommended to use this after having the dataframe in pandas to
         avoid long query times)
        :param column: the column.
        :return: the modified query.
        """
        self._order_by = column
        return self

    def select_tf_columns(self) -> 'NeemQuery':
        """
        Select tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_columns(Tf)

    def select_tf_header_columns(self) -> 'NeemQuery':
        """
        Select tf header data (frame_id, stamp) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_header_columns(TfHeader)

    def select_performer_tf_columns(self) -> 'NeemQuery':
        """
        Select performer tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_columns(PerformerTf)

    def select_performer_tf_header_columns(self) -> 'NeemQuery':
        """
        Select performer tf header data (frame_id, stamp) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_header_columns(PerformerTfHeader)

    def select_participant_tf_columns(self) -> 'NeemQuery':
        """
        Select participant tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_columns(ParticipantTf)

    def select_participant_tf_header_columns(self) -> 'NeemQuery':
        """
        Select participant tf header data (frame_id, stamp) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_header_columns(ParticipantTfHeader)

    def select_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns(TransformTranslation, TransformRotation)

    def select_performer_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select performer tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns(PerformerTransformTranslation, PerformerTransformRotation)

    def select_participant_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select participant tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns(ParticipantTransformTranslation, ParticipantTransformRotation)

    def _select_entity_tf_columns(self, entity_tf: Type[Tf]) -> 'NeemQuery':
        """
        Select entity tf data (transform, header, child_frame_id) to the query,
        :param entity_tf: The tf table for the entity.
        :return: the modified query.
        """
        self.update_selected_columns([entity_tf.child_frame_id])
        return self

    def _select_entity_tf_header_columns(self, entity_tf_header: Type[TfHeader]) -> 'NeemQuery':
        """
        Select entity tf header data (frame_id, stamp) to the query,
        :param entity_tf_header: The tf header table for the entity.
        :return: the modified query.
        """
        self.update_selected_columns([entity_tf_header.frame_id,
                                      entity_tf_header.stamp])
        return self

    def _select_entity_tf_transform_columns(self, transform_translation: Type[TransformTranslation],
                                            transform_rotation: Type[TransformRotation]) -> 'NeemQuery':
        """
        Select entity tf transform data (translation, rotation).
        :param transform_translation: The translation table for the entity.
        :param transform_rotation: The rotation table for the entity.
        :return: the modified query.
        """
        self.update_selected_columns([transform_translation.x,
                                      transform_translation.y,
                                      transform_translation.z,
                                      transform_rotation.x,
                                      transform_rotation.y,
                                      transform_rotation.z,
                                      transform_rotation.w])
        return self

    def select_time_columns(self) -> 'NeemQuery':
        """
        Select time begin and end columns of tasks.
        :return: the modified query.
        """
        self.update_selected_columns([SomaHasIntervalBegin.o, SomaHasIntervalEnd.o])
        return self

    def select_participant(self) -> 'NeemQuery':
        """
        Select participant instances column.
        :return: the modified query.
        """
        self.update_selected_columns([DulHasParticipant.dul_Object_o])
        return self

    def select_task(self) -> 'NeemQuery':
        """
        Select task instances column.
        :return: the modified query.
        """
        self.update_selected_columns([DulExecutesTask.dul_Task_o])
        return self

    def select_task_type(self) -> 'NeemQuery':
        """
        Select task type column.
        :return: the modified query.
        """
        self.select_type(TaskType)
        return self

    def select_participant_type(self) -> 'NeemQuery':
        """
        Select participant type column.
        :return: the modified query.
        """
        self.select_type(ParticipantType)
        return self

    def select_subtask(self) -> 'NeemQuery':
        """
        Select subtask instances column.
        :return: the modified query.
        """
        self.update_selected_columns([SubTask.dul_Task_o])
        return self

    def select_subtask_type(self) -> 'NeemQuery':
        """
        Select subtask type column.
        :return: the modified query.
        """
        self.select_type(SubTaskType)
        return self

    def select_type(self, type_table: Type[RdfType]) -> 'NeemQuery':
        """
        Select a type to the query,
        """
        self.update_selected_columns([type_table.o])
        return self

    def select_parameter_category(self) -> 'NeemQuery':
        """
        Select task parameter type column.
        :return: the modified query.
        """
        self.update_selected_columns([DulHasParameter.dul_Parameter_o])
        return self

    def select_parameter(self) -> 'NeemQuery':
        """
        Select task parameter column.
        :return: the modified query.
        """
        self.update_selected_columns([DulClassify.dul_Entity_o])
        return self

    def select_parameter_type(self) -> 'NeemQuery':
        """
        Select task parameter type column.
        :return: the modified query.
        """
        self.update_selected_columns([TaskParameterType.o])
        return self

    def select_stamp(self) -> 'NeemQuery':
        """
        Select the tf header stamp column.
        :return: the modified query.
        """
        self.update_selected_columns([TfHeader.stamp])
        return self

    def select_from_tasks(self) -> 'NeemQuery':
        """
        Select from the DulExecutesTask table.
        :return: the modified query.
        """
        return self.update_select_from_tables([DulExecutesTask])

    def select_environment(self) -> 'NeemQuery':
        """
        Select the environment values column.
        :return: the modified query.
        """
        self.update_selected_columns([NeemsEnvironmentIndex.environment_values])
        return self

    def select_neem_id(self) -> 'NeemQuery':
        """
        Select the neem_id column.
        :return: the modified query.
        """
        self._select_neem_id = True
        return self

    def select_neem_meta_data(self) -> 'NeemQuery':
        """
        Select the neem meta data columns.
        :return: the modified query.
        """
        self.update_selected_columns(Neem.__table__.columns)
        return self

    def select_agent(self) -> 'NeemQuery':
        """
        Select agent instances column.
        :return: the modified query.
        """
        self.update_selected_columns([Agent.dul_Entity_o])
        return self

    def select_agent_type(self) -> 'NeemQuery':
        """
        Select agent type column.
        :return: the modified query.
        """
        self.select_type(AgentType)
        return self

    def select_is_performed_by(self) -> 'NeemQuery':
        """
        Select is performed by column.
        :return: the modified query.
        """
        self.update_selected_columns([SomaIsPerformedBy.dul_Agent_o])
        return self

    def select_is_performed_by_type(self) -> 'NeemQuery':
        """
        Select is performed by type column.
        :return: the modified query.
        """
        self.select_type(IsPerformedByType)
        return self

    def select_sql_neem_id(self) -> 'NeemQuery':
        """
        Select the neem_id column.
        :return: the modified query.
        """
        self.update_selected_columns([Neem.ID])
        return self

    def select_object_mesh_path(self) -> 'NeemQuery':
        """
        Select object mesh path column.
        :return: the modified query.
        """
        self.update_selected_columns([SomaHasFilePath.o])
        return self

    def select_base_link_name(self) -> 'NeemQuery':
        """
        Select base link name column.
        :return: the modified query.
        """
        self.update_selected_columns([UrdfHasBaseLinkName.o])
        return self

    def select_performer_base_link(self) -> 'NeemQuery':
        """
        Select performer base link column.
        :return: the modified query.
        """
        self.update_selected_columns([PerformerBaseLink.urdf_Link_o])
        return self

    def select_participant_base_link(self) -> 'NeemQuery':
        """
        Select participant base link name column.
        :return: the modified query.
        """
        self.update_selected_columns([ParticipantBaseLink.urdf_Link_o])
        return self

    def select_performer_base_link_name(self) -> 'NeemQuery':
        """
        Select performer base link name column.
        :return: the modified query.
        """
        self.update_selected_columns([PerformerBaseLinkName.o])
        return self

    def select_participant_base_link_name(self) -> 'NeemQuery':
        """
        Select participant base link name column.
        :return: the modified query.
        """
        self.update_selected_columns([ParticipantBaseLinkName.o])
        return self

    def order_by_stamp(self) -> 'NeemQuery':
        """
        Order the query results by the tf header stamp column.
        :return: the modified query.
        """
        return self.order_by(TfHeader.stamp)

    def order_by_interval_begin(self) -> 'NeemQuery':
        """
        Order the query results by the soma interval begin column.
        :return: the modified query.
        """
        return self.order_by(SomaHasIntervalBegin.o)

    def order_by_interval_end(self) -> 'NeemQuery':
        """
        Order the query results by the soma interval end column.
        :return: the modified query.
        """
        return self.order_by(SomaHasIntervalEnd.o)

    def update_select_from_tables(self, tables: List[Table]) -> 'NeemQuery':
        """
        Update the selected tables.
        :param tables: the tables.
        :return: the modified query.
        """
        for table in tables:
            if table not in self.select_from_tables:
                self.select_from_tables.append(table)
        return self

    def update_selected_columns(self, columns: List[InstrumentedAttribute]) -> 'NeemQuery':
        """
        Update the selected columns.
        :param columns: the columns.
        :return: the modified query.
        """
        for col in columns:
            if col not in self.selected_columns:
                self.selected_columns.append(col)
        return self

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the query.
        :param table: the table.
        :return: True if the table exists, False otherwise.
        """
        return table in self.joins or [col in self.selected_columns for col in table.__table__.columns]

    def _update_joins_metadata(self, joins: Dict[Table, BinaryExpression],
                               in_filters: Optional[Dict[Column, List]] = None,
                               remove_filters: Optional[Dict[Column, List]] = None,
                               outer_joins: Optional[Dict[Table, bool]] = None):
        """
        Update the joins' metadata.
        :param joins: the joins.
        :param in_filters: the column values to include.
        :param remove_filters: the column values to remove.
        """
        if joins is not None:
            self.update_joins(joins)
        if in_filters is not None:
            self.update_in_filters(in_filters)
        if remove_filters is not None:
            self.update_remove_filters(remove_filters)
        if outer_joins is not None:
            self.update_outer_joins(outer_joins)

    def update_joins(self, joins: Dict[Table, BinaryExpression]):
        """
        Update the joins' dictionary by adding a new join or updating an existing one with an additional condition.
        :param joins: the joins' dictionary.
        """
        for table, on in joins.items():
            if table in self.joins:
                if on != self.joins[table]:
                    logging.error(f"Table {Table} has been joined before on a different condition")
                    raise ValueError(f"Table {Table} has been joined before on a different condition")
                else:
                    continue
            else:
                self.joins[table] = on

    def update_in_filters(self, in_filters: Dict[Column, List]):
        """
        Update the in_filters' dictionary by adding a new column or updating an existing one with additional values.
        :param in_filters: the in_filters' dictionary.
        """
        self.update_filters(self.in_filters, in_filters)

    def update_remove_filters(self, remove_filters: Dict[Column, List]):
        """
        Update the remove_filters' dictionary by adding a new column or updating an existing one with additional values.
        :param remove_filters: the remove_filters' dictionary.
        """
        self.update_filters(self.remove_filters, remove_filters)

    def update_outer_joins(self, outer_joins: Dict[Table, bool]):
        """
        Update the outer_joins' dictionary.
        :param outer_joins: the outer_joins' dictionary.
        """
        self.outer_joins.update(outer_joins)

    @staticmethod
    def update_filters(filters: Dict[Column, List], updates: Dict[Column, List]):
        """
        Update the filters' dictionary by adding a new column or updating an existing one with additional values.
        :param filters: the in_filters' dictionary.
        :param updates: the updates' dictionary.
        """
        for col, values in updates.items():
            if col in filters:
                for value in values:
                    if value not in filters[col]:
                        filters[col].append(value)
            else:
                filters[col] = values

    def join(self, table: Table, on: BinaryExpression) -> 'NeemQuery':
        """
        Join a table.
        :param table: the table.
        :param on: the condition.
        :return: the modified query.
        """
        self.update_joins({table: on})
        return self

    def filter_by_task_types(self, tasks: List[str], regexp: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by task types.
        :param tasks: the task types.
        :param regexp: whether to use regex to filter the task type or not (will use the sql like operator).
        :return: the modified query.
        """
        return self.filter_by_type(TaskType, tasks, regexp)

    def filter_by_participant_type(self, participants: List[str], regexp: Optional[bool] = False,
                                   negate: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by participant type.
        :param participants: the participant types.
        :param regexp: whether to use regex to filter the participant type or not (will use the sql like operator).
        :param negate: whether to negate the filter or not.
        :return: the modified query.
        """
        return self.filter_by_type(ParticipantType, participants, regexp=regexp, negate=negate)

    def filter_by_performer_type(self, performers: List[str], regexp: Optional[bool] = False,
                                 negate: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by performer type.
        :param performers: the performer types.
        :param regexp: whether to use regex to filter the performer type or not (will use the sql like operator).
        :param negate: whether to negate the filter or not.
        :return: the modified query.
        """
        return self.filter_by_type(IsPerformedByType, performers, regexp=regexp, negate=negate)

    def filter_by_type(self, type_table: Type[RdfType], types: List[str], regexp: Optional[bool] = False,
                       negate: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by type.
        :param type_table: the type table.
        :param types: the types.
        :param regexp: whether to use regex to filter the type or not (will use the sql like operator).
        :param negate: whether to negate the filter or not.
        :return: the modified query.
        """
        if regexp:
            cond = [type_table.o.like(f"%{t}%") for t in types]
        else:
            cond = [type_table.o == t for t in types]
        cond = or_(*cond)
        if negate:
            cond = not_(cond)
        self.filters.append(cond)
        return self

    def filter_by_sql_neem_id(self, neem_ids: List[int]) -> 'NeemQuery':
        """
        Filter the query by neem_id.
        :param neem_ids: the neem_ids.
        :return: the modified query.
        """
        self.filters.append(Neem.ID.in_(neem_ids))
        return self

    def filter_by_neem_id(self, neem_id: str) -> 'NeemQuery':
        """
        Filter the query by neem_id.
        :param neem_id: the neem_id.
        :return: the modified query.
        """
        neem_id_col = self.find_neem_id()
        self.filters.append(neem_id_col == neem_id)
        return self

    def filter(self, *filters: BinaryExpression) -> 'NeemQuery':
        """
        Filter the query.
        :param filters: the filters.
        :return: the modified query.
        """
        self.filters.extend(filters)
        return self

    @staticmethod
    def _filter(query: Select, in_: Optional[Dict[Column, List]] = None,
                remove: Optional[Dict[Column, List]] = None,
                filters: Optional[List[BinaryExpression]] = None) -> Select:
        """
        Filter the query.
        :param query: the query.
        :param in_: the column values to include.
        :param remove: the column values to remove.
        :param filters: the filters to apply.
        :return: the modified query.
        """

        def check_values(values):
            if values is not None:
                if len(values) > 0:
                    if values[0] is not None:
                        return True
            return False

        all_filters = []
        if in_ is not None:
            filters.extend([col.in_(values) for col, values in in_.items() if check_values(values)])
        if remove is not None:
            filters.extend([not_(col.in_(values)) for col, values in remove.items() if check_values(values)])
        if filters is not None:
            all_filters.extend(filters)
        if len(all_filters) > 0:
            if query is None:
                query = Select.filter(*all_filters)
            else:
                query = query.filter(*all_filters)
        return query

    def select(self, *entities: InstrumentedAttribute) -> 'NeemQuery':
        """
        Select the columns.
        :param entities: the columns.
        :return: the modified query.
        """
        self.update_selected_columns(entities)
        return self

    def select_from(self, *tables: Table) -> 'NeemQuery':
        """
        Select the tables.
        :param tables: the tables.
        :return: the modified query.
        """
        self.update_select_from_tables(tables)
        return self

    def distinct(self) -> 'NeemQuery':
        """
        Distinct the query.
        :return: the modified query.
        """
        self._distinct = True
        return self

    def get_all_joined_and_selected_tables(self, as_str: Optional[bool] = False) -> List[Table]:
        """
        Get all the joined and selected tables.
        :param as_str: whether to return the tables as strings or not.
        :return: the tables.
        """
        tables = list(self.joins.keys()) + self.select_from_tables
        if as_str:
            return [table.ID.table.name for table in tables]
        return tables

    def reset(self):
        """
        Reset the query.
        """
        self.query = None
        self.selected_columns = []
        self.joins = {}
        self.in_filters = {}
        self.remove_filters = {}
        self.outer_joins = {}
        self.filters = []
        self._limit = None
        self._order_by = None
        self.select_from_tables = []
        self.latest_executed_query = None
        self.latest_result = None
        self._distinct = False
        self._select_neem_id = False

    def __eq__(self, other):
        return self.construct_query() == other.construct_query()

    @property
    def query_changed(self):
        if self.latest_executed_query is None:
            return True
        return str(self.construct_query()) != str(self.latest_executed_query)
