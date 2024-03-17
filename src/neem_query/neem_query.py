import logging

import pandas as pd
from sqlalchemy import (create_engine, Engine, between, and_, func, Table, BinaryExpression, select,
                        Select, not_)
from sqlalchemy.orm import sessionmaker, aliased, InstrumentedAttribute, Session
from typing_extensions import Optional, List, Dict

from .neems_database import *

TaskType = aliased(RdfType)
ParticipantType = aliased(RdfType)


class NeemQuery:
    """
    A class to query the neems database using sqlalchemy.
    """
    engine: Engine
    session: Session

    def __init__(self, sql_uri: str):
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

    def get_result(self) -> pd.DataFrame:
        """
        Get the data of the query as a dataframe.
        :param chunk_size: the size of the chunks, if None, will get the whole data at once.
        :return: the dataframe.
        """
        return pd.read_sql_query(self.statement, self.engine)

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
        if len(self.selected_columns) > 0:
            self.query = select(*self.selected_columns)
        if len(self.select_from_tables) > 0:
            if self.query is None:
                self.query = select(*self.select_from_tables)
            else:
                self.query = self.query.select_from(*self.select_from_tables)
        for table, on in self.joins.items():
            self.query = self.query.join(table, on, isouter=self.outer_joins.get(table, False))
        if self._limit is not None:
            self.query = self.query.limit(self._limit)
        if self._order_by is not None:
            self.query = self.query.order_by(self._order_by)
        return self._filter(self.in_filters, self.remove_filters, self.filters)

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

    def join_type(self, type_table: Table, type_of_table: Table, type_of_column: Column,
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

    def join_participant_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of participant_types to the query,
        Assumes participant_types have been joined/selected already.
        :return: the modified query.
        """
        joins = {UrdfHasBaseLink: and_(UrdfHasBaseLink.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                                       UrdfHasBaseLink.neem_id == DulHasParticipant.neem_id)}
        outer_join = {UrdfHasBaseLink: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

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

    def join_tf_on_tasks(self) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes tasks have been joined/selected already.
        :return: the modified query.
        """
        joins = {Tf: Tf.neem_id == DulExecutesTask.neem_id,
                 TfHeader: Tf.header == TfHeader.ID}
        self.update_joins(joins)
        return self

    def join_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes UrdfHasBaseLink has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        """
        joins = {Tf: and_(Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1),
                          Tf.neem_id == UrdfHasBaseLink.neem_id),
                 TfHeader: Tf.header == TfHeader.ID}
        outer_join = {Tf: is_outer}
        self._update_joins_metadata(joins, outer_joins=outer_join)
        return self

    def join_tf_transfrom(self) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes tf has been joined/selected already.
        :return: the modified query.
        """
        joins = {TfTransform: TfTransform.ID == Tf.ID,
                 TransformTranslation: TransformTranslation.ID == TfTransform.translation,
                 TransformRotation: TransformRotation.ID == TfTransform.rotation}
        self.update_joins(joins)
        return self

    def filter_tf_by_base_link(self) -> 'NeemQuery':
        """
        Filter the tf data by base link. Assumes UrdfHasBaseLink has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1))

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

    def find_neem_id(self) -> Optional[BinaryExpression]:
        """
        Find the neem_id column to join on from the already joined/selected tables.
        :return: the neem_id column.
        """
        neem_id = None
        for table in self.joins.keys():
            if self.has_neem_id(table.ID.table):
                neem_id = table.neem_id
                break
        if neem_id is None:
            for col in self.selected_columns:
                if self.has_neem_id(col.table):
                    neem_id = col.table.columns.neem_id
                    break
        if neem_id is None:
            for table in self.select_from_tables:
                if self.has_neem_id(table.ID.table):
                    neem_id = table.neem_id
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
        """
        self.update_selected_columns([Tf.child_frame_id.label("child_frame_id"),
                                      TfHeader.frame_id.label("frame_id"),
                                      TfHeader.stamp.label("stamp")])
        return self

    def select_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select tf transform data (translation, rotation) to the query,
        """
        self.update_selected_columns([TransformTranslation.x.label("tx"),
                                      TransformTranslation.y.label("ty"),
                                      TransformTranslation.z.label("tz"),
                                      TransformRotation.x.label("qx"),
                                      TransformRotation.y.label("qy"),
                                      TransformRotation.z.label("qz"),
                                      TransformRotation.w.label("qw")])
        return self

    def select_time_columns(self) -> 'NeemQuery':
        """
        Select time columns to the query,
        """
        self.update_selected_columns([SomaHasIntervalBegin.o.label("begin"),
                                      SomaHasIntervalEnd.o.label("end")])
        return self

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
        return table in self.joins or [col in self.selected_columns for col in table.ID.table.columns]

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
                    self.joins[table] = and_(joins[table], on)
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
        self.joins[table] = on
        return self

    def filter_by_task_type(self, task: str, regexp: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by task type.
        :param task: the task type.
        :param regexp: whether to use regex to filter the task type or not (will use the sql like operator).
        :return: the modified query.
        """
        return self.filter_by_type(TaskType, task, regexp)

    def filter_by_participant_type(self, participant: str, regexp: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by participant type.
        :param participant: the participant type.
        :param regexp: whether to use regex to filter the participant type or not (will use the sql like operator).
        :return: the modified query.
        """
        return self.filter_by_type(ParticipantType, participant, regexp)

    def filter_by_type(self, type_table: Table, type_: str,
                       regexp: Optional[bool] = False,
                       use_not_: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by type.
        :param type_table: the type table.
        :param type_: the type.
        :param regexp: whether to use regex to filter the type or not (will use the sql like operator).
        :param use_not_: whether to negate the filter or not.
        :return: the modified query.
        """
        if regexp:
            cond = type_table.o.like(f"%{type_}%")
        else:
            cond = type_table.o == type_
        if use_not_:
            cond = not_(cond)
        self.filters.append(cond)
        return self

    def filter(self, *filters: BinaryExpression) -> 'NeemQuery':
        """
        Filter the query.
        :param filters: the filters.
        :return: the modified query.
        """
        self.filters.extend(filters)
        return self

    def _filter(self, in_: Optional[Dict[Column, List]] = None,
                remove: Optional[Dict[Column, List]] = None,
                filters: Optional[List[BinaryExpression]] = None) -> Select:
        """
        Filter the query.
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
            self.query = self.query.filter(*all_filters)
        return self.query

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
