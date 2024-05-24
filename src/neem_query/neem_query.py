import logging

import pandas as pd
from sqlalchemy import (create_engine, Engine, between, and_, func, Table, BinaryExpression, select,
                        Select, not_, or_, Subquery)
from sqlalchemy.orm import sessionmaker, InstrumentedAttribute, Session
from sqlalchemy.sql.selectable import NamedFromClause
# noinspection PyProtectedMember
from typing_extensions import Optional, List, Dict, Type, Union, Tuple, Any

from .enums import (column_to_label, ColumnLabel as CL, TaskType, ParticipantType, SubTaskType, SubTask,
                    TaskParameterClassificationType, TaskParameterClassification, Agent, AgentType, IsPerformedByType,
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
    urdf_folder: Optional[str] = "kinematics/"
    mesh_folders: Optional[List[str]] = ['pouring_hands_neem/meshes/',
                                         'kitchen_object_meshes/',
                                         'bielefeld_study_neem/meshes/']
    _tf_view: Optional[NamedFromClause] = None
    _performer_tf_view: Optional[NamedFromClause] = None
    _participant_tf_view: Optional[NamedFromClause] = None

    def __init__(self, sql_uri: str):
        self._select_neem_id: bool = False
        self.engine = create_engine(sql_uri)
        self.session = sessionmaker(bind=self.engine)()
        self.query: Optional[Select] = None
        self.selected_columns: Optional[List[InstrumentedAttribute]] = []
        self.joins: Optional[Dict[Table, BinaryExpression]] = {}
        self.in_filters: Optional[Dict[Column, List]] = {}
        self.remove_filters: Optional[Dict[Column, List]] = {}
        self.outer_joins: Optional[List[Table]] = []
        self.filters: Optional[List[Union[BinaryExpression, bool]]] = []
        self._limit: Optional[int] = None
        self._order_by: Optional[Column] = None
        self.select_from_tables: Optional[List[Table]] = []
        self.latest_executed_query: Optional[Select] = None
        self.latest_result: Optional[QueryResult] = None
        self._distinct = False

    @property
    def tf_view(self) -> NamedFromClause:
        """
        Get the tf view table.
        :return: the tf view table.
        """
        if self._tf_view is None:
            self._tf_view = self.create_tf_view()
        return self._tf_view

    @property
    def performer_tf_view(self) -> NamedFromClause:
        """
        Get the performer tf view table.
        :return: the performer tf view table.
        """
        if self._performer_tf_view is None:
            self._performer_tf_view = self.create_tf_view('performer_tf')
        return self._performer_tf_view

    @property
    def participant_tf_view(self) -> NamedFromClause:
        """
        Get the participant tf view table.
        :return: the participant tf view table.
        """
        if self._participant_tf_view is None:
            self._participant_tf_view = self.create_tf_view('participant_tf')
        return self._participant_tf_view

    def select_all_participants_semantic_data(self) -> 'NeemQuery':
        """
        Select all the participants' data.
        :return: the modified query.
        """
        (self.select_participant_type()
         .select_participant_base_link()
         # .select_participant_base_link_name()
         .select_participant_mesh_path()
         .select_participant())
        return self

    def select_all_participants_data(self) -> 'NeemQuery':
        """
        Select all the participants' semantic data and the tf data.
        :return: the modified query.
        """
        (self.select_all_participants_semantic_data()
         .select_participant_tf_columns()
         .select_participant_tf_transform_columns()
         )
        return self

    def select_all_performers_semantic_data(self) -> 'NeemQuery':
        """
        Select all the performers' data.
        :return: the modified query.
        """
        (self.select_is_performed_by_type().
         select_is_performed_by().
         select_performer_base_link_name())
        return self

    def select_all_performers_data(self) -> 'NeemQuery':
        """
        Select all the performers' semantic data and the tf data.
        :return: the modified query.
        """
        (self.select_all_performers_semantic_data()
         .select_performer_tf_columns()
         .select_performer_tf_transform_columns()
         )
        return self

    def select_tf_columns(self) -> 'NeemQuery':
        """
        Select tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_view_columns(self.tf_view)

    def select_performer_tf_columns(self) -> 'NeemQuery':
        """
        Select performer tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_view_columns(self.performer_tf_view)

    def select_participant_tf_columns(self) -> 'NeemQuery':
        """
        Select participant tf data (transform, header, child_frame_id) to the query,
        :return: the modified query.
        """
        return self._select_entity_tf_view_columns(self.participant_tf_view)

    def _select_entity_tf_view_columns(self, entity_tf_view: NamedFromClause) -> 'NeemQuery':
        """
        Select tf table columns of the tf view to the query.
        :param entity_tf_view: the tf view table.
        :return: the modified query.
        """
        return self._update_selected_columns([entity_tf_view.child_frame_id,
                                              entity_tf_view.frame_id,
                                              entity_tf_view.stamp])

    def select_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns_from_tf_view(self.tf_view)

    def select_performer_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select performer tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns_from_tf_view(self.performer_tf_view)

    def select_participant_tf_transform_columns(self) -> 'NeemQuery':
        """
        Select participant tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._select_entity_tf_transform_columns_from_tf_view(self.participant_tf_view)

    def _select_entity_tf_transform_columns_from_tf_view(self, entity_tf_view: NamedFromClause) -> 'NeemQuery':
        """
        Select tf transform data (translation, rotation).
        :return: the modified query.
        """
        return self._update_selected_columns([entity_tf_view.x,
                                              entity_tf_view.y,
                                              entity_tf_view.z,
                                              entity_tf_view.qx,
                                              entity_tf_view.qy,
                                              entity_tf_view.qz,
                                              entity_tf_view.qw])

    def select_time_columns(self) -> 'NeemQuery':
        """
        Select time begin and end columns of tasks.
        :return: the modified query.
        """
        self._update_selected_columns([SomaHasIntervalBegin.o, SomaHasIntervalEnd.o])
        return self

    def select_participant(self) -> 'NeemQuery':
        """
        Select participant instances column.
        :return: the modified query.
        """
        self._update_selected_columns([DulHasParticipant.dul_Object_o])
        return self

    def select_task(self) -> 'NeemQuery':
        """
        Select task instances column.
        :return: the modified query.
        """
        self._update_selected_columns([DulExecutesTask.dul_Task_o])
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
        self._update_selected_columns([SubTask.dul_Task_o])
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
        self._update_selected_columns([type_table.o])
        return self

    def select_parameter_category(self) -> 'NeemQuery':
        """
        Select task parameter type column.
        :return: the modified query.
        """
        self._update_selected_columns([DulHasParameter.dul_Parameter_o])
        return self

    def select_parameter(self) -> 'NeemQuery':
        """
        Select task parameter column.
        :return: the modified query.
        """
        self._update_selected_columns([DulClassify.dul_Entity_o])
        return self

    def select_parameter_type(self) -> 'NeemQuery':
        """
        Select task parameter type column.
        :return: the modified query.
        """
        self._update_selected_columns([TaskParameterClassificationType.o])
        return self

    def select_stamp(self) -> 'NeemQuery':
        """
        Select the tf header stamp column.
        :return: the modified query.
        """
        self._update_selected_columns([TfHeader.stamp])
        return self

    def select_from_tasks(self) -> 'NeemQuery':
        """
        Select from the DulExecutesTask table.
        :return: the modified query.
        """
        return self._update_select_from_tables(DulExecutesTask)

    def select_environment(self) -> 'NeemQuery':
        """
        Select the environment values column.
        :return: the modified query.
        """
        self._update_selected_columns([NeemsEnvironmentIndex.environment_values])
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
        Select the neem metadata columns.
        :return: the modified query.
        """
        self._update_selected_columns(Neem.__table__.columns)
        return self

    def select_agent(self) -> 'NeemQuery':
        """
        Select agent instances column.
        :return: the modified query.
        """
        self._update_selected_columns([Agent.dul_Entity_o])
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
        self._update_selected_columns([SomaIsPerformedBy.dul_Agent_o])
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
        self._update_selected_columns([Neem.ID])
        return self

    def select_participant_mesh_path(self) -> 'NeemQuery':
        """
        Select object mesh path column.
        :return: the modified query.
        """
        self._update_selected_columns([SomaHasFilePath.o])
        return self

    def select_base_link_name(self) -> 'NeemQuery':
        """
        Select base link name column.
        :return: the modified query.
        """
        self._update_selected_columns([UrdfHasBaseLinkName.o])
        return self

    def select_performer_base_link(self) -> 'NeemQuery':
        """
        Select performer base link column.
        :return: the modified query.
        """
        self._update_selected_columns([PerformerBaseLink.urdf_Link_o])
        return self

    def select_participant_base_link(self) -> 'NeemQuery':
        """
        Select participant base link name column.
        :return: the modified query.
        """
        self._update_selected_columns([ParticipantBaseLink.urdf_Link_o])
        return self

    def select_performer_base_link_name(self) -> 'NeemQuery':
        """
        Select performer base link name column.
        :return: the modified query.
        """
        self._update_selected_columns([PerformerBaseLinkName.o])
        return self

    def select_participant_base_link_name(self) -> 'NeemQuery':
        """
        Select participant base link name column.
        :return: the modified query.
        """
        self._update_selected_columns([ParticipantBaseLinkName.o])
        return self

    def select(self, *entities: Union[int, str, float, Column]) -> 'NeemQuery':
        """
        Select the columns.
        :param entities: the columns.
        :return: the modified query.
        """
        self._update_selected_columns(entities)
        return self

    def select_from(self, *tables: Type[Base]) -> 'NeemQuery':
        """
        Select the tables.
        :param tables: the tables.
        :return: the modified query.
        """
        self._update_select_from_tables(*tables)
        return self

    def distinct(self) -> 'NeemQuery':
        """
        Distinct the query.
        :return: the modified query.
        """
        self._distinct = True
        return self

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

    def construct_subquery(self, name: Optional[str] = None) -> Subquery:
        """
        Get the subquery.
        """
        return self.construct_query(label=True).subquery(name)

    def construct_query(self, label: Optional[bool] = True) -> Select:
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
            self._update_selected_columns([neem_id])
        if len(self.selected_columns) > 0:
            selected_columns = []
            if label:
                for col in self.selected_columns:
                    if col in column_to_label.keys():
                        selected_columns.append(col.label(column_to_label[col]))
                    elif col.name == "neem_id":
                        selected_columns.append(col.label(CL.neem_id.value))
                    else:
                        logging.debug(f"Column {col} not found in the column to label dictionary")
                        selected_columns.append(col.label(col.table.name + '_' + col.name))
                        # selected_columns.append(col)
            else:
                selected_columns = self.selected_columns
            query = select(*selected_columns)
        if len(self.select_from_tables) > 0:
            if query is None:
                query = select(*self.select_from_tables)
            else:
                query = query.select_from(*self.select_from_tables)
        if self._distinct:
            query = query.distinct()
        for table, on in self.joins.items():
            query = query.join(table, on, isouter=table in self.outer_joins)
        if self._limit is not None:
            query = query.limit(self._limit)
        if self._order_by is not None:
            query = query.order_by(self._order_by)
        self.query = self._filter(query, self.in_filters, self.remove_filters, self.filters)
        return self.query

    def join_task_types(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add task types to the query,
        Assumes tasks have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(TaskType, DulExecutesTask, DulExecutesTask.dul_Task_o, is_outer=is_outer)

    def join_task_time_interval(self) -> 'NeemQuery':
        """
        Add time interval of tasks to the query,
        Assumes tasks have been joined/queried already.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulHasTimeInterval, DulExecutesTask,
                                 on=DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s)
        self.join_neem_id_tables(SomaHasIntervalBegin, DulHasTimeInterval,
                                 on=SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o)
        self.join_neem_id_tables(SomaHasIntervalEnd, SomaHasIntervalBegin,
                                 on=SomaHasIntervalEnd.dul_TimeInterval_s == SomaHasIntervalBegin.dul_TimeInterval_s)
        return self

    def join_all_task_parameter_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the task parameter data.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_task_parameter(is_outer=is_outer)
         .join_task_parameter_classification(is_outer=is_outer)
         .join_task_parameter_classification_type(is_outer=is_outer))
        return self

    def join_task_parameter(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter classification table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulHasParameter, DulExecutesTask,
                                 on=DulHasParameter.dul_Concept_s == DulExecutesTask.dul_Task_o,
                                 is_outer=is_outer)
        return self

    def join_task_parameter_classification(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter table. Assumes DulHasParameter has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(TaskParameterClassification, DulHasParameter,
                                 on=TaskParameterClassification.dul_Concept_s == DulHasParameter.dul_Parameter_o,
                                 is_outer=is_outer)
        return self

    def join_task_parameter_classification_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the task parameter type table. Assumes TaskParameterCategory has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(TaskParameterClassificationType, TaskParameterClassification,
                               TaskParameterClassification.dul_Entity_o,
                               is_outer=is_outer)

    def join_is_task_of(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the is task of table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulIsTaskOf, DulExecutesTask,
                                 on=DulIsTaskOf.dul_Task_s == DulExecutesTask.dul_Task_o,
                                 is_outer=is_outer)
        return self

    def join_all_participants_data(self, is_outer: Optional[bool] = False,
                                   base_link_is_outer: Optional[bool] = False,
                                   filter_tf_by_time_interval: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the participants' semantic data and the tf data, Assumes DulHasTimeInterval has been joined/selected.
        :param is_outer: whether to use outer join or not.
        :param base_link_is_outer: whether to use outer join for the base link or not.
        :param filter_tf_by_time_interval: whether to filter tf data by time interval or not.
        :return: the modified query.
        """
        (self.join_all_participants_semantic_data(is_outer=is_outer, base_link_is_outer=base_link_is_outer)
         # .join_participant_tf_on_time_interval(begin_offset=begin_offset, end_offset=end_offset,
         #                                       is_outer=is_outer)
         .join_participant_tf(is_outer=is_outer)
         # .filter_tf_by_participant_base_link()
         )
        return self

    def join_all_participants_semantic_data(self, is_outer: Optional[bool] = False,
                                            base_link_is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the participants' data.
        :param is_outer: whether to use outer join or not.
        :param base_link_is_outer: whether to use outer join for the base link or not.
        :return: the modified query.
        """
        (self.join_task_participants(is_outer=is_outer).
         join_participant_types(is_outer=is_outer).
         join_participant_base_link(is_outer=base_link_is_outer).
         # join_participant_base_link_name(is_outer=is_outer).
         join_participant_mesh_path(is_outer=True))
        return self

    def join_task_participants(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add task participant_types to the query,
        Assumes DulExecutesTask has been joined/selected already.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulHasParticipant, DulExecutesTask,
                                 on=DulHasParticipant.dul_Event_s == DulExecutesTask.dul_Action_s,
                                 is_outer=is_outer)
        return self

    def join_participant_types(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add participant types to the query,
        Assumes participant_types have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(ParticipantType, DulHasParticipant, DulHasParticipant.dul_Object_o, is_outer=is_outer)

    def join_participant_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of participants to the query,
        Assumes DulHasParticipant have been joined/selected already.
        :return: the modified query.
        """
        self.join_neem_id_tables(ParticipantBaseLink, DulHasParticipant,
                                 on=ParticipantBaseLink.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                                 is_outer=is_outer)
        return self

    def join_participant_base_link_name(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add ParticipantBaseLinkName to the query,
        Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(ParticipantBaseLinkName, DulHasParticipant,
                                 on=ParticipantBaseLinkName.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                                 is_outer=is_outer)
        return self

    def join_participant_mesh_path(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the object mesh path table. Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_participant_shape(is_outer=is_outer).
         join_shape_mesh(is_outer=is_outer).
         join_mesh_path(is_outer=is_outer))
        return self

    def join_participant_shape(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the object shape table. Assumes DulHasParticipant has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(SomaHasShape, DulHasParticipant,
                                 on=SomaHasShape.dul_PhysicalObject_s == DulHasParticipant.dul_Object_o,
                                 is_outer=is_outer)
        return self

    def join_participant_tf(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the participant tf table. Assumes ParticipantBaseLink has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self._join_entity_tf_using_child_frame_id(self.participant_tf_view,
                                                  func.substring_index(ParticipantBaseLink.urdf_Link_o, ':', -1),
                                                  ParticipantBaseLink, is_outer=is_outer)
        return self

    def join_participant_tf_on_time_interval(self, begin_offset: Optional[float] = 0,
                                             end_offset: Optional[float] = 0,
                                             is_outer: Optional[bool] = False,
                                             use_participant_name_as_link_name: Optional[bool] = False) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin, SomaHasIntervalEnd, and ParticipantBaseLinkName have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :param is_outer: whether to use outer join or not.
        :param use_participant_name_as_link_name: whether to use the participant name as the link name or not.
        :return: the modified query.
        """
        if use_participant_name_as_link_name:
            link_name = DulHasParticipant.dul_Object_o
        else:
            link_name = ParticipantBaseLink.urdf_Link_o
        return self._join_entity_tf_on_time_interval_and_frame_id(self.participant_tf_view,
                                                                  func.substring_index(link_name,
                                                                                       ':', -1),
                                                                  begin_offset, end_offset,
                                                                  is_outer=is_outer)

    def join_participant_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes ParticipantBaseLink has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link(ParticipantTf, ParticipantTfHeader,
                                                 ParticipantBaseLink, is_outer=is_outer)

    def join_participant_tf_on_base_link_name(self) -> 'NeemQuery':
        """
        Add participant tf data (transform, header, child_frame_id) to the query,
        Assumes ParticipantBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link_name(ParticipantTf, ParticipantTfHeader, ParticipantBaseLinkName)

    def join_participant_tf_transform(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes participant tf has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(ParticipantTfTransform, ParticipantTf, ParticipantTransformTranslation,
                                              ParticipantTransformRotation, is_outer=is_outer)

    def join_all_performers_data(self, is_outer: Optional[bool] = False,
                                 begin_offset: Optional[float] = 0,
                                 end_offset: Optional[float] = 0,
                                 base_link_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the performers' data.
        :param is_outer: whether to use outer join or not.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :param base_link_outer: whether to use outer join for the base link or not.
        :return: the NEEMs as a pandas dataframe.
        """
        (self.join_all_performers_semantic_data(is_outer=is_outer, base_link_is_outer=base_link_outer)
         # .join_performer_tf_on_time_interval(begin_offset=begin_offset, end_offset=end_offset,
         #                                     is_outer=is_outer)
            .join_performer_tf(is_outer=is_outer)
         )
        return self

    def join_all_performers_semantic_data(self, is_outer: Optional[bool] = False,
                                          base_link_is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the performers' data.
        :param is_outer: whether to use outer join or not.
        :param base_link_is_outer: whether to use outer join for the base link or not.
        :return: the NEEMs as a pandas dataframe.
        """
        (self.join_task_is_performed_by(is_outer=is_outer).
         join_is_performed_by_type(is_outer=is_outer).
         join_performer_base_link_name(is_outer=base_link_is_outer))
        return self

    def join_task_is_performed_by(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the is performed by table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(SomaIsPerformedBy, DulExecutesTask,
                                 on=SomaIsPerformedBy.dul_Action_s == DulExecutesTask.dul_Action_s,
                                 is_outer=is_outer)
        return self

    def join_is_performed_by_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the is performed by type table. Assumes SomaIsPerformedBy has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(IsPerformedByType, SomaIsPerformedBy, SomaIsPerformedBy.dul_Agent_o, is_outer=is_outer)

    def join_performer_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add base links of performers to the query,
        Assumes SomaIsPerformedBy have been joined/selected already.
        :return: the modified query.
        """
        self.join_neem_id_tables(PerformerBaseLink, SomaIsPerformedBy,
                                 on=PerformerBaseLink.dul_PhysicalObject_s == SomaIsPerformedBy.dul_Agent_o,
                                 is_outer=is_outer)
        return self

    def join_performer_base_link_name(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add PerformerBaseLinkName to the query,
        Assumes SomaIsPerformedBy has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(PerformerBaseLinkName, SomaIsPerformedBy,
                                 on=PerformerBaseLinkName.dul_PhysicalObject_s == SomaIsPerformedBy.dul_Agent_o,
                                 is_outer=is_outer)
        return self

    def join_performer_tf(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the performer tf data to the query.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_using_child_frame_id(self.performer_tf_view, PerformerBaseLinkName.o,
                                                         PerformerBaseLinkName, is_outer=is_outer)

    def join_performer_tf_on_time_interval(self, begin_offset: Optional[float] = 0,
                                           end_offset: Optional[float] = 0,
                                           is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin, SomaHasIntervalEnd, and PerformerBaseLinkName have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_on_time_interval_and_frame_id(self.performer_tf_view,
                                                                  PerformerBaseLinkName.o,
                                                                  begin_offset, end_offset,
                                                                  is_outer=is_outer)

    def join_performer_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes PerformerBaseLinkName has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link(PerformerTf, PerformerTfHeader, PerformerBaseLink, is_outer=is_outer)

    def join_performer_tf_on_base_link_name(self) -> 'NeemQuery':
        """
        Add performer tf data (transform, header, child_frame_id) to the query,
        Assumes PerformerBaseLinkName has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_on_base_link_name(PerformerTf, PerformerTfHeader, PerformerBaseLinkName)

    def join_performer_tf_transform(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes performer tf has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(PerformerTfTransform, PerformerTf, PerformerTransformTranslation,
                                              PerformerTransformRotation, is_outer=is_outer)

    def join_all_subtasks_data(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join all the subtasks' data.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        (self.join_has_constituent(is_outer=is_outer)
         .join_subtasks(is_outer=is_outer)
         .join_subtask_type(is_outer=is_outer))
        return self

    def join_has_constituent(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the has constituent table. Assumes DulExecutesTask has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulHasConstituent, DulExecutesTask,
                                 on=DulHasConstituent.dul_Entity_s == DulExecutesTask.dul_Action_s,
                                 is_outer=is_outer)
        return self

    def join_subtasks(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the subtasks table. Assumes DulHasConstituent has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(SubTask, DulHasConstituent,
                                 on=SubTask.dul_Action_s == DulHasConstituent.dul_Entity_o,
                                 is_outer=is_outer)
        return self

    def join_subtask_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the subtask type table. Assumes subtasks have been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(SubTaskType, SubTask, SubTask.dul_Task_o, is_outer=is_outer)

    def join_neems_metadata(self, on: Optional[BinaryExpression] = None) -> 'NeemQuery':
        """
        Join the neem table which contains the neems metadata, if on is None, will join on the neem_id column of
         any table that has it.
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
        self._update_joins(joins)
        return self

    def join_neems_environment(self) -> 'NeemQuery':
        """
        Join the neems_environment_index table. Assumes neem has been joined/selected already.
        :return: the modified query.
        """
        joins = {NeemsEnvironmentIndex: NeemsEnvironmentIndex.neems_ID == Neem.ID}
        self._update_joins(joins)
        return self

    def join_agent(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the agent table. Assumes DulIsTaskOf has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(Agent, DulIsTaskOf,
                                 on=Agent.dul_Concept_s == DulIsTaskOf.dul_Role_o,
                                 is_outer=is_outer)
        return self

    def join_agent_type(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the agent type table. Assumes Agent has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        return self._join_type(AgentType, Agent, Agent.dul_Entity_o, is_outer=is_outer)

    def join_shape_mesh(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the shape mesh table. Assumes SomaHasShape has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(DulHasRegion, SomaHasShape,
                                 on=DulHasRegion.dul_Entity_s == SomaHasShape.soma_Shape_o,
                                 is_outer=is_outer)
        return self

    def join_mesh_path(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join the mesh path table. Assumes DulHasRegion has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(SomaHasFilePath, DulHasRegion,
                                 on=SomaHasFilePath.dul_Entity_s == DulHasRegion.dul_Region_o,
                                 is_outer=is_outer)
        return self

    def join_tf_on_time_interval(self, begin_offset: Optional[float] = 0,
                                 end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin and SomaHasIntervalEnd have been joined/selected already.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self._join_entity_tf_on_time_interval(self.tf_view, begin_offset, end_offset)

    def join_tf_on_base_link(self, is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes UrdfHasBaseLink has been joined/selected already.
        :param is_outer: whether to use outer join or not.
        """
        return self._join_entity_tf_using_child_frame_id(Tf, func.substring_index(UrdfHasBaseLink.urdf_Link_o, ':', -1),
                                                         UrdfHasBaseLink, is_outer=is_outer)

    def create_tf_view(self, name: Optional[str] = 'TfData') -> NamedFromClause:
        """
        Create a view of the TF data.
        :param name: the name of the view.
        :return: the view table.
        """
        return self._create_entity_tf_view(Tf, TfHeader, TfTransform, TransformTranslation, TransformRotation, name)

    def join_tf_transform(self) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes tf has been joined/selected already.
        :return: the modified query.
        """
        return self._join_entity_tf_transform(TfTransform, Tf, TransformTranslation, TransformRotation)

    def _join_entity_tf_on_base_link(self, entity_tf: Type[Tf], entity_tf_header: Type[TfHeader],
                                     entity_base_link: Type[UrdfHasBaseLink],
                                     is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add entity (performer, participant, ...etc.) tf data (transform, header, child_frame_id) to the query,
        :param entity_tf: The tf table for the entity.
        :param entity_tf_header: The tf header table for the entity.
        :param entity_base_link: The base link table for the entity.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        self._join_entity_tf_using_child_frame_id(entity_tf,
                                                  func.substring_index(entity_base_link.urdf_Link_o, ':', -1),
                                                  entity_base_link, is_outer=is_outer)
        return self._join_entity_tf_header_on_tf(entity_tf_header, entity_tf)

    def _join_entity_tf_on_time_interval_and_frame_id(self, entity_tf_view: NamedFromClause,
                                                      entity_frame_id: str,
                                                      begin_offset: Optional[float] = 0,
                                                      end_offset: Optional[float] = 0,
                                                      is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add entity (performer, participant, ...etc.) tf data (transform, header, child_frame_id) to the query,
        :param entity_tf_view: The view of the entity tf data.
        :param entity_frame_id: The frame id of the entity.
        :param begin_offset: The time offset from the beginning of the task.
        :param end_offset: The time offset from the end of the task.
        :param is_outer: Whether to use outer join or not.
        :return: The modified query.
        """
        self._join_entity_tf_on_time_interval(entity_tf_view, begin_offset, end_offset, is_outer=is_outer)
        self._update_join_with_conditions(entity_tf_view, [entity_tf_view.child_frame_id == entity_frame_id])
        return self

    def _join_entity_tf_on_time_interval(self, entity_tf_view: NamedFromClause,
                                         begin_offset: Optional[float] = 0,
                                         end_offset: Optional[float] = 0,
                                         is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data (transform, header, child_frame_id) to the query,
        Assumes SomaHasIntervalBegin and SomaHasIntervalEnd have been joined/selected already.
        :param entity_tf_view: the view of the entity tf data.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        cond = between(entity_tf_view.stamp,
                       SomaHasIntervalBegin.o + begin_offset,
                       SomaHasIntervalEnd.o + end_offset)
        self.join_neem_id_tables(entity_tf_view, SomaHasIntervalBegin, on=cond, is_outer=is_outer)
        return self

    def _join_entity_tf_using_child_frame_id(self, entity_tf: Union[Type[Tf], NamedFromClause],
                                             child_frame_id: str,
                                             neem_id_table: Type[Base],
                                             is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add tf data to the query,
        :param child_frame_id: The value to compare with the Tf.child_frame_id.
        :param neem_id_table: The table that has neem_id to compare with Tf.neem_id.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        self.join_neem_id_tables(entity_tf, neem_id_table,
                                 on=entity_tf.child_frame_id == child_frame_id,
                                 is_outer=is_outer)
        return self

    def _join_entity_tf_on_base_link_name(self, entity_tf: Type[Tf],
                                          entity_tf_header: Type[TfHeader],
                                          entity_base_link: Type[UrdfHasBaseLinkName]) -> 'NeemQuery':
        """
        Add entity tf data (transform, header, child_frame_id) to the query,
        Assumes entity base link name has been joined/selected already.
        :param entity_tf: the tf table for the entity.
        :param entity_tf_header: the tf header table for the entity.
        :param entity_base_link: the base link name table for the entity.
        :return: the modified query.
        """
        self.join_neem_id_tables(entity_tf, entity_base_link,
                                 on=entity_tf.child_frame_id == entity_base_link.o)
        return self._join_entity_tf_header_on_tf(entity_tf_header, entity_tf)

    def _create_entity_tf_view(self,
                               entity_tf: Type[Tf],
                               entity_tf_header: Type[TfHeader],
                               entity_tf_transform: Type[TfTransform],
                               entity_tf_translation: Type[TransformTranslation],
                               entity_tf_rotation: Type[TransformRotation],
                               name: Optional[str] = None) -> NamedFromClause:
        """
        Create a view of the TF data.
        """
        nq = NeemQuery(self.engine.url.__str__())
        subquery = (nq._select_entity_tf_columns(entity_tf, entity_tf_header)
                    ._select_entity_tf_transform_columns(entity_tf_translation, entity_tf_rotation)
                    .select(entity_tf.neem_id)
                    ._join_entity_tf_header_on_tf(entity_tf_header, entity_tf)
                    ._join_entity_tf_transform(entity_tf_transform, entity_tf, entity_tf_translation,
                                               entity_tf_rotation)
                    ).construct_subquery(name)
        return self.create_table_from_subquery(subquery)

    def _join_entity_tf_header_on_tf(self, entity_tf_header: Type[TfHeader], entity_tf: Type[Tf]) -> 'NeemQuery':
        """
        Join the entity_tf_header on the entity_tf table using the header column in the entity_tf table.
        Assumes entity_tf has been joined/selected already.
        :param entity_tf_header: The tf header table of the entity.
        :param entity_tf: The tf table of the entity.
        :return: The modified query.
        """
        joins = {entity_tf_header: entity_tf_header.ID == entity_tf.header}
        self._update_joins(joins)
        return self

    def _join_entity_tf_transform(self, transform_table: Type[TfTransform],
                                  tf_table: Type[Tf],
                                  transform_translation: Type[TransformTranslation],
                                  transform_rotation: Type[TransformRotation],
                                  is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Add transform data to the query.
        Assumes tf has been joined/selected already.
        :param transform_table: the transform table.
        :param tf_table: the tf table.
        :param transform_translation: the transform translation table.
        :param transform_rotation: the transform rotation table.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {transform_table: transform_table.ID == tf_table.transform,
                 transform_translation: transform_translation.ID == transform_table.translation,
                 transform_rotation: transform_rotation.ID == transform_table.rotation}
        outer_joins = [transform_table, transform_translation, transform_rotation] if is_outer else None
        self._update_joins_metadata(joins, outer_joins=outer_joins)
        return self

    def _join_type(self, type_table: Type[RdfType], type_of_table: Type[Base], type_of_column: Union[Column, str],
                   is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join a type table.
        :param type_table: the type table.
        :param type_of_table: the table to join on.
        :param type_of_column: the column to join on.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        self.join_neem_id_tables(type_table, type_of_table,
                                 on=and_(type_table.s == type_of_column,
                                         type_table.o != "owl:NamedIndividual"),
                                 is_outer=is_outer)
        return self

    def join_neem_id_tables(self, join_table: Type[Base],
                            join_on_table: Type[Base],
                            on: Optional[Union[BinaryExpression, bool]] = None,
                            neem_id: Optional[Column] = None,
                            is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join two tables using their neem_id columns and an additional condition specified in the on parameter.
        :param join_table: First predicate table.
        :param join_on_table: Second predicate table.
        :param on: The condition to join on. If None, will join on the neem_id column only.
        :param neem_id: The neem_id column to join on.
        :param is_outer: whether to use outer join or not.
        :return: The modified query.
        """
        if neem_id is None:
            neem_id_cond = join_table.neem_id == join_on_table.neem_id
        else:
            neem_id_cond = neem_id == join_on_table.neem_id
        cond = neem_id_cond if on is None else and_(neem_id_cond, on)
        return self.join(join_table, cond, is_outer=is_outer)

    def join(self, table: Type[Base], on: Optional[BinaryExpression] = None,
             is_outer: Optional[bool] = False) -> 'NeemQuery':
        """
        Join a table.
        :param table: the table.
        :param on: the condition to join on.
        :param is_outer: whether to use outer join or not.
        :return: the modified query.
        """
        joins = {table: on}
        outer_join = [table] if is_outer else None
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

    def order_by_tf_stamp(self) -> 'NeemQuery':
        """
        Order the query results by the tf header stamp column.
        :return: the modified query.
        """
        return self.order_by(self.tf_view.stamp)

    def order_by_participant_tf_stamp(self) -> 'NeemQuery':
        """
        Order the query results by the participant tf header stamp column.
        :return: the modified query.
        """
        return self.order_by(self.participant_tf_view.stamp)

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

    def order_by(self, column: Union[Column, int, float]) -> 'NeemQuery':
        """
        Order the query results. (It's recommended to use this after having the dataframe in pandas to
         avoid long query times)
        :param column: the column.
        :return: the modified query.
        """
        self._order_by = column
        return self

    def filter_tf_by_participant_base_link(self) -> 'NeemQuery':
        """
        Filter the tf data by base link. Assumes ParticipantBaseLink has been joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == func.substring_index(ParticipantBaseLink.urdf_Link_o, ':', -1))

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
        Filter the tf data by participant base link name. Assumes ParticipantBaseLinkName has been
         joined/selected already.
        :return: the modified query.
        """
        return self.filter(Tf.child_frame_id == ParticipantBaseLinkName.o)

    def filter_tf_by_child_frame_id(self, child_frame_id: Union[str, Column]) -> 'NeemQuery':
        """
        Filter the tf data by child frame id.
        :param child_frame_id: the child frame id.
        :return: the modified query.
        """
        return self._filter_entity_tf_by_child_frame_id(self.tf_view, child_frame_id)

    def _filter_entity_tf_by_child_frame_id(self, entity_tf_view: NamedFromClause,
                                            child_frame_id: Union[str, Column]) -> 'NeemQuery':
        """
        Filter the tf data by child frame id.
        :param child_frame_id: the child frame id.
        :return: the modified query.
        """
        return self.filter(entity_tf_view.child_frame_id == func.substring_index(child_frame_id, ':', -1))

    def filter_by_tasks(self, tasks: List[str], regexp: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the tasks by their names.
        :param tasks: the tasks.
        :param regexp: whether to use regex to filter the task or not (will use the sql like operator).
        """
        return self.filter_string_column(DulExecutesTask.dul_Task_o, tasks, regexp=regexp)

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
        return self.filter_string_column(type_table.o, types, regexp=regexp, negate=negate)

    def filter_tf_by_time_interval(self, begin_offset: Optional[float] = 0,
                                   end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Filter the tf data by time interval.
        For arguments documentation look at :py:meth:`NEEMQuery._filter_entity_tf_by_time_interval`.
        :return: the modified query.
        """
        return self._filter_entity_tf_by_time_interval(self.tf_view, begin_offset, end_offset)

    def filter_participant_tf_by_time_interval(self, begin_offset: Optional[float] = 0,
                                               end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Filter the tf data by time interval.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self._filter_entity_tf_by_time_interval(self.participant_tf_view, begin_offset, end_offset)

    def filter_performer_tf_by_time_interval(self, begin_offset: Optional[float] = 0,
                                             end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Filter the tf data by time interval.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self._filter_entity_tf_by_time_interval(self.performer_tf_view, begin_offset, end_offset)

    def _filter_entity_tf_by_time_interval(self, entity_tf_view: NamedFromClause,
                                           begin_offset: Optional[float] = 0,
                                           end_offset: Optional[float] = 0) -> 'NeemQuery':
        """
        Filter the tf data by time interval.
        :param entity_tf_view: the view of the entity tf data.
        :param begin_offset: the time offset from the beginning of the task.
        :param end_offset: the time offset from the end of the task.
        :return: the modified query.
        """
        return self.filter(between(entity_tf_view.stamp,
                                   SomaHasIntervalBegin.o + begin_offset,
                                   SomaHasIntervalEnd.o + end_offset))

    def filter_string_column(self, filter_col: Union[Column, str], values: List[str], regexp: Optional[bool] = False,
                             negate: Optional[bool] = False) -> 'NeemQuery':
        """
        Filter the query by type.
        :param filter_col: the column to filter on.
        :param values: the values to include/exclude.
        :param regexp: whether to use regex to filter the type or not (will use the sql like operator).
        :param negate: whether to negate the filter or not.
        :return: the modified query.
        """
        if regexp:
            cond = [filter_col.like(f"%{v}%") for v in values]
            cond = or_(*cond)
        else:
            cond = filter_col.in_(values)
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
        neem_id_col = self.find_neem_id(look_in_selected=True)
        self.filters.append(neem_id_col == neem_id)
        return self

    def filter(self, *filters: Union[BinaryExpression, bool]) -> 'NeemQuery':
        """
        Filter the query.
        :param filters: the filters.
        :return: the modified query.
        """
        self.filters.extend(filters)
        return self

    def find_neem_id(self, look_in_selected: Optional[bool] = False) -> Optional[Union[Column, str]]:
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

    def _select_entity_tf_columns(self, entity_tf: Type[Tf],
                                  entity_tf_header: Type[TfHeader]) -> 'NeemQuery':
        """
        Select entity tf data (transform, header, child_frame_id) to the query,
        :param entity_tf: The tf table for the entity.
        :param entity_tf_header: The tf header table for the entity.
        :return: the modified query.
        """
        self._update_selected_columns([entity_tf.child_frame_id,
                                       entity_tf_header.stamp,
                                       entity_tf_header.frame_id])
        return self

    def _select_entity_tf_header_columns(self, entity_tf_header: Type[TfHeader]) -> 'NeemQuery':
        """
        Select entity tf header data (frame_id, stamp) to the query,
        :param entity_tf_header: The tf header table for the entity.
        :return: the modified query.
        """
        self._update_selected_columns([entity_tf_header.frame_id,
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
        self._update_selected_columns([transform_translation.x,
                                       transform_translation.y,
                                       transform_translation.z,
                                       transform_rotation.x,
                                       transform_rotation.y,
                                       transform_rotation.z,
                                       transform_rotation.w])
        return self

    def _update_select_from_tables(self, *tables: Type[Base]) -> 'NeemQuery':
        """
        Update the selected tables.
        :param tables: the tables.
        :return: the modified query.
        """
        for table in tables:
            if table not in self.select_from_tables:
                self.select_from_tables.append(table)
        return self

    def _update_selected_columns(self, columns: Union[Tuple, List]) -> 'NeemQuery':
        """
        Update the selected columns.
        :param columns: the columns.
        :return: the modified query.
        """
        for col in columns:
            if col not in self.selected_columns and not isinstance(col.table, Subquery):
                self.selected_columns.append(col)
            else:
                selected_cols_names = [column_to_label[sc] if sc in column_to_label else sc.name
                                       for sc in self.selected_columns]
                try:
                    idx = selected_cols_names.index(col.name)
                    if isinstance(col.table, Subquery):
                        # replace the selected column with the column from the subquery
                        self.selected_columns[idx] = col
                except ValueError:
                    pass

        return self

    def _update_join_with_conditions(self, table: Type[Base], conditions: List[BinaryExpression]) -> 'NeemQuery':
        """
        Update the join conditions of a table.
        :param table: the table.
        :param conditions: the conditions.
        :return: the modified query.
        """
        self.joins[table] = and_(*conditions, self.joins[table])
        return self

    def _update_joins_metadata(self, joins: Dict[Any, BinaryExpression],
                               outer_joins: Optional[List] = None):
        """
        Update the joins' metadata.
        :param joins: the joins.
        :param outer_joins: the outer joins.
        """
        if joins is not None:
            self._update_joins(joins)
        if outer_joins is not None:
            self._update_outer_joins(outer_joins)

    def _update_joins(self, joins: Dict[Any, BinaryExpression]):
        """
        Update the joins' dictionary by adding a new join or updating an existing one with an additional condition.
        :param joins: the joins' dictionary.
        """
        for table, on in joins.items():
            if table in self.joins:
                if on != self.joins[table]:
                    logging.error(f"Table {table} has been joined before on a different condition")
                    raise ValueError(f"Table {table} has been joined before on a different condition")
                else:
                    continue
            else:
                self.joins[table] = on

    def _update_outer_joins(self, outer_joins: List[Table]):
        """
        Update the outer_joins' list.
        :param outer_joins: the outer_joins' list.
        """
        self.outer_joins.extend(outer_joins)

    @staticmethod
    def get_column_names(table: Table) -> List[str]:
        """
        Get the column names of a table
        :param table: the table.
        :return: the column names.
        """
        # noinspection PyTypeChecker
        return [col.name for col in table.columns]

    def has_neem_id(self, table: Table) -> bool:
        """
        Check if a table has a neem_id column
        :param table: the table.
        :return: True if the table has a neem_id column, False otherwise.
        """
        return self.has_column(table, "neem_id")

    def has_column(self, table: Table, column: str) -> bool:
        """
        Check if a table has a column
        :param table: the table.
        :param column: the column name.
        :return: True if the table has the column, False otherwise.
        """
        return column in self.get_column_names(table)

    # noinspection PyTypeChecker
    def _get_task_data(self, task: str, use_regex: Optional[bool] = False,
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

    def _get_task_data_using_joins(self, task: str, use_regexp: Optional[bool] = False,
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

    @staticmethod
    def _filter(query: Select, in_filters: Optional[Dict[Column, List]] = None,
                remove: Optional[Dict[Column, List]] = None,
                filters: Optional[List[BinaryExpression]] = None) -> Select:
        """
        Filter the query.
        :param query: the query.
        :param in_filters: the column values to include.
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
        if in_filters is not None:
            filters.extend([col.in_(values) for col, values in in_filters.items() if check_values(values)])
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

    @staticmethod
    def create_table_from_subquery(subquery: Subquery) -> NamedFromClause:
        """
        Create a table from a subquery.
        """
        table = subquery.alias(subquery.name)
        for c in table.c:
            setattr(table, c.name, c)
        return table

    @classmethod
    def _get_mesh_links(cls):
        """
        Get the mesh links of the objects.
        """
        return [cls.neem_data_link + folder for folder in cls.mesh_folders]

    @classmethod
    def _get_urdf_link(cls):
        """
        Get the link to the folder where all NEEM URDFs are stored.
        """
        return cls.neem_data_link + cls.urdf_folder

    def reset(self):
        """
        Reset the query.
        """
        self.query = None
        self.selected_columns = []
        self.joins = {}
        self.in_filters = {}
        self.remove_filters = {}
        self.outer_joins = []
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
