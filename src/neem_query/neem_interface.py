import pandas as pd
from sqlalchemy import Engine, text
from typing_extensions import Optional, List

from .neem_query import NeemQuery
from .neems_database import *


def get_dataframe_from_sql_query_file(sql_filename: str, engine: Engine) -> pd.DataFrame:
    """
    Read a SQL file and return the result as a pandas DataFrame
    :param sql_filename: the name of the SQL file.
    :param engine: the SQLAlchemy engine to use.
    """
    sql_query = get_sql_query_from_file(sql_filename)
    df = get_dataframe_from_sql_query(sql_query, engine)
    return df


def get_sql_query_from_file(sql_filename: str) -> str:
    """
    Read a SQL file and return the content as a string
    :param sql_filename: the name of the SQL file.
    """
    with open(sql_filename, 'r') as sql_file:
        sql_query = sql_file.read()
    return sql_query


def get_dataframe_from_sql_query(sql_query: str, engine: Engine) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame
    :param sql_query: the SQL query.
    :param engine: the SQLAlchemy engine to use.
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql_query), conn)
    return df


class NeemInterface(NeemQuery):
    """
    A high level interface to the NEEMQuery that provides a more user-friendly API,
    and some useful methods to work with the NEEMs data. Also, it preserves flexibility of the NEEMQuery,
    and allows the user to use it directly for more complex and custom built queries.
    """

    def __init__(self, sql_url: str):
        """
        Initialize the NEEM interface.
        :param sql_url: the URL to the NEEM database.
        """
        super().__init__(sql_url)

    def query_plan_of_neem(self, neem_id: int) -> NeemQuery:
        """
        Get the complete cram plan of a neem given the neem ID.
        :param neem_id: The id in (ID) column of the Neems table.
        :return: The plan as a neem query.
        """
        # noinspection PyTypeChecker
        self.query_plans().join_neems().filter(Neem.ID == neem_id)
        return self

    def query_plans(self) -> NeemQuery:
        """
        Get all the plans in the database.
        :return: The plans as a neem query.
        """
        (self.query_task_sequence().select_subtask_type().select_participant_type().select_parameter_type().
         select_is_performed_by().select_object_mesh_path().
         join_all_subtasks_data(is_outer=True).
         join_all_task_participants_data(is_outer=True).
         join_all_task_parameter_data(is_outer=True).join_task_is_performed_by().join_object_mesh_path(is_outer=True))
        return self

    def query_task_sequence_of_neem(self, sql_neem_id: int) -> NeemQuery:
        """
        Get the task tree of a plan of a certain neem.
        :param sql_neem_id: The sql ID column of the Neems table.
        :return: The task tree of a single neem as a neem query.
        """
        # noinspection PyTypeChecker
        self.query_task_sequence().join_neems().filter_by_sql_neem_id([sql_neem_id])
        return self

    def query_task_sequence(self) -> NeemQuery:
        """
        Get the task tree of all the plans in the database.
        :return: The task tree as a neem query.
        """
        self.reset()
        # noinspection PyTypeChecker
        (self.select_task_type().
         select_time_columns().
         select_neem_id().
         select_from_tasks().
         join_task_types().
         join_task_time_interval().
         order_by_interval_begin())
        return self

    def query_task_data(self, tasks: List[str],
                        regexp: Optional[bool] = False) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        :param tasks: the task names.
        :param regexp: whether to use regular expressions or not.
        :return: the query.
        """
        self.query_neems_motion_replay_data().filter_by_task_types(tasks, regexp)
        return self

    def query_tasks_semantic_data(self, tasks: List[str], outer_join_task_parameters: Optional[bool] = True,
                                  regexp: Optional[bool] = True) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        :param tasks: the task names.
        :param outer_join_task_parameters: whether to use outer join for the task parameters or not.
        :param regexp: whether to use regular expressions or not.
        :return: the query.
        """
        self.reset()
        (self.select_neem_id().select_sql_neem_id().select_task().select_task_type().select_participant().
         select_participant_type().select_environment().select_is_performed_by().select_is_performed_by_type()
         .select_parameter_type().select_object_mesh_path().
         select_from_tasks().
         join_task_types().filter_by_task_types(tasks, regexp=regexp).
         join_task_participants(is_outer=True).
         join_participant_types(is_outer=True).
         join_object_mesh_path(is_outer=True).
         join_task_is_performed_by().join_is_performed_by_type().
         join_all_task_parameter_data(is_outer=outer_join_task_parameters).
         join_task_time_interval().
         join_neems().join_neems_environment().
         order_by_interval_begin()
         )
        return self

    def query_neems_motion_replay_data(self, participant_necessary: Optional[bool] = False,
                                       filter_tf_by_base_link: Optional[bool] = True) -> NeemQuery:
        """
        Get the data needed to replay the motions of the NEEMs.
        :param participant_necessary: whether to only include tasks that have a participant or not.
        :param filter_tf_by_base_link: whether to filter the TFs by the base link or not.
        :return: the query.
        """
        self.reset()
        (self.select_participant().select_participant_type().select_object_mesh_path().
         select_is_performed_by().select_is_performed_by_type().
         select_tf_columns().select_tf_header_columns().select_tf_transform_columns().
         select_neem_id().
         select_environment().
         select_from_tasks().
         join_task_types().
         join_all_task_participants_data(is_outer=not participant_necessary).
         join_object_mesh_path(is_outer=True).
         join_task_time_interval().
         join_tf_on_time_interval().join_tf_header_on_tf().
         join_tf_transfrom().
         join_task_is_performed_by(is_outer=True).join_is_performed_by_type(is_outer=True).
         join_neems().join_neems_environment()
         .order_by_stamp()
         )
        if filter_tf_by_base_link:
            self.filter_tf_by_base_link()
        return self


