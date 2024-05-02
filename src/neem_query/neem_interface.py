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
        self.query_plans().join_neems_metadata().filter(Neem.ID == neem_id)
        return self

    def query_plans(self) -> NeemQuery:
        """
        Get all the plans in the database.
        :return: The plans as a neem query.
        """
        (self.query_task_sequence().select_subtask_type().select_participant_type().select_parameter_type().
         select_is_performed_by().select_participant_mesh_path().
         join_all_subtasks_data(is_outer=True).
         join_all_participants_semantic_data(is_outer=True).
         join_all_task_parameter_data(is_outer=True).join_task_is_performed_by())
        return self

    def query_task_sequence_of_neem(self, sql_neem_id: int) -> NeemQuery:
        """
        Get the task tree of a plan of a certain neem.
        :param sql_neem_id: The sql ID column of the Neems table.
        :return: The task tree of a single neem as a neem query.
        """
        # noinspection PyTypeChecker
        self.query_task_sequence().join_neems_metadata().filter_by_sql_neem_id([sql_neem_id])
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

    def query_task_motion_data(self, tasks: List[str], participant_necessary: Optional[bool] = False,
                               regexp: Optional[bool] = False) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        :param tasks: the task names.
        :param participant_necessary: whether to only include tasks that have a participant or not.
        :param regexp: whether to use regular expressions or not.
        :return: the query.
        """
        (self.query_neems_motion_replay_data(participant_necessary=participant_necessary)
         .filter_by_task_types(tasks, regexp))
        return self

    def query_all_task_data(self, task_types: Optional[List[str]] = None,
                            task_parameters_necessary: Optional[bool] = False,
                            participant_base_link_necessary: Optional[bool] = True,
                            performer_base_link_necessary: Optional[bool] = False,
                            participant_necessary: Optional[bool] = True,
                            performer_necessary: Optional[bool] = True,
                            tasks: Optional[List[str]] = None,
                            regexp: Optional[bool] = True,
                            select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get all the task data from the NEEMs.
        For documentation of the parameters, see  :py:meth:`_query_tasks_data` method.
        """
        self._query_tasks_data(task_types=task_types, task_parameters_necessary=task_parameters_necessary,
                               tasks=tasks, regexp=regexp, select_columns=select_columns)
        if select_columns:
            self.select_all_participants_data().select_all_performers_data()
        (self.
         join_all_participants_data(is_outer=not participant_necessary,
                                    base_link_is_outer=not participant_base_link_necessary).
         join_all_performers_data(is_outer=not performer_necessary,
                                  base_link_outer=not performer_base_link_necessary)
         )
        return self

    def query_tasks_semantic_data(self, task_types: Optional[List[str]] = None,
                                  task_parameters_necessary: Optional[bool] = False,
                                  participant_base_link_necessary: Optional[bool] = False,
                                  performer_base_link_necessary: Optional[bool] = False,
                                  participant_necessary: Optional[bool] = False,
                                  performer_necessary: Optional[bool] = False,
                                  tasks: Optional[List[str]] = None,
                                  regexp: Optional[bool] = True,
                                  select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        For documentation of the parameters, see  :py:meth:`_query_tasks_data` method.
        """
        self._query_tasks_data(task_types=task_types, task_parameters_necessary=task_parameters_necessary,
                               tasks=tasks, regexp=regexp, select_columns=select_columns)
        if select_columns:
            self.select_all_participants_semantic_data().select_all_performers_semantic_data()
        (self.
         join_all_participants_semantic_data(is_outer=not participant_necessary,
                                             base_link_is_outer=not participant_base_link_necessary).
         join_all_performers_semantic_data(is_outer=not performer_necessary,
                                           base_link_is_outer=not performer_base_link_necessary)
         )
        return self

    def _query_tasks_data(self, task_types: Optional[List[str]] = None,
                          task_parameters_necessary: Optional[bool] = False,
                          tasks: Optional[List[str]] = None,
                          regexp: Optional[bool] = True,
                          select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        :param task_types: the task type names.
        :param task_parameters_necessary: whether to use outer join for the task parameters or not.
        :param tasks: the task names.
        :param regexp: whether to use regular expressions or not.
        :param select_columns: whether to select the columns or not.
        :return: the query.
        """
        self.reset()
        if select_columns:
            (self.select_neem_id().select_sql_neem_id().select_task().select_task_type()
             .select_environment().select_parameter_type().select_time_columns())
        (self.
         select_from_tasks().
         join_task_types().
         join_all_task_parameter_data(is_outer=not task_parameters_necessary).
         join_task_time_interval().
         join_neems_metadata().join_neems_environment()
         .order_by_interval_begin()
         )
        if task_types is not None:
            self.filter_by_task_types(task_types, regexp=regexp)
        if tasks is not None:
            self.filter_by_tasks(tasks)
        return self

    def query_neems_motion_replay_data(self, participant_necessary: Optional[bool] = True,
                                       participant_base_link_necessary: Optional[bool] = False) -> NeemQuery:
        """
        Get the data needed to replay the motions of the NEEMs.
        :param participant_necessary: whether to only include tasks that have a participant or not.
        :param participant_base_link_necessary: whether to only include tasks that have a participant base link or not.
        :return: the query.
        """
        self.reset()
        (self.select_all_participants_data().
         select_neem_id().select_environment().select_sql_neem_id().select_task_type().
         select_from_tasks().
         join_task_types().
         join_task_time_interval().
         join_all_participants_data(is_outer=not participant_necessary,
                                    base_link_is_outer=not participant_base_link_necessary).
         join_neems_metadata().join_neems_environment()
         .order_by_participant_tf_stamp()
         )
        return self
