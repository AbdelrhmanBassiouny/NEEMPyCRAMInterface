import pandas as pd
from sqlalchemy import Engine, text
from typing_extensions import Optional

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

    def get_plan_of_neem(self, neem_id: int) -> NeemQuery:
        """
        Get the complete cram plan of a neem given the neem ID.
        :param neem_id: The id in (ID) column of the Neems table.
        :return: The plan as a neem query.
        """
        # noinspection PyTypeChecker
        self.get_all_plans().join_neems().filter(Neem.ID == neem_id)
        return self

    def get_all_plans(self) -> NeemQuery:
        """
        Get all the plans in the database.
        :return: The plans as a neem query.
        """
        (self.get_task_sequence().select_subtask_type().select_participant_type().select_parameter().
         join_subtasks(is_outer=True).
         join_all_task_participants_data(is_outer=True).
         join_task_parameter(is_outer=True).join_parameter_classification(is_outer=True))
        return self

    def get_task_sequence_of_neem(self, neem_id: int) -> NeemQuery:
        """
        Get the task tree of a plan of a certain neem.
        :param neem_id: The id in (ID) column of the Neems table.
        :return: The task tree of a single neem as a neem query.
        """
        # noinspection PyTypeChecker
        self.get_task_sequence().join_neems().filter(Neem.ID == neem_id)
        return self

    def get_task_sequence(self) -> NeemQuery:
        """
        Get the task tree of all the plans in the database.
        :return: The task tree as a neem query.
        """
        self.reset()
        # noinspection PyTypeChecker
        (self.select_task_type().select_time_columns().select(DulExecutesTask.neem_id).
         select_from_tasks().
         join_task_types().
         join_task_time_interval().
         order_by(SomaHasIntervalBegin.o))
        return self

    def get_task_data_from_all_neems(self, task: str,
                                     regexp: Optional[bool] = False) -> NeemQuery:
        """
        Get the data of a certain task from all the NEEMs.
        :param task: the task name.
        :param regexp: whether to use regular expressions or not.
        :return: the query.
        """
        self.get_neems_motion_replay_data().filter_by_task_type(task, regexp)
        return self

    def get_neems_motion_replay_data(self) -> NeemQuery:
        """
        Get the data needed to replay the motions of the NEEMs.
        :return: the query.
        """
        self.reset()
        (self.select_participant().select_participant_type().
         select_tf_columns().select_tf_transform_columns().
         select(Neem._id).
         select_from_tasks().
         join_task_types().
         join_all_task_participants_data(is_outer=True).
         join_task_time_interval().
         join_tf_on_time_interval().join_tf_transfrom().
         join_neems().join_neems_environment()
         .order_by_stamp())
        return self
