import pandas as pd
from sqlalchemy import Engine, text
from typing_extensions import List, Tuple, Optional

from .neem_query import NeemQuery
from .neems_database import *
from .enums import ColumnLabel as CL


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


class NeemInterface:

    def __init__(self, sql_url: str):
        self.nq = NeemQuery(sql_url)

    def get_plan_of_neem(self, neem_id: int) -> NeemQuery:
        """
        Get the complete cram plan of a neem given the neem ID.
        :param neem_id: The id in (ID) column of the Neems table.
        :return: The plan as a neem query.
        """
        # noinspection PyTypeChecker
        self.get_all_plans().join_neems().filter(Neem.ID == neem_id)
        return self.nq

    def get_all_plans(self) -> NeemQuery:
        """
        Get all the plans in the database.
        :return: The plans as a neem query.
        """
        (self.get_task_sequence().select_subtask_type().select_participant_type().select_parameter().
         join_subtasks(is_outer=True).
         join_all_task_participants_data(is_outer=True).
         join_task_parameter(is_outer=True).join_parameter_classification(is_outer=True))
        return self.nq

    def get_task_sequence_of_neem(self, neem_id: int) -> NeemQuery:
        """
        Get the task tree of a plan of a certain neem.
        :param neem_id: The id in (ID) column of the Neems table.
        :return: The task tree of a single neem as a neem query.
        """
        # noinspection PyTypeChecker
        self.get_task_sequence().join_neems().filter(Neem.ID == neem_id)
        return self.nq

    def get_task_sequence(self) -> NeemQuery:
        """
        Get the task tree of all the plans in the database.
        :return: The task tree as a neem query.
        """
        self.nq.reset()
        # noinspection PyTypeChecker
        (self.nq.select_task_type().select_time_columns().select(DulExecutesTask.neem_id).
         select_from_tasks().
         join_task_types().
         join_task_time_interval().
         order_by(SomaHasIntervalBegin.o))
        return self.nq

    def get_neems_containing_task(self, task: str,
                                  regexp: Optional[bool] = False) -> NeemQuery:
        """
        Get the NEEMs that have a certain task.
        :param task: the task name.
        :param regexp: whether to use regular expressions or not.
        :return: the query.
        """
        self.nq.reset()
        (self.nq.select_participant_type().select_tf_columns().select_tf_transform_columns().
         select_from_tasks().
         join_task_types().filter_by_task_type(task, regexp=regexp).
         join_all_task_participants_data(is_outer=True).
         join_task_time_interval().
         join_tf_on_time_interval().join_tf_transfrom().
         join_neems().join_neems_environment()
         .order_by_stamp())
        return self.nq

    def filter_by_neem_id(self, df: pd.DataFrame, neem_id: str) -> pd.DataFrame:
        """
        Get the data of a certain NEEM from a DataFrame
        :param df: the DataFrame which has all the NEEMs data.
        :param neem_id: the NEEM ID.
        :return: the data of the NEEM.
        """
        return self.filter_dataframe(df, {CL.neem_id.value: neem_id})

    @staticmethod
    def get_neem_ids(df, unique: Optional[bool] = True) -> List[str]:
        """
        Get the NEEM IDs from a DataFrame
        :param df: the DataFrame which has all the NEEMs data.
        :param unique: whether to return unique NEEM IDs or not.
        :return: the NEEM IDs.
        """
        if unique:
            return df[CL.neem_id.value].unique().tolist()
        else:
            return df[CL.neem_id.value].tolist()

    def get_participants_per_neem(self, df: pd.DataFrame, unique: Optional[bool] = True) -> List[
        Tuple[str, str]]:
        """
        Get the participant_types in each NEEM
        :param df: the DataFrame which has all the NEEMs data.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in each NEEM.
        """
        neem_ids = self.get_neem_ids(df)
        participants_per_neem = []
        for neem_id in neem_ids:
            neem_df = self.filter_by_neem_id(df, neem_id)
            participants = self.get_participants(neem_df, unique)
            participants_per_neem.extend([(neem_id, p) for p in participants])
        return participants_per_neem

    @staticmethod
    def get_participants(df: pd.DataFrame, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participant_types in a certain NEEM
        :param df: the DataFrame which has the neem data.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in the NEEM.
        """
        if unique:
            return df[CL.participant.value].unique().tolist()
        else:
            return df[CL.participant.value].tolist()

    def filter_by_participant_type(self, df: pd.DataFrame, participant_type: str) -> pd.DataFrame:
        """
        Get the data of a certain participant type from a DataFrame
        :param df: the DataFrame which has the neem data.
        :param participant_type: the participant type.
        :return: the data of the participant type.
        """
        return self.filter_dataframe(df, {CL.participant_type.value: participant_type})

    @staticmethod
    def get_environment(df: pd.DataFrame) -> List[str]:
        """
        Get the environment in a certain NEEM
        :param df: the DataFrame which has the neem data.
        :return: the environment in the NEEM.
        """
        return df[CL.environment.value].unique().tolist()

    def filter_by_participant(self, df: pd.DataFrame, participant: str) -> pd.DataFrame:
        """
        Get the data of a certain participant from a DataFrame
        :param df: the DataFrame which has the neem data.
        :param participant: the participant.
        :return: the data of the participant.
        """
        return self.filter_dataframe(df, {CL.participant.value: participant})

    def filter_by_task(self, neem_df: pd.DataFrame, task: str) -> pd.DataFrame:
        """
        Get the data of a certain task from a DataFrame
        :param neem_df: the DataFrame which has the neem data.
        :param task: the task name.
        :return: the data of the task.
        """
        return self.filter_dataframe(neem_df, {CL.task.value: task})

    def filter_dataframe(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        """
        Filter a DataFrame by a dictionary of filters
        :param df: the DataFrame to filter.
        :param filters: the filters to apply.
        :return: the filtered DataFrame.
        """
        indices = self.get_indices(df, filters)
        return df[indices]

    @staticmethod
    def get_indices(df: pd.DataFrame, filters: dict) -> pd.Series:
        """
        Get the indices for a DataFrame by a dictionary of filters
        :param df: the DataFrame to filter.
        :param filters: the filters to apply.
        :return: the indices for the filtered DataFrame.
        """
        initial_condition = True
        indices = None
        for column, value in filters.items():
            if initial_condition:
                indices = df[column] == value
                initial_condition = False
            else:
                indices = indices & (df[column] == value)
        return indices

    def normalize_time(self, neem_df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize the time in a NEEM DataFrame
        :param neem_df: the DataFrame which has the neem data.
        :return: the normalized DataFrame.
        """
        neem_df[CL.stamp.value] = neem_df[CL.stamp.value] - neem_df[CL.stamp.value].min()
        return neem_df

    def get_stamp(self, df: pd.DataFrame) -> List[float]:
        """
        Get times from a DataFrame
        :param df: the DataFrame to yield times from.
        :return: the time stamps as a list.
        """
        return df[CL.stamp.value].tolist()

    def get_child_frame_id(self, df: pd.DataFrame) -> List[str]:
        """
        Get child_frame_id from a DataFrame
        :param df: the DataFrame to yield child_frame_id from.
        :return: the child_frame_ids as a list.
        """
        return df[CL.child_frame_id.value].tolist()

    def get_frame_id(self, df: pd.DataFrame) -> List[str]:
        """
        Get frame_id from a DataFrame
        :param df: the DataFrame to yield frame_id from.
        :return: the frame_ids as a list.
        """
        return df[CL.frame_id.value].tolist()

    def get_positions(self, df: pd.DataFrame) -> Tuple[List[float], List[float], List[float]]:
        """
        Get positions from a DataFrame
        :param df: the DataFrame to yield positions from.
        :return: the positions as 3 lists for x, y, and z values.
        """
        return (df[CL.translation_x.value].tolist(), df[CL.translation_y.value].tolist(),
                df[CL.translation_z.value].tolist())

    def get_orientations(self, df: pd.DataFrame) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Get orientations from a DataFrame
        :param df: the DataFrame to yield orientations from.
        :return: the orientations.
        """
        return (df[CL.orientation_x.value].tolist(), df[CL.orientation_y.value].tolist(),
                df[CL.orientation_z.value].tolist(),
                df[CL.orientation_w.value].tolist())
