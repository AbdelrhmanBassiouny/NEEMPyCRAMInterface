import pandas as pd
import rospy
from sqlalchemy import Engine, text
<<<<<<< HEAD
=======
from typing_extensions import List, Iterable, Tuple, Optional

from .datastructures.pose import Pose, Transform
>>>>>>> 6fa0ced... [NEEM2PyCRAM]


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


<<<<<<< HEAD
class NEEMLoader:
    """
    A class to load data from a NEEM stored in a SQL database.
    """

    def __init__(self, engine: Engine, sql_query: str, neem_id: str):
        """
        Create a NEEMLoader
        :param engine: the SQLAlchemy engine to use.
        :param sql_query: the SQL query.
        :param neem_id: the NEEM ID.
        """
        self.engine = engine
        all_neems_df = get_dataframe_from_sql_query(sql_query, engine)
        self.df = self.get_data_of_certain_neem(all_neems_df, neem_id)

    @classmethod
    def from_sql_query_file(cls, engine: Engine, sql_filename: str, neem_id: str) -> 'NEEMLoader':
        """
        Create a NEEMLoader from a SQL file
        :param engine: the SQLAlchemy engine to use.
        :param sql_filename: the name of the SQL file.
        :param neem_id: the NEEM ID.
        """
        sql_query = get_sql_query_from_file(sql_filename)
        return cls(engine, sql_query, neem_id)

    @staticmethod
    def get_data_of_certain_neem(all_neems_df: pd.DataFrame, neem_id: str) -> pd.DataFrame:
        """
        Get the data of a certain NEEM from a DataFrame
        :param all_neems_df: the DataFrame which has all the NEEMs data.
        :param neem_id: the NEEM ID.
        :return: the data of the NEEM.
        """
        neem_indices = all_neems_df['neem_id'] == neem_id
        return all_neems_df[neem_indices]

    @staticmethod
    def get_participants(neem_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get the participants in a certain NEEM
        :param neem_df: the DataFrame which has the neem data.
        :return: the participants in the NEEM.
        """
        return neem_df['participant'].unique()
=======
def filter_by_neem_id(all_neems_df: pd.DataFrame, neem_id: str) -> pd.DataFrame:
    """
    Get the data of a certain NEEM from a DataFrame
    :param all_neems_df: the DataFrame which has all the NEEMs data.
    :param neem_id: the NEEM ID.
    :return: the data of the NEEM.
    """
    return filter_dataframe(all_neems_df, {'neem_id': neem_id})


def get_neem_ids(all_neems_df: pd.DataFrame) -> List[str]:
    """
    Get the NEEM IDs from a DataFrame
    :param all_neems_df: the DataFrame which has all the NEEMs data.
    :return: the NEEM IDs.
    """
    return all_neems_df['neem_id'].unique().tolist()


def get_participants(neem_df: pd.DataFrame, unique: Optional[bool] = True) -> List[str]:
    """
    Get the participants in a certain NEEM
    :param neem_df: the DataFrame which has the neem data.
    :param unique: whether to return unique participants or not.
    :return: the participants in the NEEM.
    """
    if unique:
        return neem_df['has_participant'].unique().tolist()
    else:
        return neem_df['has_participant'].tolist()


def filter_by_participant_type(neem_df: pd.DataFrame, participant_type: str) -> pd.DataFrame:
    """
    Get the data of a certain participant type from a DataFrame
    :param neem_df: the DataFrame which has the neem data.
    :param participant_type: the participant type.
    :return: the data of the participant type.
    """
    return filter_dataframe(neem_df, {'participant_type': participant_type})


def get_environment(neem_df: pd.DataFrame) -> List[str]:
    """
    Get the environment in a certain NEEM
    :param neem_df: the DataFrame which has the neem data.
    :return: the environment in the NEEM.
    """
    return neem_df['environment'].unique().tolist()


def filter_by_participant(neem_df: pd.DataFrame, participant: str) -> pd.DataFrame:
    """
    Get the data of a certain participant from a DataFrame
    :param neem_df: the DataFrame which has the neem data.
    :param participant: the participant.
    :return: the data of the participant.
    """
    return filter_dataframe(neem_df, {'has_participant': participant})


def filter_dataframe(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Filter a DataFrame by a dictionary of filters
    :param df: the DataFrame to filter.
    :param filters: the filters to apply.
    :return: the filtered DataFrame.
    """
    indices = get_indices(df, filters)
    return df[indices]


def get_indices(df: pd.DataFrame, filters: dict) -> pd.Series:
    """
    Get the indices for a DataFrame by a dictionary of filters
    :param df: the DataFrame to filter.
    :param filters: the filters to apply.
    :return: the indices for the filtered DataFrame.
    """
    indices = pd.Series([True] * len(df))
    for column, value in filters.items():
        indices = indices & (df[column] == value)
    return indices


def normalize_time(neem_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the time in a NEEM DataFrame
    :param neem_df: the DataFrame which has the neem data.
    :return: the normalized DataFrame.
    """
    neem_df['stamp'] = neem_df['stamp'] - neem_df['stamp'].min()
    return neem_df


def get_transforms(df: pd.DataFrame) -> List[Transform]:
    """
    Get transforms from a DataFrame
    :param df: the DataFrame to get transforms from.
    :return: the transforms as a list.
    """
    position = get_positions(df)
    orientation = get_orientations(df)
    frame_id = get_frame_id(df)
    child_frame_id = get_child_frame_id(df)
    transforms = [Transform([x, y, z], [rx, ry, rz, rw], frame_id, child_frame_id, time=rospy.Time())
                  for x, y, z, rx, ry, rz, rw, frame_id, child_frame_id in
                  zip(*position, *orientation, frame_id, child_frame_id)]
    return transforms


def get_stamp(df: pd.DataFrame) -> List[float]:
    """
    Get times from a DataFrame
    :param df: the DataFrame to yield times from.
    :return: the time stamps as a list.
    """
    return df['stamp'].tolist()


def get_child_frame_id(df: pd.DataFrame) -> List[str]:
    """
    Get child_frame_id from a DataFrame
    :param df: the DataFrame to yield child_frame_id from.
    :return: the child_frame_ids as a list.
    """
    return df['child_frame_id'].tolist()


def get_frame_id(df: pd.DataFrame) -> List[str]:
    """
    Get frame_id from a DataFrame
    :param df: the DataFrame to yield frame_id from.
    :return: the frame_ids as a list.
    """
    return df['frame_id'].tolist()


def get_poses(df: pd.DataFrame) -> List[Pose]:
    """
    Get poses from a DataFrame
    :param df: the DataFrame to yield poses from.
    :return: the poses as a list.
    """
    positions = get_positions(df)
    orientations = get_orientations(df)
    poses = [Pose([x, y, z], [rx, ry, rz, rw])
             for x, y, z, rx, ry, rz, rw in zip(*positions, *orientations)]
    return poses


def get_positions(df: pd.DataFrame) -> Tuple[List[float], List[float], List[float]]:
    """
    Get positions from a DataFrame
    :param df: the DataFrame to yield positions from.
    :return: the positions as 3 lists for x, y, and z values.
    """
    return df['tx'].tolist(), df['ty'].tolist(), df['tz'].tolist()


def get_orientations(df: pd.DataFrame) -> Tuple[List[float], List[float], List[float], List[float]]:
    """
    Get orientations from a DataFrame
    :param df: the DataFrame to yield orientations from.
    :return: the orientations.
    """
    return df['rx'].tolist(), df['ry'].tolist(), df['rz'].tolist(), df['rw'].tolist()
>>>>>>> 6fa0ced... [NEEM2PyCRAM]
