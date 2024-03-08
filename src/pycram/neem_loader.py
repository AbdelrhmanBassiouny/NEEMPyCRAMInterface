import pandas as pd
from sqlalchemy import Engine, text


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
