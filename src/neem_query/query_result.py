from typing import List, Tuple, Optional
import pandas as pd
from .enums import ColumnLabel as CL


class QueryResult:
    """
    A class to hold and process the result of a query in a pandas DataFrame.
    """
    pd.set_option('display.float_format', lambda x: '%.3f' % x)
    pd.set_option('display.max_columns', None)

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the query result.
        :param df: the DataFrame to hold the query result.
        """
        self.df = df

    def filter_by_neem_id(self, neem_id: str) -> 'QueryResult':
        """
        Get the data of a certain NEEM from the query result DataFrame
        :param neem_id: the NEEM ID.
        :return: the data of the NEEM.
        """
        return self.filter_dataframe({CL.neem_id.value: neem_id})

    def filter_by_participant_type(self, participant_type: str) -> 'QueryResult':
        """
        Get the data of a certain participant type from the query result DataFrame.
        :param participant_type: the participant type.
        :return: the data of the participant type.
        """
        return self.filter_dataframe({CL.participant_type.value: participant_type})

    def filter_by_participant(self, participant: str) -> 'QueryResult':
        """
        Get the data of a certain participant from the query result DataFrame.
        :param participant: the participant.
        :return: the data of the participant.
        """
        return self.filter_dataframe({CL.participant.value: participant})

    def filter_by_task(self, task: str) -> 'QueryResult':
        """
        Get the data of a certain task from the query result DataFrame.
        :param task: the task name.
        :return: the data of the task.
        """
        return self.filter_dataframe({CL.task.value: task})

    def filter_dataframe(self, filters: dict) -> 'QueryResult':
        """
        Filter a DataFrame by a dictionary of filters
        :param filters: the filters to apply.
        :return: the filtered DataFrame.
        """
        indices = self.get_indices(filters)
        return QueryResult(self.df[indices])

    def get_indices(self, filters: dict) -> pd.Series:
        """
        Get the indices for the query result dataframe by a dictionary of filters
        :param filters: the filters to apply.
        :return: the indices for the filtered DataFrame.
        """
        initial_condition = True
        indices = None
        for column, value in filters.items():
            if initial_condition:
                indices = self.df[column] == value
                initial_condition = False
            else:
                indices = indices & (self.df[column] == value)
        return indices

    def normalize_time(self) -> 'QueryResult':
        """
        Normalize the time in the query result DataFrame.
        :return: the normalized DataFrame.
        """
        self.df[CL.stamp.value] = self.df[CL.stamp.value] - self.df[CL.stamp.value].min()
        return QueryResult(self.df.copy())

    def get_neem_ids(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the NEEM IDs from the query result DataFrame.
        :param unique: whether to return unique NEEM IDs or not.
        :return: the NEEM IDs.
        """
        if unique:
            return self.df[CL.neem_id.value].unique().tolist()
        else:
            return self.df[CL.neem_id.value].tolist()

    def get_participants_per_neem(self, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get the participant_types in each NEEM from the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in each NEEM.
        """
        neem_ids = self.get_neem_ids()
        participants_per_neem = []
        for neem_id in neem_ids:
            participants = self.filter_by_neem_id(neem_id).get_participants(unique)
            participants_per_neem.extend([(neem_id, p) for p in participants])
        return participants_per_neem

    def get_participants(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participant_types in the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in the NEEM.
        """
        if unique:
            return self.df[CL.participant.value].unique().tolist()
        else:
            return self.df[CL.participant.value].tolist()

    def get_environments(self) -> List[str]:
        """
        Get the environments in the query result DataFrame.
        :return: the environment in the NEEM.
        """
        return self.df[CL.environment.value].unique().tolist()

    def get_stamp(self) -> List[float]:
        """
        Get times from the query result DataFrame.
        :return: the time stamps as a list.
        """
        return self.df[CL.stamp.value].tolist()

    def get_child_frame_id(self) -> List[str]:
        """
        Get child_frame_id from the query result DataFrame.
        :return: the child_frame_ids as a list.
        """
        return self.df[CL.child_frame_id.value].tolist()

    def get_frame_id(self) -> List[str]:
        """
        Get frame_id from the query result DataFrame.
        :return: the frame_ids as a list.
        """
        return self.df[CL.frame_id.value].tolist()

    def get_positions(self) -> Tuple[List[float], List[float], List[float]]:
        """
        Get positions from the query result DataFrame.
        :return: the positions as 3 lists for x, y, and z values.
        """
        return (self.df[CL.translation_x.value].tolist(), self.df[CL.translation_y.value].tolist(),
                self.df[CL.translation_z.value].tolist())

    def get_orientations(self) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Get orientations from the query result DataFrame.
        :return: the orientations.
        """
        return (self.df[CL.orientation_x.value].tolist(), self.df[CL.orientation_y.value].tolist(),
                self.df[CL.orientation_z.value].tolist(),
                self.df[CL.orientation_w.value].tolist())
