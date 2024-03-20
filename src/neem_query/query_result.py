from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
from typing_extensions import Any

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

    def filter_by_task_type(self, task_type: str) -> 'QueryResult':
        """
        Get the data of a certain task type from the query result DataFrame.
        :param task_type: the task type.
        :return: the data of the task type.
        """
        return self.filter_dataframe({CL.task_type.value: task_type})

    def filter_by_subtask(self, subtask: str) -> 'QueryResult':
        """
        Get the data of a certain subtask from the query result DataFrame.
        :param subtask: the subtask name.
        :return: the data of the subtask.
        """
        return self.filter_dataframe({CL.subtask.value: subtask})

    def filter_by_subtask_type(self, subtask_type: str) -> 'QueryResult':
        """
        Get the data of a certain subtask type from the query result DataFrame.
        :param subtask_type: the subtask type.
        :return: the data of the subtask type.
        """
        return self.filter_dataframe({CL.subtask_type.value: subtask_type})

    def filter_by_task_parameter(self, task_parameter: str) -> 'QueryResult':
        """
        Get the data of a certain task parameter from the query result DataFrame.
        :param task_parameter: the task parameter.
        :return: the data of the task parameter.
        """
        return self.filter_dataframe({CL.task_parameter.value: task_parameter})

    def filter_by_task_parameter_category(self, task_parameter_category: str) -> 'QueryResult':
        """
        Get the data of a certain task parameter category from the query result DataFrame.
        :param task_parameter_category: the task parameter category.
        :return: the data of the task parameter category.
        """
        return self.filter_dataframe({CL.task_parameter_category.value: task_parameter_category})

    def filter_by_task_parameter_type(self, task_parameter_type: str) -> 'QueryResult':
        """
        Get the data of a certain task parameter type from the query result DataFrame.
        :param task_parameter_type: the task parameter type.
        :return: the data of the task parameter type.
        """
        return self.filter_dataframe({CL.task_parameter_type.value: task_parameter_type})

    def filter_by_agent(self, agent: str) -> 'QueryResult':
        """
        Get the data of a certain agent from the query result DataFrame.
        :param agent: the agent.
        :return: the data of the agent.
        """
        return self.filter_dataframe({CL.agent.value: agent})

    def filter_by_agent_type(self, agent_type: str) -> 'QueryResult':
        """
        Get the data of a certain agent type from the query result DataFrame.
        :param agent_type: the agent type.
        :return: the data of the agent type.
        """
        return self.filter_dataframe({CL.agent_type.value: agent_type})

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
        return self.get_column_values(CL.neem_id.value, unique)

    def get_participants_per_neem(self, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get the participants in each NEEM from the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :return: the participants in each NEEM.
        """
        return self.get_column_value_per_neem(CL.participant.value, unique)

    def get_participant_types_per_neem(self, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get the participant_types in each NEEM from the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in each NEEM.
        """
        return self.get_column_value_per_neem(CL.participant_type.value, unique)

    def get_agents_per_neem(self, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get the agents in each NEEM from the query result DataFrame.
        :param unique: whether to return unique agents or not.
        :return: the agents in each NEEM.
        """
        return self.get_column_value_per_neem(CL.agent.value, unique)

    def get_agent_types_per_neem(self, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get the agent_types in each NEEM from the query result DataFrame.
        :param unique: whether to return unique agent_types or not.
        :return: the agent_types in each NEEM.
        """
        return self.get_column_value_per_neem(CL.agent_type.value, unique)

    def get_column_value_per_neem(self, entity: str, unique: Optional[bool] = True) -> List[Tuple[str, str]]:
        """
        Get a specific entity (participant, agent, ...etc.) in each NEEM from the query result DataFrame.
        :param entity: the entity to get.
        :param unique: whether to return unique entities or not.
        :return: the entities in each NEEM.
        """
        neem_ids = self.get_neem_ids()
        entities_per_neem = []
        for neem_id in neem_ids:
            entities = self.filter_by_neem_id(neem_id).get_column_values(entity, unique)
            entities_per_neem.extend([(neem_id, neem_entity) for neem_entity in entities])
        return entities_per_neem

    def get_participants(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participants in the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :return: the participants in the NEEM.
        """
        return self.get_column_values(CL.participant.value, unique)

    def get_participant_types(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participant_types in the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in the NEEM.
        """
        return self.get_column_values(CL.participant_type.value, unique)

    def get_environments(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the environments in the query result DataFrame.
        :param unique: whether to return unique environments or not.
        :return: the environment in the NEEM.
        """
        return self.get_column_values(CL.environment.value, unique)

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

    def get_all_subtask_types_of_task_type(self, task_type: str, unique: Optional[bool] = True) -> List[str]:
        """
        Get all subtasks of a certain task type from the query result DataFrame.
        :param task_type: the task type.
        :param unique: whether to return unique subtasks or not.
        :return: the subtasks.
        """
        return self.filter_by_task_type(task_type).get_subtask_types(unique=unique)

    def get_all_task_types_of_subtask_type(self, subtask_type: str, unique: Optional[bool] = True) -> List[str]:
        """
        Get all task types of a certain subtask type from the query result DataFrame.
        :param subtask_type: the subtask type.
        :param unique: whether to return unique task types or not.
        :return: the task types.
        """
        return self.filter_by_subtask(subtask_type).get_task_types(unique=unique)

    def get_tasks(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the tasks in the query result DataFrame.
        :param unique: whether to return unique tasks or not.
        :return: the tasks in the NEEM.
        """
        return self.get_column_values(CL.task.value, unique)

    def get_task_types(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the task types in the query result DataFrame.
        :param unique: whether to return unique task types or not.
        :return: the task types in the NEEM.
        """
        return self.get_column_values(CL.task_type.value, unique)

    def get_subtasks(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the subtasks in the query result DataFrame.
        :param unique: whether to return unique subtasks or not.
        :return: the subtasks in the NEEM.
        """
        return self.get_column_values(CL.subtask.value, unique)

    def get_subtask_types(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the subtask types in the query result DataFrame.
        :param unique: whether to return unique subtask types or not.
        :return: the subtask types in the NEEM.
        """
        return self.get_column_values(CL.subtask_type.value, unique)

    def get_task_parameters(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the task parameters in the query result DataFrame.
        :param unique: whether to return unique task parameters or not.
        :return: the task parameters in the NEEM.
        """
        return self.get_column_values(CL.task_parameter.value, unique)

    def get_task_parameter_categories(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the task parameter categories in the query result DataFrame.
        :param unique: whether to return unique task parameter categories or not.
        :return: the task parameter categories in the NEEM.
        """
        return self.get_column_values(CL.task_parameter_category.value, unique)

    def get_task_parameter_types(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the task parameter types in the query result DataFrame.
        :param unique: whether to return unique task parameter types or not.
        :return: the task parameter types in the NEEM.
        """
        return self.get_column_values(CL.task_parameter_type.value, unique)

    def get_time_intervals(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the time intervals in the query result DataFrame.
        :param unique: whether to return unique time intervals or not.
        :return: the time intervals in the NEEM.
        """
        return self.get_column_values(CL.time_interval.value, unique)

    def get_time_interval_begin(self) -> List[str]:
        """
        Get the time interval begin in the query result DataFrame.
        :return: the time interval begin in the NEEM.
        """
        return self.get_column_values(CL.time_interval_begin.value, False)

    def get_time_interval_end(self) -> List[str]:
        """
        Get the time interval end in the query result DataFrame.
        :return: the time interval end in the NEEM.
        """
        return self.get_column_values(CL.time_interval_end.value, False)

    def get_agents(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the agents in the query result DataFrame.
        :param unique: whether to return unique agents or not.
        :return: the agents in the NEEM.
        """
        return self.get_column_values(CL.agent.value, unique)

    def get_agent_types(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the agent types in the query result DataFrame.
        :param unique: whether to return unique agent types or not.
        :return: the agent types in the NEEM.
        """
        return self.get_column_values(CL.agent_type.value, unique)

    def get_tasks_of_agent(self, agent: str, unique: Optional[bool] = False) -> List[str]:
        """
        Get the tasks of a certain agent from the query result DataFrame.
        :param agent: the agent name.
        :param unique: whether to return unique tasks or not.
        :return: the tasks.
        """
        return self.filter_by_participant(agent).get_tasks(unique)

    def get_column_values(self, column: str, unique: Optional[bool] = False) -> List[Any]:
        """
        Get a column from the query result DataFrame.
        :param column: the column to get.
        :param unique: whether to return unique values or not.
        :return: the column values.
        """
        if unique:
            return self.df[column].unique().tolist()
        else:
            return self.df[column].tolist()

    def get_multi_column_values(self, columns: List[str], unique: Optional[bool] = False) -> np.ndarray:
        """
        Get multiple columns from the query result DataFrame.
        :param columns: the columns to get.
        :param unique: whether to return unique values or not.
        :return: the column values.
        """
        if unique:
            return self.df[columns].drop_duplicates().values
        else:
            return self.df[columns].values

    def get_columns(self) -> List[str]:
        """
        Get the columns of the query result DataFrame.
        :return: the columns.
        """
        return self.df.columns.tolist()
