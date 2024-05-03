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

    def filter_by_neem_id(self, neem_ids: List[str]) -> 'QueryResult':
        """
        Get the data of a certain NEEM from the query result DataFrame
        :param neem_ids: the NEEM IDs.
        :return: the data of the NEEM.
        """
        return self.filter_dataframe({CL.neem_id.value: neem_ids})

    def filter_by_sql_neem_id(self, neem_ids: List[int]) -> 'QueryResult':
        """
        Get the data of certain NEEMs from the query result DataFrame
        :param neem_ids: the NEEM IDs.
        :return: the data of the NEEMs.
        """
        return self.filter_dataframe({CL.neem_sql_id.value: neem_ids})

    def filter_by_participant_type(self, participant_types: List[str]) -> 'QueryResult':
        """
        Get the data of a certain participant type from the query result DataFrame.
        :param participant_types: the participant types.
        :return: the data of the participant type.
        """
        return self.filter_dataframe({CL.participant_type.value: participant_types})

    def filter_by_participant(self, participants: List[str]) -> 'QueryResult':
        """
        Get the data of a certain participant from the query result DataFrame.
        :param participants: the participants.
        :return: the data of the participants.
        """
        return self.filter_dataframe({CL.participant.value: participants})

    def filter_by_performer_type(self, performer_types: List[str]) -> 'QueryResult':
        """
        Get the data of a certain performer type from the query result DataFrame.
        :param performer_types: the performer types.
        :return: the data of the performer types.
        """
        return self.filter_dataframe({CL.is_performed_by_type.value: performer_types})

    def filter_by_performer(self, performers: List[str]) -> 'QueryResult':
        """
        Get the data of a certain performer from the query result DataFrame.
        :param performers: the performers.
        :return: the data of the performers.
        """
        return self.filter_dataframe({CL.is_performed_by.value: performers})

    def filter_by_task(self, tasks: List[str]) -> 'QueryResult':
        """
        Get the data of a certain task from the query result DataFrame.
        :param tasks: the task names.
        :return: the data of the tasks.
        """
        return self.filter_dataframe({CL.task.value: tasks})

    def filter_by_task_type(self, task_types: List[str]) -> 'QueryResult':
        """
        Get the data of certain task types from the query result DataFrame.
        :param task_types: the task types.
        :return: the data of the task types.
        """
        return self.filter_dataframe({CL.task_type.value: task_types})

    def filter_by_subtask(self, subtasks: List[str]) -> 'QueryResult':
        """
        Get the data of certain subtasks from the query result DataFrame.
        :param subtasks: the subtask names.
        :return: the data of the subtasks.
        """
        return self.filter_dataframe({CL.subtask.value: subtasks})

    def filter_by_subtask_type(self, subtask_types: List[str]) -> 'QueryResult':
        """
        Get the data of certain subtask types from the query result DataFrame.
        :param subtask_types: the subtask types.
        :return: the data of the subtask types.
        """
        return self.filter_dataframe({CL.subtask_type.value: subtask_types})

    def filter_by_task_parameter(self, task_parameters: List[str]) -> 'QueryResult':
        """
        Get the data of certain task parameters from the query result DataFrame.
        :param task_parameters: the task parameter.
        :return: the data of the task parameter.
        """
        return self.filter_dataframe({CL.task_parameter.value: task_parameters})

    def filter_by_task_parameter_category(self, task_parameter_categories: List[str]) -> 'QueryResult':
        """
        Get the data of certain task parameter categories from the query result DataFrame.
        :param task_parameter_categories: the task parameter categories.
        :return: the data of the task parameter categories.
        """
        return self.filter_dataframe({CL.task_parameter_classification.value: task_parameter_categories})

    def filter_by_task_parameter_type(self, task_parameter_types: List[str]) -> 'QueryResult':
        """
        Get the data of certain task parameter types from the query result DataFrame.
        :param task_parameter_types: the task parameter types.
        :return: the data of the task parameter types.
        """
        return self.filter_dataframe({CL.task_parameter_classification_type.value: task_parameter_types})

    def filter_by_agent(self, agents: List[str]) -> 'QueryResult':
        """
        Get the data of certain agents from the query result DataFrame.
        :param agents: the agents.
        :return: the data of the agents.
        """
        return self.filter_dataframe({CL.agent.value: agents})

    def filter_by_agent_type(self, agent_types: List[str]) -> 'QueryResult':
        """
        Get the data of certain agent types from the query result DataFrame.
        :param agent_types: the agent types.
        :return: the data of the agent types.
        """
        return self.filter_dataframe({CL.agent_type.value: agent_types})

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
        for column, values in filters.items():
            new_indices = np.logical_or.reduce([self.df[column] == v for v in values])
            if initial_condition:
                indices = new_indices
                initial_condition = False
            else:
                indices = indices & new_indices
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
        return self.get_column_values(CL.neem_id.value, unique=unique)

    def get_participants_per_neem(self, unique: Optional[bool] = True,
                                  drop_na: Optional[bool] = False) -> List[Tuple[int, str]]:
        """
        Get the participants in each NEEM from the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :param drop_na: whether to drop None values or not.
        :return: the participants in each NEEM.
        """
        return self.get_column_value_per_neem(CL.participant.value, unique=unique, drop_na=drop_na)

    def get_participant_types_per_neem(self, unique: Optional[bool] = True,
                                       drop_na: Optional[bool] = False) -> List[Tuple[int, str]]:
        """
        Get the participant_types in each NEEM from the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :param drop_na: whether to drop None values or not.
        :return: the participant_types in each NEEM.
        """
        return self.get_column_value_per_neem(CL.participant_type.value, unique=unique, drop_na=drop_na)

    def get_agents_per_neem(self, unique: Optional[bool] = True,
                            drop_na: Optional[bool] = False) -> List[Tuple[int, str]]:
        """
        Get the agents in each NEEM from the query result DataFrame.
        :param unique: whether to return unique agents or not.
        :param drop_na: whether to drop None values or not.
        :return: the agents in each NEEM.
        """
        return self.get_column_value_per_neem(CL.agent.value, unique=unique, drop_na=drop_na)

    def get_agent_types_per_neem(self, unique: Optional[bool] = True,
                                 drop_na: Optional[bool] = False) -> List[Tuple[int, str]]:
        """
        Get the agent_types in each NEEM from the query result DataFrame.
        :param unique: whether to return unique agent_types or not.
        :param drop_na: whether to drop None values or not.
        :return: the agent_types in each NEEM.
        """
        return self.get_column_value_per_neem(CL.agent_type.value, unique=unique, drop_na=drop_na)

    def get_column_value_per_neem(self, entity: str, unique: Optional[bool] = True,
                                  drop_na: Optional[bool] = False) -> List[Tuple[int, str]]:
        """
        Get a specific entity (participant, agent, ...etc.) in each NEEM from the query result DataFrame.
        :param entity: the entity to get.
        :param unique: whether to return unique entities or not.
        :param drop_na: whether to drop None values or not.
        :return: the entities in each NEEM.
        """
        sql_neem_ids = self.get_sql_neem_ids(unique=True)
        entities_per_neem = []
        for sql_neem_id in sql_neem_ids:
            entities = (self.filter_by_sql_neem_id([sql_neem_id]).
                        get_column_values(entity, unique=unique, drop_na=drop_na))
            entities_per_neem.extend([(sql_neem_id, neem_entity) for neem_entity in entities])
        return entities_per_neem

    def get_participants(self, unique: Optional[bool] = True, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the participants in the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :param drop_na: whether to drop None values or not.
        :return: the participants in the NEEM.
        """
        return self.get_column_values(CL.participant.value, unique=unique, drop_na=drop_na)

    def get_participant_types(self, unique: Optional[bool] = True, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the participant_types in the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :param drop_na: whether to drop None values or not.
        :return: the participant_types in the NEEM.
        """
        return self.get_column_values(CL.participant_type.value, unique=unique, drop_na=drop_na)

    def get_performers(self, unique: Optional[bool] = True, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the performers in the query result DataFrame.
        :param unique: whether to return unique performers or not.
        :param drop_na: whether to drop None values or not.
        :return: the performers in the NEEM.
        """
        return self.get_column_values(CL.is_performed_by.value, unique=unique, drop_na=drop_na)

    def get_performer_types(self, unique: Optional[bool] = True, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the performer_types in the query result DataFrame.
        :param unique: whether to return unique performer_types or not.
        :param drop_na: whether to drop None values or not.
        :return: the performer_types in the NEEM.
        """
        return self.get_column_values(CL.is_performed_by_type.value, unique=unique, drop_na=drop_na)

    def get_environments(self, unique: Optional[bool] = True, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the environments in the query result DataFrame.
        :param unique: whether to return unique environments or not.
        :param drop_na: whether to drop None values or not.
        :return: the environment in the NEEM.
        """
        return self.get_column_values(CL.environment.value, unique=unique, drop_na=drop_na)

    def get_participant_stamp(self) -> List[float]:
        """
        Get times from the query result DataFrame.
        :return: the time stamps as a list.
        """
        return self.df[CL.participant_stamp.value].tolist()

    def get_participant_child_frame_id(self) -> List[str]:
        """
        Get child_frame_id from the query result DataFrame.
        :return: the child_frame_ids as a list.
        """
        return self.df[CL.participant_child_frame_id.value].tolist()

    def get_participant_frame_id(self) -> List[str]:
        """
        Get frame_id from the query result DataFrame.
        :return: the frame_ids as a list.
        """
        return self.df[CL.participant_frame_id.value].tolist()

    def get_participant_positions(self) -> Tuple[List[float], List[float], List[float]]:
        """
        Get positions from the query result DataFrame.
        :return: the positions as 3 lists for x, y, and z values.
        """
        return (self.df[CL.participant_translation_x.value].tolist(), self.df[CL.participant_translation_y.value].tolist(),
                self.df[CL.participant_translation_z.value].tolist())

    def get_participant_orientations(self) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Get orientations from the query result DataFrame.
        :return: the orientations.
        """
        return (self.df[CL.participant_orientation_x.value].tolist(), self.df[CL.participant_orientation_y.value].tolist(),
                self.df[CL.participant_orientation_z.value].tolist(),
                self.df[CL.participant_orientation_w.value].tolist())

    def get_performer_stamp(self) -> List[float]:
        """
        Get times from the query result DataFrame.
        :return: the time stamps as a list.
        """
        return self.df[CL.performer_stamp.value].tolist()

    def get_performer_child_frame_id(self) -> List[str]:
        """
        Get child_frame_id from the query result DataFrame.
        :return: the child_frame_ids as a list.
        """
        return self.df[CL.performer_child_frame_id.value].tolist()

    def get_performer_frame_id(self) -> List[str]:
        """
        Get frame_id from the query result DataFrame.
        :return: the frame_ids as a list.
        """
        return self.df[CL.performer_frame_id.value].tolist()

    def get_performer_positions(self) -> Tuple[List[float], List[float], List[float]]:
        """
        Get positions from the query result DataFrame.
        :return: the positions as 3 lists for x, y, and z values.
        """
        return (self.df[CL.performer_translation_x.value].tolist(), self.df[CL.performer_translation_y.value].tolist(),
                self.df[CL.performer_translation_z.value].tolist())

    def get_performer_orientations(self) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Get orientations from the query result DataFrame.
        :return: the orientations.
        """
        return (self.df[CL.performer_orientation_x.value].tolist(), self.df[CL.performer_orientation_y.value].tolist(),
                self.df[CL.performer_orientation_z.value].tolist(),
                self.df[CL.performer_orientation_w.value].tolist())

    def get_all_subtask_types_of_task_type(self, task_type: str,
                                           unique: Optional[bool] = True,
                                           drop_na: Optional[bool] = False) -> List[str]:
        """
        Get all subtasks of a certain task type from the query result DataFrame.
        :param task_type: the task type.
        :param unique: whether to return unique subtasks or not.
        :param drop_na: whether to drop None values or not.
        :return: the subtasks.
        """
        return self.filter_by_task_type([task_type]).get_subtask_types(unique=unique, drop_na=drop_na)

    def get_all_task_types_of_subtask_type(self, subtask_type: str,
                                           unique: Optional[bool] = True,
                                           drop_na: Optional[bool] = False) -> List[str]:
        """
        Get all task types of a certain subtask type from the query result DataFrame.
        :param subtask_type: the subtask type.
        :param unique: whether to return unique task types or not.
        :param drop_na: whether to drop None values or not.
        :return: the task types.
        """
        return self.filter_by_subtask([subtask_type]).get_task_types(unique=unique, drop_na=drop_na)

    def get_tasks(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the tasks in the query result DataFrame.
        :param unique: whether to return unique tasks or not.
        :param drop_na: whether to drop None values or not.
        :return: the tasks in the NEEM.
        """
        return self.get_column_values(CL.task.value, unique=unique, drop_na=drop_na)

    def get_task_types(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the task types in the query result DataFrame.
        :param unique: whether to return unique task types or not.
        :param drop_na: whether to drop None values or not.
        :return: the task types in the NEEM.
        """
        return self.get_column_values(CL.task_type.value, unique=unique, drop_na=drop_na)

    def get_subtasks(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the subtasks in the query result DataFrame.
        :param unique: whether to return unique subtasks or not.
        :param drop_na: whether to drop None values or not.
        :return: the subtasks in the NEEM.
        """
        return self.get_column_values(CL.subtask.value, unique=unique, drop_na=drop_na)

    def get_subtask_types(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the subtask types in the query result DataFrame.
        :param unique: whether to return unique subtask types or not.
        :param drop_na: whether to drop None values or not.
        :return: the subtask types in the NEEM.
        """
        return self.get_column_values(CL.subtask_type.value, unique=unique, drop_na=drop_na)

    def get_task_parameters(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the task parameters in the query result DataFrame.
        :param unique: whether to return unique task parameters or not.
        :param drop_na: whether to drop None values or not.
        :return: the task parameters in the NEEM.
        """
        return self.get_column_values(CL.task_parameter.value, unique=unique, drop_na=drop_na)

    def get_task_parameter_categories(self, unique: Optional[bool] = False,
                                      drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the task parameter categories in the query result DataFrame.
        :param unique: whether to return unique task parameter categories or not.
        :param drop_na: whether to drop None values or not.
        :return: the task parameter categories in the NEEM.
        """
        return self.get_column_values(CL.task_parameter_classification.value, unique=unique, drop_na=drop_na)

    def get_task_parameter_types(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the task parameter types in the query result DataFrame.
        :param unique: whether to return unique task parameter types or not.
        :param drop_na: whether to drop None values or not.
        :return: the task parameter types in the NEEM.
        """
        return self.get_column_values(CL.task_parameter_classification_type.value, unique=unique, drop_na=drop_na)

    def get_time_intervals(self, unique: Optional[bool] = False) -> List[str]:
        """
        Get the time intervals in the query result DataFrame.
        :param unique: whether to return unique time intervals or not.
        :return: the time intervals in the NEEM.
        """
        return self.get_column_values(CL.time_interval.value, unique)

    def get_time_interval_begin(self) -> List[float]:
        """
        Get the time interval begin in the query result DataFrame.
        :return: the time interval begin in the NEEM.
        """
        return self.get_column_values(CL.time_interval_begin.value, False)

    def get_time_interval_end(self) -> List[float]:
        """
        Get the time interval end in the query result DataFrame.
        :return: the time interval end in the NEEM.
        """
        return self.get_column_values(CL.time_interval_end.value, False)

    def get_agents(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the agents in the query result DataFrame.
        :param unique: whether to return unique agents or not.
        :param drop_na: whether to drop None values or not.
        :return: the agents in the NEEM.
        """
        return self.get_column_values(CL.agent.value, unique=unique, drop_na=drop_na)

    def get_agent_types(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the agent types in the query result DataFrame.
        :param unique: whether to return unique agent types or not.
        :param drop_na: whether to drop None values or not.
        :return: the agent types in the NEEM.
        """
        return self.get_column_values(CL.agent_type.value, unique=unique, drop_na=drop_na)

    def get_tasks_of_agent(self, agent: str, unique: Optional[bool] = False,
                           drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the tasks of a certain agent from the query result DataFrame.
        :param agent: the agent name.
        :param unique: whether to return unique tasks or not.
        :param drop_na: whether to drop None values or not.
        :return: the tasks.
        """
        return self.filter_by_participant([agent]).get_tasks(unique=unique, drop_na=drop_na)

    def get_task_is_performed_by(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the task performers in the query result DataFrame.
        :param unique: whether to return unique task performers or not.
        :param drop_na: whether to drop None values or not.
        :return: the task performers in the NEEM.
        """
        return self.get_column_values(CL.is_performed_by.value, unique=unique, drop_na=drop_na)

    def get_object_mesh_path(self, unique: Optional[bool] = False, drop_na: Optional[bool] = False) -> List[str]:
        """
        Get the object mesh path in the query result DataFrame.
        :param unique: whether to return unique object mesh path or not.
        :param drop_na: whether to drop None values or not.
        :return: the object mesh path in the NEEM.
        """
        return self.get_column_values(CL.object_mesh_path.value, unique=unique, drop_na=drop_na)

    def get_column_values(self, column: str, unique: Optional[bool] = False,
                          drop_na: Optional[bool] = False) -> List[Any]:
        """
        Get a column from the query result DataFrame.
        :param column: the column to get.
        :param unique: whether to return unique values or not.
        :param drop_na: whether to drop None values or not.
        :return: the column values.
        """
        if unique:
            # filter from none values
            return self.df[column].dropna().unique().tolist()
        else:
            if drop_na:
                return self.df[column].dropna().tolist()
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

    def get_sql_neem_ids(self, unique: Optional[bool] = False):
        """
        Get the SQL NEEM IDs from the query result DataFrame.
        :param unique: whether to return unique SQL NEEM IDs or not.
        :return: the SQL NEEM IDs.
        """
        return self.get_column_values(CL.neem_sql_id.value, unique=unique)

    def get_task_start_time(self, task: str, sql_neem_id: int) -> float:
        """
        Get the start time of a task in a certain NEEM.
        :param task: the task name.
        :param sql_neem_id: the SQL NEEM ID.
        :return: the start time.
        """
        return self.filter_by_sql_neem_id([sql_neem_id]).filter_by_task([task]).get_time_interval_begin()[0]

    def get_task_end_time(self, task: str, sql_neem_id: int) -> float:
        """
        Get the end time of a task in a certain NEEM.
        :param task: the task name.
        :param sql_neem_id: the SQL NEEM ID.
        :return: the end time.
        """
        return self.filter_by_sql_neem_id([sql_neem_id]).filter_by_task([task]).get_time_interval_end()[0]

    def order_by(self, df_column_name: str):
        """
        Order the query result DataFrame by a certain column.
        :param df_column_name: the column name to order by.
        """
        self.df = self.df.sort_values(by=df_column_name)
        return self
