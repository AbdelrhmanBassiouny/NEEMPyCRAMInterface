import logging
import time
from dataclasses import dataclass, astuple

import rospy
from pycram.datastructures.enums import ObjectType
from pycram.datastructures.pose import Pose, Transform
from pycram.designators import action_designator
from pycram.world_concepts.world_object import Object
from typing_extensions import Optional, Dict, Tuple, List, Callable

from .neem_interface import NeemInterface
from .query_result import QueryResult
from .utils import ModuleInspector
from .enums import ColumnLabel as CL


action_designator_inspector = ModuleInspector(action_designator.__name__)
pycram_actions = action_designator_inspector.get_all_classes_dict()

soma_to_pycram = {'soma:Grasping': pycram_actions['GraspingAction'],
                  'soma:PositioningArm': pycram_actions['ParkArmsAction'],
                  'soma:SettingGripper': pycram_actions['SetGripperAction'],
                  'soma:LookingAt': pycram_actions['LookAtAction'],
                  'soma:Releasing': pycram_actions['ReleaseAction'],
                  'soma:PickingUp': pycram_actions['PickUpAction'],
                  'soma:Placing': pycram_actions['PlaceAction'],
                  'soma:Gripping': pycram_actions['GripAction'],
                  'soma:Closing': pycram_actions['CloseAction'],
                  'soma:Opening': pycram_actions['OpenAction'],
                  'soma:OpeningGripper': pycram_actions['SetGripperAction'],
                  'soma:Navigating': pycram_actions['NavigateAction'],
                  'soma:Delivering': pycram_actions['TransportAction'],
                  'soma:Detecting': pycram_actions['DetectAction'],
                  'soma:AssumingArmPose:': pycram_actions['ParkArmsAction'],
                  }

@dataclass
class ReplayNEEMMotionData:
    """
    A data class to hold the data required to replay NEEM motions.
    """
    poses: List[Pose]
    times: List[float]
    participant_instances: List[str]
    neem_ids: List[str]


class PyCRAMNEEMInterface(NeemInterface):
    """
    A class to interface with the NEEM database and PyCRAM.
    """

    def __init__(self, db_url: str):
        """
        Initialize the PyCRAM NEEM interface.
        :param db_url: the URL to the NEEM database.
        """
        super().__init__(db_url)

    def redo_neem_plan(self):
        """
        Redo NEEM actions using PyCRAM. The query should contain:
         neem_id, participant, action, parameters, stamp.
         One could use the get_plan_of_neem to get the data. Then filter it as needed.
        """
        environment_obj, participant_objects = self.get_and_spawn_environment_and_participants()
        agent_objects = self.get_and_spawn_agents()
        tasks = self.query_result.get_column_value_per_neem(CL.task_type.value)
        for neem_id, participant, task, parameters, current_time in zip(self.get_neem_ids(unique=False),
                                                                        self.get_participants(unique=False),
                                                                        tasks,
                                                                        self.query_result.get_column_value_per_neem(CL.task_parameter.value),
                                                                        self.get_stamp()):
            # TODO: Implement neem_task_goal_resolver to get task goal like placing goal.
            # TODO: Create designators for objects.
            if task in soma_to_pycram:
                action = soma_to_pycram[task]
                action_description = action(parameters)
                action_description.ground()
                action_description.resolve()
                action_description.perform()
            else:
                logging.warning(f'No action found for task {task}')

    def replay_neem_motions(self):
        """
        Replay NEEMs using PyCRAM. The query should contain:
         neem_id, environment, participant, translation, orientation, stamp.
         One could use the get_motion_replay_data method to get the data. Then filter it as needed.
        """

        environment_obj, participant_objects = self.get_and_spawn_environment_and_participants()

        motion_data = self.get_motion_data()
        poses = motion_data.poses
        times = motion_data.times
        participant_instances = motion_data.participant_instances
        neem_ids = motion_data.neem_ids

        prev_time = 0
        for neem_id, participant, pose, current_time in zip(neem_ids, participant_instances, poses, times):
            if prev_time > 0:
                wait_time = current_time - prev_time
                if wait_time > 1:
                    wait_time = 1
                time.sleep(wait_time)
            prev_time = current_time
            participant_objects[(neem_id, participant)].set_pose(pose)

    def get_motion_data(self):
        """
        Get motion data required to replay motions from the query result.
        """
        poses = self.get_poses()
        times = self.get_stamp()
        participant_instances = self.get_participants(unique=False)
        neem_ids = self.get_neem_ids(unique=False)
        return ReplayNEEMMotionData(poses, times, participant_instances, neem_ids)

    def get_and_spawn_environment_and_participants(self):
        """
        Get and spawn the environment and participants in the NEEM using PyCRAM.
        """
        environment_obj = self.get_and_spawn_environment()
        participant_objects = self.get_and_spawn_participants()
        return environment_obj, participant_objects

    def get_and_spawn_environment(self) -> Object:
        """
        Get and spawn the environment in the NEEM using PyCRAM.
        :return: the environment as a PyCRAM object.
        """
        environments = self.query_result.get_environments()
        environment_path = self.get_description_of_environment(environments[0])
        return Object(environments[0], ObjectType.ENVIRONMENT, environment_path)

    def get_and_spawn_agents(self) -> Dict[Tuple[str, str], Object]:
        """
        Get and spawn the agents in the NEEM using PyCRAM.
        :return: the agents as a dictionary of PyCRAM objects.
        """
        return self.get_and_spawn_entities(CL.agent.value,
                                           self.get_description_of_agent,
                                           lambda _: ObjectType.ROBOT)

    def get_and_spawn_participants(self) -> Dict[Tuple[str, str], Object]:
        """
        Get and spawn the participants in the NEEM using PyCRAM.
        :return: the participants as a dictionary of PyCRAM objects.
        """
        return self.get_and_spawn_entities(CL.participant.value,
                                           self.get_description_of_participant,
                                           self.get_object_type)

    def get_and_spawn_entities(self, entity_column_name: str,
                               description_getter: Callable[[str], str],
                               object_type_getter: Callable[[str], ObjectType]) -> Dict[Tuple[str, str], Object]:
        """
        Get and spawn the entities in the NEEM using PyCRAM.
        :param entity_column_name: the entity to get and spawn.
        :param description_getter: the function to get the description of the entity.
        :param object_type_getter: the function to get the type of the entity.
        :return: the entities as a dictionary of PyCRAM objects.
        """
        entities = self.query_result.get_column_value_per_neem(entity_column_name)
        entity_objects = {}
        neem_entities = []
        for neem, neem_entity in entities:
            try:
                description = description_getter(neem_entity)
            except ValueError as e:
                logging.warning(f'Error getting description for entity {neem_entity}: {e}')
                continue
            neem_entity_name = neem_entity
            if neem_entity in neem_entities:
                neem_entity_name = f'{neem_entity}_{neem}'
            entity_object = Object(neem_entity_name, object_type_getter(neem_entity), description)
            entity_objects[(neem, neem_entity)] = entity_object
            neem_entities.append(neem_entity)
        return entity_objects

    @staticmethod
    def get_description_of_agent(agent: str) -> str:
        """
        Get the description of an agent.
        :param agent: the agent to get the description of.
        :return: the description of the agent.
        """
        if 'pr2' in agent.lower():
            return 'pr2.urdf'
        elif 'boxy' in agent.lower():
            return 'boxy.urdf'
        elif 'hsrb' in agent.lower():
            return 'hsrb.urdf'
        elif 'donbot' in agent.lower():
            return 'iai_donbot.urdf'
        elif 'tiago' in agent.lower():
            return 'tiago_dual.urdf'
        elif 'ur5e' in agent.lower():
            return 'ur5e_without_gripper.urdf'
        elif 'ur5' in agent.lower():
            return 'ur5_robotiq.urdf'
        else:
            logging.debug(f'No description found for agent {agent}')
            raise ValueError(f'No description found for agent {agent}')

    @staticmethod
    def get_description_of_participant(participant: str) -> str:
        """
        Get the description of a participant.
        :param participant: the participant to get the description of.
        :return: the description of the participant.
        """
        if 'cup' in participant.lower():
            return 'jeroen_cup.stl'
        elif 'bowl' in participant.lower() or 'pot' in participant.lower():
            return 'bowl.stl'
        elif 'pitcher' in participant.lower():
            return 'Static_MilkPitcher.stl'
        elif 'milk' in participant.lower():
            return 'milk.stl'
        elif 'bottle' in participant.lower():
            return 'Static_CokeBottle.stl'
        elif 'cereal' in participant.lower():
            return 'cereal.stl'
        elif 'spoon' in participant.lower():
            return 'spoon.stl'
        else:
            logging.error(f'No description found for participant {participant}')
            raise ValueError(f'No description found for participant {participant}')

    @staticmethod
    def get_description_of_environment(environment: str) -> str:
        """
        Get the description of an environment.
        :param environment: The environment to get the description of.
        :return: The description of the environment.
        """
        if environment == 'Kitchen':
            environment_path = 'apartment.urdf'
        else:
            logging.error(f'No description found for environment {environment}')
            raise ValueError(f'No description found for environment {environment}')
        return environment_path

    @staticmethod
    def get_object_type(participant: str) -> ObjectType:
        """
        Get the type of pycram object that is a participant in the neem task.
        :param participant: the neem task participant to get the type of.
        :return: the type of the participant/object.
        """
        if 'bowl' in participant.lower() or 'pot' in participant.lower():
            return ObjectType.BOWL
        elif 'milk' in participant.lower():
            return ObjectType.MILK
        elif 'cup' in participant.lower():
            return ObjectType.JEROEN_CUP
        else:
            return ObjectType.GENERIC_OBJECT

    def get_transforms(self) -> List[Transform]:
        """
        Get transforms from the query result.
        :return: the transforms as a list.
        """
        query_result = self.get_result()
        position = query_result.get_positions()
        orientation = query_result.get_orientations()
        frame_id = query_result.get_frame_id()
        child_frame_id = query_result.get_child_frame_id()
        transforms = [Transform([x, y, z], [rx, ry, rz, rw], frame_id, child_frame_id, time=rospy.Time())
                      for x, y, z, rx, ry, rz, rw, frame_id, child_frame_id in
                      zip(*position, *orientation, frame_id, child_frame_id)]
        return transforms

    def get_poses(self) -> List[Pose]:
        """
        Get poses from the query result.
        :return: the poses as a list.
        """
        positions = self.query_result.get_positions()
        orientations = self.query_result.get_orientations()
        poses = [Pose([x, y, z], [rx, ry, rz, rw])
                 for x, y, z, rx, ry, rz, rw in zip(*positions, *orientations)]
        return poses

    def get_stamp(self) -> List[float]:
        """
        Get times from the query result DataFrame.
        :return: the time stamps as a list.
        """
        return self.query_result.get_stamp()

    def get_participants(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participants in the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :return: the participants in the NEEM.
        """
        return self.query_result.get_participants(unique)

    def get_participant_types(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the participant_types in the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :return: the participant_types in the NEEM.
        """
        return self.query_result.get_participant_types(unique)

    def get_neem_ids(self, unique: Optional[bool] = True) -> List[str]:
        """
        Get the NEEM IDs in the query result DataFrame.
        :param unique: whether to return unique NEEM IDs or not.
        :return: the NEEM IDs in the NEEM.
        """
        return self.query_result.get_neem_ids(unique)

    @property
    def query_result(self) -> QueryResult:
        """
        Get the query result.
        :return: the query result.
        """
        return self.get_result()
