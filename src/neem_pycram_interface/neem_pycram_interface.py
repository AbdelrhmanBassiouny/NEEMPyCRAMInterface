import logging
import os
import shutil
import time
from dataclasses import dataclass
from urllib import request

import rospy
from neem_query.enums import ColumnLabel as CL
from neem_query.neem_interface import NeemInterface
from neem_query.neem_query import NeemQuery
from neem_query.neems_database import *
from neem_query.query_result import QueryResult
from sqlalchemy import and_
from typing_extensions import Optional, Dict, Tuple, List, Callable, Union, Set

from pycram.datastructures.enums import ObjectType, Arms, Grasp
from pycram.datastructures.pose import Pose, Transform
from pycram.datastructures.world import World
from pycram.designator import ObjectDesignatorDescription
from pycram.designators.action_designator import PickUpAction, ParkArmsAction, NavigateAction, GraspingAction, \
    SetGripperAction, LookAtAction, ReleaseAction, PlaceAction, GripAction, CloseAction, OpenAction, TransportAction, \
    DetectAction, MoveTorsoAction
from pycram.designators.location_designator import CostmapLocation
from pycram.designators.object_designator import BelieveObject
from pycram.plan_failures import ObjectUnfetchable
from pycram.process_module import simulated_robot
from pycram.robot_descriptions import robot_description
from pycram.world_concepts.world_object import Object
from pycram.external_interfaces.ik import IKError
from .utils import RepositorySearch


@dataclass
class ReplayNEEMMotionData:
    """
    A data class to hold the data required to replay NEEM motions.
    """
    poses: List[Pose]
    times: List[float]
    entity_instances: List[str]

    def get_latest_pose_before_time_stamp(self, entity_instance: str, stamp: float) -> Pose:
        """
        Get the latest pose of an entity instance before a given time stamp.
        :param entity_instance: the entity instance to get the latest pose of.
        :param stamp: the time stamp to get the latest pose before.
        :return: the latest pose of the entity instance before the given time stamp.
        """
        poses = [pose for pose, time_, instance in zip(self.poses, self.times, self.entity_instances)
                 if time_ <= stamp and instance == entity_instance]
        return poses[-1]

    def get_latest_pose_of_entity_instance(self, entity_instance: str) -> Pose:
        """
        Get the latest pose of an entity instance.
        :param entity_instance: the entity instance to get the latest pose of.
        :return: the latest pose of the entity instance.
        """
        return self.filter_by_entity_instance(entity_instance).poses[-1]

    def filter_by_entity_instance(self, entity_instance: str) -> 'ReplayNEEMMotionData':
        """
        Filter the data by the entity instance.
        :param entity_instance: the entity instance to filter by.
        :return: the filtered data.
        """
        poses = [pose for pose, instance in zip(self.poses, self.entity_instances) if instance == entity_instance]
        times = [time_ for time_, instance in zip(self.times, self.entity_instances) if instance == entity_instance]
        return ReplayNEEMMotionData(poses, times, [entity_instance] * len(poses))


@dataclass
class NEEMObjects:
    """
    A data class to hold the pycram objects in the NEEM.
    """
    environment: Object
    participants: Dict[str, Object]
    performers: Dict[str, Object]


def fetch_action(object_designator: ObjectDesignatorDescription.Object, arm: str) -> None:
    """
    Fetch an object using the given arm.
    :param object_designator: the object to fetch.
    :param arm: the arm to use to fetch the object.
    """
    robot_desig = BelieveObject(names=[robot_description.name])
    ParkArmsAction([Arms.BOTH]).resolve().perform()
    pickup_loc = CostmapLocation(target=object_designator, reachable_for=robot_desig.resolve(),
                                 reachable_arm=arm)
    # Tries to find a pick-up posotion for the robot that uses the given arm
    pickup_pose = None
    for pose in pickup_loc:
        if arm in pose.reachable_arms:
            pickup_pose = pose
            break
    if not pickup_pose:
        raise ObjectUnfetchable(
            f"Found no pose for the robot to grasp the object: {object_designator} with arm: {arm}")

    NavigateAction([pickup_pose.pose]).resolve().perform()
    PickUpAction(object_designator, [arm], ["front"]).resolve().perform()
    ParkArmsAction([Arms.BOTH]).resolve().perform()


class PyCRAMNEEMInterface(NeemInterface):
    """
    A class to interface with the NEEM database and PyCRAM.
    """

    soma_to_pycram_actions = {'soma:Grasping': GraspingAction,
                              'soma:PositioningArm': ParkArmsAction,
                              'soma:SettingGripper': SetGripperAction,
                              'soma:LookingAt': LookAtAction,
                              'soma:Releasing': ReleaseAction,
                              'soma:PickingUp': PickUpAction,
                              'soma:Placing': PlaceAction,
                              'soma:Gripping': GripAction,
                              'soma:Closing': CloseAction,
                              'soma:Opening': OpenAction,
                              'soma:OpeningGripper': SetGripperAction,
                              'soma:Navigating': NavigateAction,
                              'soma:Delivering': TransportAction,
                              'soma:Detecting': DetectAction,
                              'soma:AssumingArmPose:': ParkArmsAction,
                              }
    """
    A dictionary to map soma actions to PyCRAM actions.
    """

    soma_to_pycram_grasps = {'soma:FrontGrasp': Grasp.FRONT,
                             'soma:TopGrasp': Grasp.TOP,
                             'soma:LeftGrasp': Grasp.LEFT,
                             'soma:RightGrasp': Grasp.RIGHT}
    """
    A dictionary to map soma grasps to PyCRAM grasps.
    """

    known_robots = ['pr2', 'boxy', 'hsrb', 'donbot', 'tiago', 'ur5e', 'ur5']
    """
    A list of known robots that can be spawned and used in pycram.
    """

    def __init__(self, db_url: str):
        """
        Initialize the PyCRAM NEEM interface.
        :param db_url: the URL to the NEEM database.
        """
        super().__init__(db_url)
        self.all_data_dirs = []
        self.all_data_dirs.extend(World.data_directory)
        self.all_data_dirs.append(World.data_directory[0] + '/robots')
        self.mesh_repo_search = RepositorySearch(self.neem_data_link, start_search_in=self._get_mesh_links())
        self.urdf_repo_search = RepositorySearch(self.neem_data_link, start_search_in=[self._get_urdf_link()])
        self.replay_environment_initialized = False

    @classmethod
    def from_pycram_neem_interface(cls, pycram_neem_interface: 'PyCRAMNEEMInterface'):
        """
        Create a new PyCRAM NEEM interface from an existing one.
        :param pycram_neem_interface: the existing PyCRAM NEEM interface.
        :return: the new PyCRAM NEEM interface.
        """
        return cls(pycram_neem_interface.engine.url.__str__().replace('***', 'password'))

    def redo_neem_plan(self):
        """
        Redo NEEM actions using PyCRAM. The query should contain:
         neem_id, participant, action, parameters, stamp.
         One could use the get_plan_of_neem to get the data. Then filter it as needed.
        """
        environment_obj, participant_objects = self.get_and_spawn_environment_and_participants()
        agent_objects = self.get_and_spawn_performers()
        tasks = self.get_result().get_column_value_per_neem(CL.task_type.value)
        for neem_id, participant, task, parameters, current_time in zip(self.get_neem_ids(unique=False),
                                                                        self.get_participants(unique=False),
                                                                        tasks,
                                                                        self.get_result().get_column_value_per_neem(
                                                                            CL.task_parameter.value),
                                                                        self.get_participant_stamp()):
            # TODO: Implement neem_task_goal_resolver to get task goal like placing goal.
            # TODO: Create designators for objects.
            if task in self.soma_to_pycram_actions:
                action = self.soma_to_pycram_actions[task]
                action_description = action(parameters)
                action_description.ground()
                action_description.resolve()
                action_description.perform()
            else:
                logging.warning(f'No action found for task {task}')

    def redo_pick_action(self, sql_neem_id: Optional[int] = None):
        """
        Redo pick actions from neem(s) using PyCRAM.
        The query should contain:
         neem_id, action, participant, performed_by.
        """
        self.query_pick_actions(sql_neem_id)
        tasks = self.get_result().get_tasks(unique=True)
        task = tasks[0]
        qr = self.get_result().filter_by_task([task])
        environment_desig, participant_desigs, performer_desigs = self.spawn_neem_objects_and_get_designators(qr)
        participant_desig = list(participant_desigs.values())[0]
        grasp = qr.get_task_parameter_types()[0]
        if grasp in self.soma_to_pycram_grasps:
            grasps = [self.soma_to_pycram_grasps[grasp].name.lower()]
        else:
            grasps = [g.name.lower() for g in Grasp]
        arms = [arm.name.lower() for arm in Arms]
        with simulated_robot():
            PickUpAction(participant_desig, arms, grasps).resolve().perform()

    def redo_fetch_action(self, sql_neem_id: Optional[int] = None):
        """
        Redo fetch actions from neem(s) using PyCRAM.
        The query should contain:
         neem_id, action, participant, performed_by.
        """
        self.query_fetch_actions(sql_neem_id)
        task = self.get_result().get_tasks(unique=True)[0]
        qr = self.get_result().filter_by_task([task])
        environment_desig, participant_desigs, performer_desigs = self.spawn_neem_objects_and_get_designators(qr)
        participant_desig = list(participant_desigs.values())[0]
        with simulated_robot():
            fetch_action(participant_desig, Arms.RIGHT.name.lower())

    def redo_grasping_action(self, sql_neem_id: Optional[int] = None):
        """
        Redo grasping actions from neem(s) using PyCRAM.
        """
        self.query_actions(['grasping'], sql_neem_id)
        task = self.get_result().get_tasks(unique=True)[0]

        participant_designators, robot_designator = self.set_pre_task_state(task, sql_neem_id)
        participant_designator = None
        for participant_designator in participant_designators.values():
            if 'hand' in participant_designator.resolve().world_object.name.lower():
                # skip vr hand
                continue
            else:
                participant_designator = participant_designator
                break
        if participant_designator is None:
            raise ValueError('No participant found')

        with simulated_robot():

            self.pre_grasp_action(participant_designator)

            arms = [arm.name.lower() for arm in Arms]

            def action():
                GraspingAction(arms, participant_designator).resolve().perform()

            try:
                action()
            except IKError:
                self.mitigate_grasp_or_pick_failure(participant_designator, robot_designator, action)

    @staticmethod
    def spawn_pr2_and_get_object(pose: Optional[Pose] = None) -> Object:
        """
        Spawn the PR2 robot and get the pycram object for it.
        :param pose: the pose of the PR2 robot.
        :return: the pycram object for the PR2 robot.
        """
        if pose is None:
            pose = Pose([1.6, 2.3, 0], [0, 0, 0.1, 1])
        return Object('pr2', ObjectType.ROBOT, robot_description.name + '.urdf', pose=pose)

    def get_latest_pose_of_participant(self,
                                       participant: str,
                                       query_result: Optional[QueryResult] = None,
                                       before_stamp: Optional[float] = None) -> Pose:
        """
        Get the latest pose of a participant.
        :param participant: the participant to get the latest pose of.
        :param query_result: the query result to get the latest pose from.
        :param before_stamp: the time stamp to get the latest pose before.
        :return: the latest pose of the participant.
        """
        participant_motion_data = self.get_participant_motion_data(query_result)
        if before_stamp is not None:
            pose = participant_motion_data.get_latest_pose_before_time_stamp(participant, before_stamp)
        else:
            pose = participant_motion_data.get_latest_pose_of_entity_instance(participant)
        return pose

    def get_latest_pose_of_performer(self, performer: str, query_result: Optional[QueryResult] = None,
                                     before_stamp: Optional[float] = None) -> Pose:
        """
        Get the latest pose of a performer.
        :param performer: the performer to get the latest pose of.
        :param query_result: the query result to get the latest pose from.
        :param before_stamp: the time stamp to get the latest pose before.
        :return: the latest pose of the performer.
        """
        performer_motion_data = self.get_performer_motion_data(query_result)
        if before_stamp is not None:
            pose = performer_motion_data.get_latest_pose_before_time_stamp(performer, before_stamp)
        else:
            pose = performer_motion_data.get_latest_pose_of_entity_instance(performer)
        return pose

    def pre_grasp_action(self, participant_designator: ObjectDesignatorDescription):
        """
        Perform the pre-grasp action by parking arms and moving the torso.
        :param participant_designator: the participant designator.
        """
        self.get_ready_action()
        participant_pose = participant_designator.resolve().world_object.get_pose()
        LookAtAction([participant_pose]).resolve().perform()

    def mitigate_grasp_or_pick_failure(self, participant_designator: ObjectDesignatorDescription,
                                       robot_designator: ObjectDesignatorDescription,
                                       action: Callable[[], None]):
        """
        Mitigate the grasp or pick failure by moving the robot to a different pose.
        :param participant_designator: the participant designator.
        :param robot_designator: the robot designator.
        :param action: the action to perform.
        """
        torso_pose = 0.3
        while 0.3 >= torso_pose >= 0.1:
            try:
                self.get_ready_action(torso_pose)
                pick_up_location = CostmapLocation(target=participant_designator.resolve(),
                                                   reachable_for=robot_designator.resolve()).resolve()
                print(pick_up_location)
                NavigateAction([pick_up_location.pose]).resolve().perform()
                action()
                break
            except StopIteration:
                torso_pose -= 0.01

    @staticmethod
    def get_ready_action(torso_pose: Optional[float] = 0.25):
        """
        Get the ready action, which parks the arms and moves the torso.
        :param torso_pose: the torso pose to move to.
        """
        ParkArmsAction([Arms.BOTH]).resolve().perform()
        MoveTorsoAction([torso_pose]).resolve().perform()

    def get_performer_motion_data(self, query_result: Optional[QueryResult] = None) -> ReplayNEEMMotionData:
        """
        Get the motion data of the performer.
        :param query_result: the query result to get the motion data from.
        :return: the motion data of the performer.
        """
        poses = self.get_performer_poses(query_result)
        times = self.get_performer_stamp(query_result)
        performer_instances = self.get_performers(query_result=query_result, unique=False)
        return ReplayNEEMMotionData(poses, times, performer_instances)

    def set_pre_task_state(self, task: str, sql_neem_id: int) -> Tuple[Dict[str, BelieveObject], BelieveObject]:
        """
        Set the state of the world before the task.
        :param task: the task to set the state before.
        :param sql_neem_id: the sql id of the NEEM.
        :return: the designators for the participants and the robot.
        """
        qr = self.get_result().filter_by_task([task]).filter_by_sql_neem_id([sql_neem_id])
        print(qr.df)
        environment_designators, participant_designators, performer_designators = (
            self.spawn_neem_objects_and_get_designators(qr))
        task_start_time = qr.get_time_interval_begin()[0]
        self.query_neems_motion_replay_data(participant_necessary=False, participant_base_link_necessary=False,
                                            sql_neem_ids=[sql_neem_id])
        self.set_pre_task_participants_state(participant_designators, task_start_time)
        robot = None
        if len(performer_designators) > 0:
            performer_designator = list(performer_designators.values())[0]
            performer_object = performer_designator.resolve().world_object
            if performer_object.obj_type == ObjectType.ROBOT:
                robot = performer_object
                robot_pose = self.get_latest_pose_of_performer(list(performer_designators.keys())[0])
                robot.set_pose(robot_pose)
        if robot is None:
            robot = self.spawn_pr2_and_get_object()
        robot_designator = BelieveObject(names=[robot.name])
        return participant_designators, robot_designator

    def set_pre_task_participants_state(self, participant_designators: Dict[str, BelieveObject],
                                        task_start_time: Optional[float] = None):
        """
        Set the state of the participants before the task.
        :param participant_designators: the designators for the participants.
        :param task_start_time: the time stamp to set the state before.
        """
        participant_designators = {name: designator for name, designator in participant_designators.items()
                                   if 'hand' not in name.lower()}
        for participant_designator in participant_designators.values():
            participant = list(participant_designators.keys())[0]
            participant_pose = self.get_latest_pose_of_participant(participant, before_stamp=task_start_time)
            participant_object = participant_designator.resolve().world_object
            participant_object.set_pose(participant_pose)

    def get_pre_task_state(self, task: str, sql_neem_id: int):
        """
        Get the state of the world before the task.
        :param task: the task to get the state before.
        :param sql_neem_id: the sql id of the NEEM.
        :return:
        """
        pni = PyCRAMNEEMInterface.from_pycram_neem_interface(self)
        q = (pni.query_neems_motion_replay_data(participant_necessary=True, participant_base_link_necessary=True)
             .filter_by_sql_neem_id([sql_neem_id])
             .filter_by_tasks([task])
             # .filter_tf_by_child_frame_id("SM_Bowl_2")
             )
        print(q.construct_query())
        qr = q.get_result()
        print(qr.df)
        print(qr.get_sql_neem_ids(unique=True))
        print(qr.get_task_types(unique=True))
        # print(df)
        return qr.df

    def get_prev_task_data(self, curr_task: str, sql_neem_id: int):
        """
        Get the previous task of the current task.
        :param curr_task: the current task.
        :param sql_neem_id: the sql id of the NEEM.
        """
        df = (self.query_prev_task_data(curr_task, sql_neem_id)
              .select_time_columns().select_task().select_participant_base_link()
              ).get_result().order_by(CL.time_interval_end.value).df.head(1)
        return df

    def query_prev_task_data(self, curr_task: str, sql_neem_id: int) -> 'NeemQuery':
        """
        Query the previous task of the current task.
        :param curr_task: the current task.
        :param sql_neem_id: the sql id of the NEEM.
        """
        curr_task_start_time = self.get_result().get_task_start_time(curr_task, sql_neem_id)
        ni = NeemInterface.from_neem_interface(self)
        return (ni.query_tasks_semantic_data(task_parameters_necessary=False)
                .filter_by_sql_neem_id([sql_neem_id]).filter(and_(SomaHasIntervalBegin.o < curr_task_start_time))
                ).order_by(CL.time_interval_end.value)

    @staticmethod
    def get_performer_base_link(performer: str) -> str:
        """
        Get the base link of a performer.
        :param performer: the performer to get the base link of.
        :return: the base link of the performer.
        """
        if 'pr2' in performer.lower():
            return 'pr2:base_link'
        else:
            logging.error(f'No base link found for performer {performer}')
            raise ValueError(f'No base link found for performer {performer}')

    def get_and_spawn_neem_objects(self, query_result: Optional[QueryResult] = None) -> NEEMObjects:
        """
        Get and spawn the objects in the NEEM using PyCRAM.
        :param query_result: the query result to get the objects from.
        """
        environment_obj = self.get_and_spawn_environment(query_result)
        participant_objects = self.get_and_spawn_participants(query_result)
        performer_objects = self.get_and_spawn_performers(query_result)
        return NEEMObjects(environment_obj, participant_objects, performer_objects)

    def spawn_neem_objects_and_get_designators(self, query_result: QueryResult) -> \
            Tuple[BelieveObject, Dict[str, BelieveObject], Dict[str, BelieveObject]]:
        """
        Get the designators for the objects in the NEEM.
        :param query_result: the query result to get the objects from.
        """
        neem_objects = self.get_and_spawn_neem_objects(query_result)
        environment_desig = BelieveObject(names=[neem_objects.environment.name])
        participant_desigs = {name: BelieveObject(names=[object_.name])
                              for name, object_ in neem_objects.participants.items()}
        performer_desigs = {name: BelieveObject(names=[object_.name], types=[object_.obj_type])
                            for name, object_ in neem_objects.performers.items()}
        return environment_desig, participant_desigs, performer_desigs

    def query_transport_actions(self, sql_neem_id: Optional[int] = None,
                                task_parameters_necessary: Optional[bool] = False,
                                participant_necessary: Optional[bool] = False,
                                performer_necessary: Optional[bool] = False,
                                participant_base_link_necessary: Optional[bool] = False,
                                performer_base_link_necessary: Optional[bool] = False,
                                select_columns: Optional[bool] = True
                                ) -> NeemQuery:
        """
        Get the query to redo transport actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        return self.query_actions(['transport'],
                                  sql_neem_id=sql_neem_id,
                                  task_parameters_necessary=task_parameters_necessary,
                                  participant_necessary=participant_necessary,
                                  performer_necessary=performer_necessary,
                                  participant_base_link_necessary=participant_base_link_necessary,
                                  performer_base_link_necessary=performer_base_link_necessary,
                                  select_columns=select_columns)

    def query_pick_actions(self, sql_neem_id: Optional[int] = None,
                           task_parameters_necessary: Optional[bool] = False,
                           participant_necessary: Optional[bool] = False,
                           performer_necessary: Optional[bool] = False,
                           participant_base_link_necessary: Optional[bool] = False,
                           performer_base_link_necessary: Optional[bool] = False,
                           select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo pick actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        self.query_actions(['pick'], sql_neem_id=sql_neem_id, task_parameters_necessary=task_parameters_necessary,
                           participant_necessary=participant_necessary, performer_necessary=performer_necessary,
                           participant_base_link_necessary=participant_base_link_necessary,
                           performer_base_link_necessary=performer_base_link_necessary, select_columns=select_columns)
        return self

    def query_vr_fetch_actions(self, sql_neem_id: Optional[int] = None,
                               task_parameters_necessary: Optional[bool] = False,
                               participant_necessary: Optional[bool] = True,
                               performer_necessary: Optional[bool] = False,
                               participant_base_link_necessary: Optional[bool] = False,
                               performer_base_link_necessary: Optional[bool] = False,
                               select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo fetch actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        return self.query_vr_actions('fetch', sql_neem_id=sql_neem_id,
                                     task_parameters_necessary=task_parameters_necessary,
                                     participant_necessary=participant_necessary,
                                     performer_necessary=performer_necessary,
                                     participant_base_link_necessary=participant_base_link_necessary,
                                     performer_base_link_necessary=performer_base_link_necessary,
                                     select_columns=select_columns)

    def query_vr_pick_actions(self, sql_neem_id: Optional[int] = None,
                              task_parameters_necessary: Optional[bool] = False,
                              participant_necessary: Optional[bool] = True,
                              performer_necessary: Optional[bool] = False,
                              participant_base_link_necessary: Optional[bool] = False,
                              performer_base_link_necessary: Optional[bool] = False,
                              select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo pick actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        return self.query_vr_actions('pick', sql_neem_id=sql_neem_id,
                                     task_parameters_necessary=task_parameters_necessary,
                                     participant_necessary=participant_necessary,
                                     performer_necessary=performer_necessary,
                                     participant_base_link_necessary=participant_base_link_necessary,
                                     performer_base_link_necessary=performer_base_link_necessary,
                                     select_columns=select_columns)

    def query_vr_actions(self, action_name: Optional[str] = None,
                         sql_neem_id: Optional[int] = None,
                         task_parameters_necessary: Optional[bool] = False,
                         participant_necessary: Optional[bool] = False,
                         performer_necessary: Optional[bool] = False,
                         participant_base_link_necessary: Optional[bool] = False,
                         performer_base_link_necessary: Optional[bool] = False,
                         select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        self.query_actions(action_name, sql_neem_id=sql_neem_id, task_parameters_necessary=task_parameters_necessary,
                           participant_necessary=participant_necessary, performer_necessary=performer_necessary,
                           participant_base_link_necessary=participant_base_link_necessary,
                           performer_base_link_necessary=performer_base_link_necessary, select_columns=select_columns)
        self.filter_by_performer_type(['Natural'], regexp=True, negate=False)
        return self

    def query_fetch_actions(self, sql_neem_id: Optional[int] = None,
                            task_parameters_necessary: Optional[bool] = False,
                            participant_necessary: Optional[bool] = True,
                            performer_necessary: Optional[bool] = False,
                            participant_base_link_necessary: Optional[bool] = False,
                            performer_base_link_necessary: Optional[bool] = False,
                            select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo fetch actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """
        return self.query_actions(['fetch'], sql_neem_id=sql_neem_id,
                                  task_parameters_necessary=task_parameters_necessary,
                                  participant_necessary=participant_necessary,
                                  performer_necessary=performer_necessary,
                                  participant_base_link_necessary=participant_base_link_necessary,
                                  performer_base_link_necessary=performer_base_link_necessary,
                                  select_columns=select_columns)

    def query_navigate_actions(self, sql_neem_id: Optional[int] = None) -> NeemQuery:
        """
        Get the query to redo navigate actions from neem(s) using PyCRAM.
        For arguments documentation look at :py:meth:`PyCRAMNEEMInterface.query_actions`.
        """

        return self.query_actions(['navigating', 'navigation'], sql_neem_id=sql_neem_id)

    def query_actions(self, action_name: Optional[List[str]] = None,
                      sql_neem_id: Optional[int] = None,
                      task_parameters_necessary: Optional[bool] = False,
                      participant_necessary: Optional[bool] = False,
                      performer_necessary: Optional[bool] = False,
                      participant_base_link_necessary: Optional[bool] = False,
                      performer_base_link_necessary: Optional[bool] = False,
                      select_columns: Optional[bool] = True) -> NeemQuery:
        """
        Get the query to redo actions from neem(s) using PyCRAM.
        The query should contain:
         neem_id, action, participant, performed_by.
        :param action_name: the name of the action to query.
        :param sql_neem_id: the id of the NEEM in sql, if None, all NEEMs are considered.
        :param task_parameters_necessary: whether to use outer join for the task parameters or not.
        :param participant_necessary: whether to only include tasks that have a participant or not.
        :param performer_necessary: whether to only include tasks that have a performer or not.
        :param participant_base_link_necessary: whether to use outer join for the participant base link or not.
        :param performer_base_link_necessary: whether to use outer join for the performer base link or not.
        :param select_columns: whether to select the columns or not.
        :return: the modified query.
        """
        self.query_tasks_semantic_data(action_name if action_name is not None else None,
                                       task_parameters_necessary=task_parameters_necessary,
                                       participant_base_link_necessary=participant_base_link_necessary,
                                       performer_base_link_necessary=performer_base_link_necessary,
                                       participant_necessary=participant_necessary,
                                       performer_necessary=performer_necessary,
                                       select_columns=select_columns)
        if sql_neem_id is not None:
            self.filter_by_sql_neem_id([sql_neem_id])
        return self

    def replay_motion_of_neem(self, sql_neem_ids: Optional[List[int]] = None, real_time: Optional[bool] = True):
        """
        Replay the motions of a NEEM using PyCRAM.
        The query should contain:
         neem_id, participant, translation, orientation, stamp.
         :param sql_neem_ids: the sql ids of the NEEMs.
            :param real_time: whether to replay the motions in real time or not.
        """
        self.query_neems_motion_replay_data(sql_neem_ids=sql_neem_ids)
        self.replay_motions_in_query(real_time=real_time)

    def replay_motion_of_task_type(self, task: str, sql_neem_id: Optional[int] = None):
        """
        Replay the motions of a task type using PyCRAM.
        The query should contain:
         neem_id, participant, translation, orientation, stamp.
        """
        self.query_neems_motion_replay_data(sql_neem_id=sql_neem_id).filter_by_task_types([task], regexp=True)
        self.replay_motions_in_query()

    def replay_motions_in_query(self, query_result: Optional[QueryResult] = None,
                                real_time: Optional[bool] = True) -> None:
        """
        Replay NEEMs Motion using PyCRAM. The query should contain:
         environment, participant, translation, orientation, stamp.
         One could use the get_motion_replay_data method to get the data. Then filter it as needed.
        :param query_result: the query result to replay the motions from.
        :param real_time: whether to replay the motions in real time or not.
        """
        query_result = query_result if query_result is not None else self.get_result()
        environment_obj, participant_objects = self.get_and_spawn_environment_and_participants(query_result)

        motion_data = self.get_participant_motion_data(query_result)
        poses = motion_data.poses
        times = motion_data.times
        participant_instances = motion_data.entity_instances
        unique_participants = list(set(participant_instances))
        moved_participants = set()
        prev_time = 0
        for participant, pose, current_time in zip(participant_instances, poses, times):
            if prev_time > 0:
                wait_time = current_time - prev_time
                if wait_time > 1:
                    wait_time = 1
                if real_time:
                    time.sleep(wait_time)
            prev_time = current_time
            participant_objects[participant].set_pose(pose)
            if not self.replay_environment_initialized:
                if pose != Pose():
                    moved_participants.add(participant)
                if self._all_participants_moved(unique_participants, moved_participants):
                    self.replay_environment_initialized = True

        self.replay_environment_initialized = False

    @staticmethod
    def _all_participants_moved(unique_participants: List[str], moved_participants: Set[str]) -> bool:
        """
        Check if all participants have moved.
        :param unique_participants: the unique participants.
        :param moved_participants: the moved participants.
        :return: whether all participants have moved.
        """
        return all([p in moved_participants for p in unique_participants])

    def get_participant_motion_data(self, query_result: Optional[QueryResult] = None) -> ReplayNEEMMotionData:
        """
        Get motion data required to replay motions from the query result.
        :param query_result: the query result to get the motion data from.
        """
        query_result = query_result if query_result is not None else self.get_result()
        poses = self.get_participant_poses(query_result)
        times = self.get_participant_stamp(query_result)
        participant_instances = self.get_participants(query_result=query_result, unique=False)
        return ReplayNEEMMotionData(poses, times, participant_instances)

    def get_and_spawn_environment_and_participants(self, query_result: Optional[QueryResult] = None) \
            -> Tuple[Object, Dict[str, Object]]:
        """
        Get and spawn the environment and participants in the NEEM using PyCRAM.
        :param query_result: the query result to get the environment and participants from.
        :return: the environment and participants as PyCRAM objects.
        """
        environment_obj = self.get_and_spawn_environment(query_result)
        participant_objects = self.get_and_spawn_participants(query_result)
        return environment_obj, participant_objects

    def get_and_spawn_environment(self, query_result: Optional[QueryResult] = None) -> Object:
        """
        Get and spawn the environment in the NEEM using PyCRAM.
        :param query_result: the query result to get the environment from.
        :return: the environment as a PyCRAM object.
        """
        query_result = query_result if query_result is not None else self.get_result()
        environments = query_result.get_environments()
        environment_path = self.get_description_of_environment(environments[0])
        return Object(environments[0], ObjectType.ENVIRONMENT, environment_path)

    def get_and_spawn_performers(self, query_result: Optional[QueryResult] = None) -> Dict[str, Object]:
        """
        Get and spawn the agents in the NEEM using PyCRAM.
        :param query_result: the query result to get the agents from.
        :return: A dictionary of agents as PyCRAM objects.
        """
        return self.get_and_spawn_entities(CL.is_performed_by.value,
                                           lambda agent, _: self.get_description_of_performer(agent),
                                           self.get_performer_object_type,
                                           query_result)

    def get_and_spawn_participants(self, query_result: Optional[QueryResult] = None) -> Dict[str, Object]:
        """
        Get and spawn the participants in the NEEM using PyCRAM.
        :param query_result: the query result to get the participants from.
        :return: A dictionary of participants as PyCRAM objects.
        """
        return self.get_and_spawn_entities(CL.participant.value,
                                           self.get_description_of_participant,
                                           lambda participant, _: self.get_object_type(participant),
                                           query_result)

    def get_and_spawn_entities(self,
                               entity_column_name: str,
                               description_getter: Callable[[str, QueryResult], str],
                               object_type_getter: Callable[[str, QueryResult], ObjectType],
                               query_result: Optional[QueryResult] = None) -> Dict[str, Object]:
        """
        Get and spawn the entities in the NEEM using PyCRAM.
        :param entity_column_name: the entity to get and spawn.
        :param description_getter: the function to get the description of the entity.
        :param object_type_getter: the function to get the type of the entity.
        :param query_result: the query result to get the entities from.
        :return: A dictionary of entities as PyCRAM objects.
        """
        query_result = query_result if query_result is not None else self.get_result()
        entities = query_result.get_column_values(entity_column_name, unique=True)
        entity_objects = {}
        for entity in entities:
            try:
                description = description_getter(entity, query_result)
            except ValueError as e:
                rospy.logwarn(f'Error getting description for entity {entity}: {e}')
                continue
            entity_name = entity
            if ':' in entity_name:
                entity_name = entity_name.split(':')[-1]
            object_names = [obj.name for obj in World.current_world.objects]
            if entity in object_names:
                entity_name = f'{entity}_{object_names.count(entity)}'
            entity_object = Object(entity_name, object_type_getter(entity, query_result), description)
            entity_objects[entity] = entity_object
        return entity_objects

    @staticmethod
    def get_description_of_performer(agent: str) -> str:
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

    def get_and_download_mesh_of_participant(self, participant: str,
                                             query_result: Optional[QueryResult] = None) -> Union[str, None]:
        """
        Get the mesh of a participant and download it.
        :param participant: the participant to get the mesh of.
        :param query_result: the query result to get the mesh from.
        :return: the download path of the mesh file.
        """
        mesh_link = self.get_mesh_link_of_object_in_neem(participant, query_result)
        if mesh_link is not None:
            download_path = self.download_file(mesh_link)
            if download_path is not None:
                return download_path

    def get_description_of_participant(self, participant: str,
                                       query_result: Optional[QueryResult] = None) -> Union[str, None]:
        """
        Get the description of a participant.
        :param participant: the participant to get the description of.
        :param query_result: the query result to get the description from.
        :return: the description of the participant.
        """
        participant_name_candidates = self._filter_participant_name(participant)

        if 'NIL' in participant_name_candidates:
            return None

        file_path = self._find_file_in_data_dir(participant_name_candidates)
        if file_path is not None:
            return file_path

        download_path = self.get_and_download_mesh_of_participant(participant, query_result)
        if download_path is not None:
            return download_path

        download_path = self._search_for_participant_in_online_repository(participant_name_candidates)
        if download_path is not None:
            return download_path

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
        elif 'plate' in participant.lower():
            return 'bowl.stl'
        else:
            logging.error(f'No description found for participant {participant}')
            raise ValueError(f'No description found for participant {participant}')

    def _find_file_in_data_dir(self, file_name: List[str]) -> Union[str, None]:
        """
        Find a file in the data directories.
        :param file_name: the file to find.
        :return: the path of the file in the data directories.
        """
        for data_dir in self.all_data_dirs:
            for file in os.listdir(data_dir):
                for name in file_name:
                    if name in file:
                        return os.path.join(data_dir, file)

    def _filter_participant_name(self, participant: str) -> List[str]:
        """
        Filter the participant name.
        :param participant: the participant to filter.
        :return: the filtered participant name candidates.
        """

        participant_name_candidates = []
        participant_name = participant.split(':')[-1]

        while participant_name[-1].isdigit():
            participant_name = participant_name[:-1]

        participant_name = participant_name.strip(' _-')

        participant_name = participant_name.split('_')
        if len(participant_name) > 2:
            participant_name = '_'.join(participant_name[:-1])
        else:
            participant_name = '_'.join(participant_name)
        # if it ends with a number, remove the number
        while participant_name[-1].isdigit():
            participant_name = participant_name[:-1]

        participant_name_candidates.append(participant_name)
        if '_' in participant_name:
            if participant_name[0].islower():
                participant_name_candidates.append(self._make_camel_case(participant_name))
            else:
                participant_name_candidates.append(''.join(participant_name.split('_')))

        return participant_name_candidates

    @staticmethod
    def _make_camel_case(participant_name: str) -> str:
        """
        Make the participant name camel case.
        :param participant_name: the participant name to make camel case.
        :return: the participant name in camel case.
        """
        participant_name = participant_name.split('_')
        participant_name = [part.capitalize() for part in participant_name]
        return ''.join(participant_name)

    def get_all_files_in_resources(self) -> List[str]:
        """
        Get all the files in the data directories.
        """
        files = []
        for data_dir in self.all_data_dirs:
            files.extend(os.listdir(data_dir))
        return files

    def _search_for_participant_in_online_repository(self, participant_names: List[str]) -> Union[str, None]:
        """
        Search for a participant in the online repository.
        :param participant_names: the participants to search for.
        :return: the download path of the participant mesh file.
        """
        return self._search_for_file_in_online_repository(self.mesh_repo_search, participant_names, ignore=['.mtl'])

    def _search_for_urdf_in_online_repository(self, urdf_name: str) -> Union[str, None]:
        """
        Search for a URDF in the online repository.
        :param urdf_name: the URDF to search for.
        :return: the download path of the URDF file.
        """
        return self._search_for_file_in_online_repository(self.urdf_repo_search, [urdf_name])

    def _search_for_file_in_online_repository(self, repo_search_obj: RepositorySearch,
                                              file_names: List[str], ignore: Optional[List[str]] = None) -> Union[
        str, None]:
        """
        Search for a file in the online repository.
        :param repo_search_obj: the repository search object to use.
        :param file_names: the files to search for.
        :param ignore: the patterns in filenames to ignore if the filename has that pattern(s).
        :return: the download path of the file.
        """
        file_names = repo_search_obj.search_similar_file_names(file_names, ignore=ignore, find_all=False)
        if len(file_names) > 0:
            download_path = self.download_file(file_names[0])
            return download_path

    def get_mesh_link_of_object_in_neem(self, object_name: str,
                                        query_result: Optional[QueryResult] = None) -> Union[str, None]:
        """
        Get the mesh link of an object in a NEEM.
        :param object_name: The name of the object.
        :param query_result: The query result to get the mesh link from.
        :return: The mesh link of the object.
        """
        query_result = query_result if query_result is not None else self.get_result()
        mesh_path_df = query_result.filter_by_participant([object_name]).df
        mesh_path_df = mesh_path_df.dropna(subset=[CL.object_mesh_path.value]).drop_duplicates()
        if len(mesh_path_df) == 0:
            return None
        mesh_path = mesh_path_df[CL.object_mesh_path.value].values[0]
        if mesh_path is None:
            return None
        if 'package:/' in mesh_path:
            mesh_link = mesh_path.replace('package:/', self.neem_data_link)
        else:
            mesh_link = mesh_path
        return mesh_link

    def download_file(self, file_link: str) -> Union[str, None]:
        """
        Download a file.
        :param file_link: The link of the file.
        :return: The download path of the file.
        """
        file_name = file_link.split('/')[-1]
        download_path = os.path.join(World.data_directory[0], file_name)
        try:
            with request.urlopen(file_link, timeout=1) as response:
                with open(download_path, 'wb') as file:
                    shutil.copyfileobj(response, file)
            while not os.path.exists(download_path):
                pass
            return download_path
        except Exception as e:
            logging.warning(f'Failed to download file from {file_link}. Error: {e}')
            return None

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

    def get_performer_object_type(self, performer: str, query_result: Optional[QueryResult] = None) \
            -> Optional[ObjectType]:
        """
        Get the type of pycram object that is a performer in the neem task.
        :param performer: the neem task performer to get the type of.
        :param query_result: the query result to get the type from.
        :return: the type of the performer/object.
        """

        if self.is_a_known_robot(performer):
            return ObjectType.ROBOT

        query_result = query_result if query_result is not None else self.get_result()
        performer_type = query_result.filter_by_performer([performer]).get_performer_types()[0]
        if performer_type is not None:
            if self.is_a_human(performer_type):
                return ObjectType.HUMAN

        return None

    def is_a_known_robot(self, performer: str) -> bool:
        """
        Check if the performer is a known robot.
        :param performer: the performer to check.
        :return: whether the performer is a known robot or not.
        """
        for robot in self.known_robots:
            if robot in performer.lower():
                return True
        return False

    @staticmethod
    def is_a_human(performer_type: str) -> bool:
        """
        Check if the performer is a human.
        :param performer_type: the performer type to check.
        :return: whether the performer is a human or not.
        """
        if any([v in performer_type.lower() for v in ['natural', 'human', 'person', 'hand']]):
            return True
        return False

    def get_participant_transforms(self, query_result: Optional[QueryResult] = None) -> List[Transform]:
        """
        Get transforms from the query result.
        :return: the transforms as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        position = query_result.get_participant_positions()
        orientation = query_result.get_participant_orientations()
        frame_id = query_result.get_participant_frame_id()
        child_frame_id = query_result.get_participant_child_frame_id()
        transforms = [Transform([x, y, z], [rx, ry, rz, rw], frame_id, child_frame_id, time=rospy.Time())
                      for x, y, z, rx, ry, rz, rw, frame_id, child_frame_id in
                      zip(*position, *orientation, frame_id, child_frame_id)]
        return transforms

    def get_participant_poses(self, query_result: Optional[QueryResult] = None) -> List[Pose]:
        """
        Get poses from the query result.
        :param query_result: the query result to get the poses from.
        :return: the poses as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        positions = query_result.get_participant_positions()
        orientations = query_result.get_participant_orientations()
        poses = [Pose([x, y, z], [rx, ry, rz, rw])
                 for x, y, z, rx, ry, rz, rw in zip(*positions, *orientations)]
        return poses

    def get_participant_stamp(self, query_result: Optional[QueryResult] = None) -> List[float]:
        """
        Get times from the query result DataFrame.
        :param query_result: the query result to get the times from.
        :return: the time stamps as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_participant_stamp()

    def get_participants(self, unique: Optional[bool] = True,
                         query_result: Optional[QueryResult] = None) -> List[str]:
        """
        Get the participants in the query result DataFrame.
        :param unique: whether to return unique participants or not.
        :param query_result: the query result to get the participants from.
        :return: the participants in the NEEM.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_participants(unique)

    def get_participant_types(self, unique: Optional[bool] = True,
                              query_result: Optional[QueryResult] = None) -> List[str]:
        """
        Get the participant_types in the query result DataFrame.
        :param unique: whether to return unique participant_types or not.
        :param query_result: the query result to get the participant_types from.
        :return: the participant_types in the NEEM.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_participant_types(unique)

    def get_performer_transforms(self, query_result: Optional[QueryResult] = None) -> List[Transform]:
        """
        Get transforms from the query result.
        :return: the transforms as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        position = query_result.get_performer_positions()
        orientation = query_result.get_performer_orientations()
        frame_id = query_result.get_performer_frame_id()
        child_frame_id = query_result.get_performer_child_frame_id()
        transforms = [Transform([x, y, z], [rx, ry, rz, rw], frame_id, child_frame_id, time=rospy.Time())
                      for x, y, z, rx, ry, rz, rw, frame_id, child_frame_id in
                      zip(*position, *orientation, frame_id, child_frame_id)]
        return transforms

    def get_performer_poses(self, query_result: Optional[QueryResult] = None) -> List[Pose]:
        """
        Get poses from the query result.
        :param query_result: the query result to get the poses from.
        :return: the poses as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        positions = query_result.get_performer_positions()
        orientations = query_result.get_performer_orientations()
        poses = [Pose([x, y, z], [rx, ry, rz, rw])
                 for x, y, z, rx, ry, rz, rw in zip(*positions, *orientations)]
        return poses

    def get_performer_stamp(self, query_result: Optional[QueryResult] = None) -> List[float]:
        """
        Get times from the query result DataFrame.
        :param query_result: the query result to get the times from.
        :return: the time stamps as a list.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_performer_stamp()

    def get_performers(self, unique: Optional[bool] = True,
                       query_result: Optional[QueryResult] = None) -> List[str]:
        """
        Get the performers in the query result DataFrame.
        :param unique: whether to return unique performers or not.
        :param query_result: the query result to get the performers from.
        :return: the performers in the NEEM.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_performers(unique)

    def get_performer_types(self, unique: Optional[bool] = True,
                            query_result: Optional[QueryResult] = None) -> List[str]:
        """
        Get the performer_types in the query result DataFrame.
        :param unique: whether to return unique performer_types or not.
        :param query_result: the query result to get the performer_types from.
        :return: the performer_types in the NEEM.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_performer_types(unique)

    def get_neem_ids(self, unique: Optional[bool] = True, query_result: Optional[QueryResult] = None) -> List[str]:
        """
        Get the NEEM IDs in the query result DataFrame.
        :param unique: whether to return unique NEEM IDs or not.
        :param query_result: the query result to get the NEEM IDs from.
        :return: the NEEM IDs in the NEEM.
        """
        query_result = query_result if query_result is not None else self.get_result()
        return query_result.get_neem_ids(unique)
