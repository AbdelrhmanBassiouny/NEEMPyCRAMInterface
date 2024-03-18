import logging

from typing_extensions import Optional

from .neem_interface import NeemInterface
import pandas as pd
from typing import List, Tuple
import rospy

from pycram.datastructures.pose import Pose, Transform

from .query_result import QueryResult


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

    def replay_neem_motions(self, neem_id: Optional[int] = None):
        """
        Replay a NEEM using PyCRAM.
        :param neem_id: the NEEM ID to replay.
        """
        self.get_neems_motion_replay_data().filter_by_participant_type('soma:DesignedContainer')
        if neem_id is not None:
            self.nq.filter(Neem.ID == neem_id)
        neem_qr = self.get_result()
        unique_participants = neem_qr.get_participants_per_neem()
        print(unique_participants)
        environment = neem_qr.get_environment()

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

    def get_poses(self, df: pd.DataFrame) -> List[Pose]:
        """
        Get poses from a DataFrame
        :param df: the DataFrame to yield poses from.
        :return: the poses as a list.
        """
        positions = self.get_positions(df)
        orientations = self.get_orientations(df)
        poses = [Pose([x, y, z], [rx, ry, rz, rw])
                 for x, y, z, rx, ry, rz, rw in zip(*positions, *orientations)]
        return poses
