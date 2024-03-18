from .neem_interface import NeemInterface
import pandas as pd
from typing import List, Tuple
import rospy

from pycram.datastructures.pose import Pose, Transform


class PyCRAMNeemInterface(NeemInterface):
    """
    A class to interface with the NEEM database and PyCRAM.
    """

    def __init__(self, db_url: str):
        """
        Initialize the PyCRAM NEEM interface.
        :param db_url: the URL to the NEEM database.
        """
        super().__init__(db_url)

    def get_transforms(self, df: pd.DataFrame) -> List[Transform]:
        """
        Get transforms from a DataFrame
        :param df: the DataFrame to get transforms from.
        :return: the transforms as a list.
        """
        position = self.get_positions(df)
        orientation = self.get_orientations(df)
        frame_id = self.get_frame_id(df)
        child_frame_id = self.get_child_frame_id(df)
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
