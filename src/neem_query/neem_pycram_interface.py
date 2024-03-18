from pycram.datastructures.pose import Pose, Transform
from .neem_interface import get_positions, get_orientations, get_frame_id, get_child_frame_id
import pandas as pd
from typing import List, Tuple
import rospy


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