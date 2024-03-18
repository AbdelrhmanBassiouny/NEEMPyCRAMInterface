from unittest import TestCase
from neem_query.neem_pycram_interface import PyCRAMNeemInterface
import pandas as pd
from pycram.datastructures.pose import Pose, Transform


class TestNeemPycramInterface(TestCase):
    neem_df: pd.DataFrame
    pni: PyCRAMNeemInterface

    @classmethod
    def setUpClass(cls):
        cls.pni = PyCRAMNeemInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.neem_df = (cls.pni.get_neems_containing_task('Pour', regexp=True).
                       limit(100).get_result())

    def test_get_poses(self):
        poses = self.pni.get_poses(self.neem_df)
        self.assertIsInstance(poses, list)
        self.assertTrue(len(poses) > 0)
        self.assertIsInstance(poses[0], Pose)

    def test_get_transforms(self):
        transforms = self.pni.get_transforms(self.neem_df)
        self.assertIsInstance(transforms, list)
        self.assertTrue(len(transforms) > 0)
        self.assertIsInstance(transforms[0], Transform)
