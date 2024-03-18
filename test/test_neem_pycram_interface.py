from unittest import TestCase, skipIf
from neem_query.neem_pycram_interface import PyCRAMNEEMInterface
import pandas as pd

from neem_query.query_result import QueryResult

pycram_not_found = False
try:
    from pycram.datastructures.pose import Pose, Transform
except ImportError:
    pycram_not_found = True


@skipIf(pycram_not_found, "PyCRAM not found.")
class TestNeemPycramInterface(TestCase):
    neem_qr: QueryResult
    pni: PyCRAMNEEMInterface

    @classmethod
    def setUpClass(cls):
        cls.pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.neem_qr = (cls.pni.get_task_data_from_all_neems('Pour', regexp=True).
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
