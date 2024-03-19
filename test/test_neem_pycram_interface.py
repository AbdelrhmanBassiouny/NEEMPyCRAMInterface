from unittest import TestCase, skipIf
from neem_query.neem_pycram_interface import PyCRAMNEEMInterface
import pandas as pd

from neem_query.query_result import QueryResult

pycram_not_found = False
try:
    from pycram.datastructures.pose import Pose, Transform
    from pycram.world_concepts.world_object import Object
    from pycram.datastructures.enums import WorldMode
    from pycram.worlds.bullet_world import BulletWorld
except ImportError:
    pycram_not_found = True


@skipIf(pycram_not_found, "PyCRAM not found.")
class TestNeemPycramInterface(TestCase):
    neem_qr: QueryResult
    pni: PyCRAMNEEMInterface
    world: BulletWorld

    @classmethod
    def setUpClass(cls):
        cls.pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.neem_qr = (cls.pni.get_task_data_from_all_neems('Pour', regexp=True).
                       filter_by_participant_type('soma:DesignedContainer').
                       limit(100).get_result())
        cls.world = BulletWorld(mode=WorldMode.GUI)

    def test_get_and_spawn_environment(self):
        environment = self.pni.get_and_spawn_environment()
        self.assertIsInstance(environment, Object)

    def test_get_and_spawn_participants(self):
        participants = self.pni.get_and_spawn_participants()
        self.assertIsInstance(participants, dict)
        self.assertIsInstance(list(participants.values())[0], Object)

    def test_get_neem_ids(self):
        neem_ids = self.pni.get_neem_ids()
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)
        self.assertIsInstance(neem_ids[0], str)

    def test_get_poses(self):
        poses = self.pni.get_poses()
        self.assertIsInstance(poses, list)
        self.assertTrue(len(poses) > 0)
        self.assertIsInstance(poses[0], Pose)

    def test_get_transforms(self):
        transforms = self.pni.get_transforms()
        self.assertIsInstance(transforms, list)
        self.assertTrue(len(transforms) > 0)
        self.assertIsInstance(transforms[0], Transform)

    def test_get_stamp(self):
        stamps = self.pni.get_stamp()
        self.assertIsInstance(stamps, list)
        self.assertTrue(len(stamps) > 0)
        self.assertIsInstance(stamps[0], float)

    def test_replay_neem_motions(self):
        (self.pni.get_neems_motion_replay_data().
         filter_by_task_type('Pour', regexp=True).
         filter_by_participant_type('soma:DesignedContainer'))
        self.pni.replay_neem_motions()
