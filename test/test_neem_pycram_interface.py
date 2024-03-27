from unittest import TestCase, skipIf

from neem_query.neem_pycram_interface import PyCRAMNEEMInterface, ReplayNEEMMotionData
from neem_query.query_result import QueryResult

pycram_not_found = False
try:
    from pycram.datastructures.pose import Pose, Transform
    from pycram.world_concepts.world_object import Object
    from pycram.datastructures.enums import WorldMode
    from pycram.worlds.bullet_world import BulletWorld
    from pycram.ros.viz_marker_publisher import VizMarkerPublisher
except ImportError:
    pycram_not_found = True


@skipIf(pycram_not_found, "PyCRAM not found.")
class TestNeemPycramInterface(TestCase):
    neem_qr: QueryResult
    pni: PyCRAMNEEMInterface
    world: BulletWorld
    vis_mark_publisher: VizMarkerPublisher

    @classmethod
    def setUpClass(cls):
        cls.pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.world = BulletWorld(mode=WorldMode.DIRECT)
        cls.vis_mark_publisher = VizMarkerPublisher()

    @classmethod
    def tearDownClass(cls):
        cls.vis_mark_publisher._stop_publishing()
        cls.world.exit()

    def tearDown(self):
        self.pni.reset()

    def get_pouring_action_data(self):
        self.neem_qr = (self.pni.query_task_data(['Pour'], regexp=True).
                        filter_by_participant_type(['soma:DesignedContainer']).
                        limit(100).get_result())

    def test_get_and_spawn_environment(self):
        self.get_pouring_action_data()
        environment = self.pni.get_and_spawn_environment()
        self.assertIsInstance(environment, Object)

    def test_get_and_spawn_participants(self):
        self.get_pouring_action_data()
        participants = self.pni.get_and_spawn_participants()
        self.assertIsInstance(participants, dict)
        self.assertIsInstance(list(participants.values())[0], Object)

    def test_get_and_spawn_performers(self):
        self.pni.query_plan_of_neem(5)
        performers = self.pni.get_and_spawn_performers()
        self.assertIsInstance(performers, dict)
        self.assertIsInstance(list(performers.values())[0], Object)

    def test_get_neem_ids(self):
        self.get_pouring_action_data()
        neem_ids = self.pni.get_neem_ids()
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)
        self.assertIsInstance(neem_ids[0], str)

    def test_get_poses(self):
        self.get_pouring_action_data()
        poses = self.pni.get_poses()
        self.assertIsInstance(poses, list)
        self.assertTrue(len(poses) > 0)
        self.assertIsInstance(poses[0], Pose)

    def test_get_transforms(self):
        self.get_pouring_action_data()
        transforms = self.pni.get_transforms()
        self.assertIsInstance(transforms, list)
        self.assertTrue(len(transforms) > 0)
        self.assertIsInstance(transforms[0], Transform)

    def test_get_stamp(self):
        self.get_pouring_action_data()
        stamps = self.pni.get_stamp()
        self.assertIsInstance(stamps, list)
        self.assertTrue(len(stamps) > 0)
        self.assertIsInstance(stamps[0], float)

    def test_replay_neem_motions(self):
        (self.pni.query_neems_motion_replay_data().
         filter_by_task_types(['Pour'], regexp=True)
         .filter_by_participant_type(['soma:DesignedContainer']))
        motion_data: ReplayNEEMMotionData = self.pni.get_motion_data()
        self.assertEqual(len(motion_data.poses), len(motion_data.neem_ids))
        self.assertEqual(len(motion_data.neem_ids), len(motion_data.times))
        self.assertEqual(len(motion_data.times), len(motion_data.participant_instances))
        self.assertTrue(len(motion_data.poses) > 0)

    def test_make_camel_case(self):
        camel_case = self.pni._make_camel_case('right_hand')
        self.assertTrue(camel_case == 'RightHand')

    def test_filter_participant_name(self):
        name = self.pni._filter_participant_name('right_hand')
        self.assertTrue(name == ['right_hand', 'RightHand'])
        name = self.pni._filter_participant_name('particle10_1')
        self.assertTrue(name == ['particle'])
        name = self.pni._filter_participant_name('soma:SM_Mug3')
        self.assertTrue(name == ['SM_Mug', 'SMMug'])

    def test_get_all_files_in_resources(self):
        files = self.pni.get_all_files_in_resources()
        self.assertIsInstance(files, list)
        self.assertTrue(len(files) > 0)
        self.assertIsInstance(files[0], str)

    def test_get_and_download_mesh_of_participant(self):
        (self.pni.select_participant().select_object_mesh_path().
         select_neem_id().
         select_from_tasks().
         join_task_participants().
         join_object_mesh_path(is_outer=False)
         )
        mesh_path = self.pni.get_and_download_mesh_of_participant('soma:SM_Cup_2',
                                                                  '633819942a459501ef4d4209')
        self.assertIsInstance(mesh_path, str)

    def test_find_file_in_data_dir(self):
        path = self.pni._find_file_in_data_dir(['particle'])
        self.assertIsInstance(path, str)
        with open(path, 'r') as f:
            self.assertTrue(f.read() is not None)

    def test_query_pick_actions(self):
        qr = self.pni.query_pick_actions().select_task_type().get_result()
        df = qr.df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df['task_type'] == 'soma:PickingUp'))

