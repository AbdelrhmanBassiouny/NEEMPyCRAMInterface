from unittest import TestCase, skipIf, skip

import pandas as pd

from neem_query.enums import ColumnLabel as CL
from neem_pycram_interface.neem_pycram_interface import PyCRAMNEEMInterface, ReplayNEEMMotionData
from neem_query.query_result import QueryResult

pycram_found = True
try:
    from pycram.datastructures.pose import Pose, Transform
    from pycram.world_concepts.world_object import Object
    from pycram.datastructures.enums import WorldMode
    from pycram.worlds.bullet_world import BulletWorld
    from pycram.ros.viz_marker_publisher import VizMarkerPublisher
except ImportError:
    pycram_found = False


@skipIf(not pycram_found, "PyCRAM not found.")
class TestNeemPycramInterface(TestCase):
    neem_qr: QueryResult
    pni: PyCRAMNEEMInterface
    if pycram_found:
        world: BulletWorld
        vis_mark_publisher: VizMarkerPublisher

    @classmethod
    def setUpClass(cls):
        if not pycram_found:
            return
        cls.pni = PyCRAMNEEMInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.world = BulletWorld(mode=WorldMode.DIRECT)
        cls.vis_mark_publisher = VizMarkerPublisher()

    @classmethod
    def tearDownClass(cls):
        cls.vis_mark_publisher._stop_publishing()
        cls.world.exit()

    def tearDown(self):
        self.pni.reset()
        self.world.current_world.reset_world_and_remove_objects()

    def get_pouring_action_data(self):
        query = (self.pni.query_task_motion_data(['Pour'], regexp=True).
                 filter_by_participant_type(['soma:DesignedContainer']).
                 limit(100))
        # print(query.construct_query())
        query.get_result()

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
        poses = self.pni.get_participant_poses()
        self.assertIsInstance(poses, list)
        self.assertTrue(len(poses) > 0)
        self.assertIsInstance(poses[0], Pose)

    def test_get_transforms(self):
        self.get_pouring_action_data()
        transforms = self.pni.get_participant_transforms()
        self.assertIsInstance(transforms, list)
        self.assertTrue(len(transforms) > 0)
        self.assertIsInstance(transforms[0], Transform)

    def test_get_stamp(self):
        self.get_pouring_action_data()
        stamps = self.pni.get_participant_stamp()
        self.assertIsInstance(stamps, list)
        self.assertTrue(len(stamps) > 0)
        self.assertIsInstance(stamps[0], float)

    def test_replay_neem_motions_data(self):
        (self.pni.query_neems_motion_replay_data().
         filter_by_task_types(['Pour'], regexp=True)
         .filter_by_participant_type(['soma:DesignedContainer']))
        motion_data: ReplayNEEMMotionData = self.pni.get_participant_motion_data()
        self.assertEqual(len(motion_data.poses), len(motion_data.times))
        self.assertEqual(len(motion_data.times), len(motion_data.entity_instances))
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
        (self.pni.select_participant().select_participant_mesh_path().
         select_neem_id().filter_by_neem_id('633819942a459501ef4d4209').
         select_from_tasks().
         join_task_participants().
         join_participant_mesh_path(is_outer=False)
         )
        mesh_path = self.pni.get_and_download_mesh_of_participant('soma:SM_Cup_2')
        self.assertIsInstance(mesh_path, str)

    def test_find_file_in_data_dir(self):
        path = self.pni._find_file_in_data_dir(['pr2'])
        self.assertIsInstance(path, str)
        with open(path, 'r') as f:
            self.assertTrue(f.read() is not None)

    def test_query_actions(self):
        df = self.pni.query_actions(participant_necessary=True).get_result().get_task_types(unique=True)
        self.assertTrue(len(df) > 0)

    def test_query_vr_actions(self):
        df = self.pni.query_vr_actions(participant_necessary=True).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.is_performed_by_type.value] == 'dul:NaturalPerson'))

    def test_query_pick_actions(self):
        df = self.pni.query_pick_actions().get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df['task_type'] == 'soma:PickingUp'))

    def test_query_vr_pick_actions(self):
        df = self.pni.query_vr_pick_actions().get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.is_performed_by_type.value] == 'dul:NaturalPerson'))

    # @skip("No complete navigate data available in the NEEM database")
    def test_query_navigate_actions(self):
        df = self.pni.query_navigate_actions().get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.task_type.value] == 'soma:Navigating'))

    def test_get_pre_task_state(self):
        qr = self.pni.query_tasks_semantic_data(['grasping'], participant_necessary=True,
                                                participant_base_link_necessary=True).get_result()
        tasks = qr.get_tasks()
        sql_neem_ids = qr.get_sql_neem_ids()
        task = tasks[1]  # 'soma:Grasping_HXUPQLZY'
        sql_neem_id = sql_neem_ids[1]  # 14
        state = self.pni.get_pre_task_state(task, sql_neem_id)
        self.assertIsInstance(state, pd.DataFrame)
        self.assertTrue(len(state) > 0)

    def test_query_all_task_data(self):
        q = self.pni.query_all_task_data(['pour'])
        df = q.get_result().df
        self.assertTrue(len(df) > 0)

    def test_query_transport_actions(self):
        q = self.pni.query_transport_actions()
        df = q.get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.task_type.value] == 'soma:Transporting'))

    @skip("No complete fetch data available in the NEEM database")
    def test_redo_fetch_action_for_neem(self):
        self.pni.redo_fetch_action()

    @skip("No complete pick data available in the NEEM database")
    def test_redo_pick_action_for_neem(self):
        self.pni.redo_pick_action()

    def test_redo_grasping_action_for_neem(self):
        self.pni.redo_grasping_action(14)

    def test_motion_replay(self):
        self.pni.replay_motion_of_neem([14], real_time=False)
