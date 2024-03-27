from unittest import TestCase

import pandas as pd

from neem_query import NeemQuery, TaskType, ParticipantType
from neem_query.neems_database import *
from neem_query.enums import ColumnLabel as CL


class TestNeemSqlAlchemy(TestCase):
    nq: NeemQuery

    def setUp(self):
        self.nq = NeemQuery("mysql+pymysql://newuser:password@localhost/test")

    def tearDown(self):
        self.nq.reset()

    def test_sql_like(self):
        tasks = (self.nq.session.query(DulExecutesTask).
                 filter(DulExecutesTask.dul_Task_o.like("%Pour%")).first())
        self.assertIsNotNone(tasks)

    def test_get_task_data(self):
        task_data = self.nq.get_task_data("Pour", use_regex=True)
        self.assertIsNotNone(task_data)

    def test_get_task_data_using_joins(self):
        task_data = self.nq.get_task_data_using_joins("Pour", use_regexp=True)
        self.assertIsNotNone(task_data)

    def test_join_task_participants(self):
        df = (self.nq.select_from_tasks().
              join_task_participants()).get_result().df
        self.assertIsNotNone(df)

    def test_join_task_participant_types(self):
        df = (self.nq.select_from(DulHasParticipant).
              join_participant_types()).get_result().df
        self.assertIsNotNone(df)

    def test_join_task_types(self):
        df = (self.nq.select_task().
              join_task_types()).get_result().df
        self.assertIsNotNone(df)

    def test_outer_join(self):
        outer_df = (self.nq.select_task().
                    join_task_participants(is_outer=True)).get_result().df
        self.nq.reset()
        df = (self.nq.select_task().
              join_task_participants(is_outer=False)).get_result().df
        self.assertIsNotNone(len(df) < len(outer_df))

    def test_multi_join(self):
        df = (self.nq.select_stamp().select_participant_type().
              select_tf_columns().
              select_tf_transform_columns().
              select_from_tasks().
              join_task_types().
              join_task_participants().
              join_participant_types().
              join_participant_base_link().
              join_task_time_interval().
              join_tf_on_time_interval().
              join_tf_transfrom().join_neems().join_neems_environment().
              filter_tf_by_base_link().
              filter_by_task_types("Pour", regexp=True).order_by_stamp()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_get_neem(self):
        neem = self.nq.session.query(Neem).first()
        self.assertIsNotNone(neem)

    def test_get_neem_ids(self):
        neem_ids = self.nq.session.query(Neem._id).all()
        self.assertTrue(len(neem_ids) > 0)

    def test_query_changed(self):
        query = self.nq.select_from_tasks().join_neems()
        self.assertTrue(self.nq.query_changed)
        result = query.get_result()
        self.assertFalse(query.query_changed)
        query.join_task_types()
        self.assertTrue(query.query_changed)

    def test_join_agent(self):
        df = (self.nq.select_agent().select(Neem.ID).select_from_tasks().join_neems().join_agent()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_join_agent_types(self):
        df = ((self.nq.select_agent_type().select(Neem.ID).select_from_tasks().
               join_neems().join_agent().join_agent_type()).get_result().df)
        self.assertTrue(len(df) > 0)

    def test_join_is_performed_by(self):
        df = (self.nq.select_is_performed_by().select(Neem.ID).select_from_tasks().
              join_neems().join_is_performed_by()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_join_object_mesh_path(self):
        df = (self.nq.select_object_mesh_path().select_participant().select(Neem.ID).select_from(DulHasParticipant).
              join_neems().join_object_mesh_path()).get_result().filter_dataframe({CL.neem_sql_id.value:[5]}).df
        self.assertTrue(len(df) > 0)

    def test_get_unique_task_types(self):
        df = self.nq.select_task_type().select_from_tasks().join_task_types().distinct().get_result().df
        self.assertTrue(len(df) > 0)
