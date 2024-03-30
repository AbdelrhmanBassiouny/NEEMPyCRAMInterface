from unittest import TestCase

import pandas as pd
from sqlalchemy import between, and_, func, not_

from neem_query import NeemQuery
from neem_query.neems_database import *
from neem_query.enums import ColumnLabel as CL, PerformerBaseLinkName, PerformerTfHeader, PerformerTf, \
    ParticipantBaseLinkName, ParticipantBaseLink, IsPerformedByType


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
              select_tf_columns().select_tf_header_columns().
              select_tf_transform_columns().
              select_from_tasks().
              join_task_types().
              join_task_participants().
              join_participant_types().
              join_participant_base_link().
              join_task_time_interval().
              join_tf_on_time_interval().join_tf_header_on_tf().
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
              join_neems().join_task_is_performed_by()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_join_object_mesh_path(self):
        df = (self.nq.select_object_mesh_path().select_participant().select(Neem.ID).select_from(DulHasParticipant).
              join_neems().join_object_mesh_path()).get_result().filter_dataframe({CL.neem_sql_id.value: [5]}).df
        self.assertTrue(len(df) > 0)

    def test_get_unique_task_types(self):
        df = self.nq.select_task_type().select_from_tasks().join_task_types().distinct().get_result().df
        self.assertTrue(len(df) > 0)

    def test_performer_base_link_name_with_tf(self):
        df = (self.nq.select_is_performed_by().select_performer_tf_columns().select_sql_neem_id().
              select_from(SomaIsPerformedBy).
              join_performer_base_link_name().join_neems()
              .join_performer_tf_on_base_link_name()
              .limit(100)
              ).get_result().df
        print(df)
        self.assertTrue(len(df) > 0)

    def test_participant_base_link_name_with_tf(self):
        df = (self.nq.select_participant().select_participant_tf_columns().select_sql_neem_id().
              select_from(DulHasParticipant).
              join_participant_base_link_name().join_neems()
              .join_participant_tf_on_base_link_name()
              .limit(100)
              ).get_result().df
        print(df)
        self.assertTrue(len(df) > 0)

    def test_base_link_name(self):
        df = (self.nq.select_task()
              .select_from_tasks()
              .join_task_time_interval()

              ###################
              # NEEM Data
              ###################
              # .select_sql_neem_id()
              # .join_neems()
              # .filter(not_(Neem.description.like("%VR%")))
              # .filter(Neem.description.like("%VR%"))

              ###################
              # Participants Data
              ###################
              .select_participant()
              .join_task_participants()
              .select_participant_tf_columns()

              # Base Link
              .select_participant_base_link()
              .join_participant_base_link()
              .join_participant_tf_on_base_link()

              # Base Link Name
              # .select_participant_base_link_name()
              # .join_participant_base_link_name()
              # .join_participant_tf_on_base_link_name()

              # Time Interval
              # .join_participant_tf_on_time_interval(begin_offset=0)

              .select_participant_tf_header_columns()
              .join_participant_tf_header_on_tf()

              .select_participant_tf_transform_columns()
              .join_participant_tf_transform()

              ###################
              # Performers Data
              ###################
              .select_is_performed_by()
              .join_task_is_performed_by()
              # .filter(SomaIsPerformedBy.dul_Agent_o.like("%pr2%"))

              .select_is_performed_by_type()
              .join_is_performed_by_type()
              .filter_by_performer_type(["Natural"], regexp=True)

              # .select_performer_tf_columns()

              # Base Link
              # .select_performer_base_link()
              # .join_performer_base_link()
              # .join_performer_tf_on_base_link()

              # Base Link Name
              # .select_performer_base_link_name()
              # .join_performer_base_link_name()
              # .join_performer_tf_on_base_link_name()

              # Time Interval
              # .join_performer_tf_on_time_interval(begin_offset=0)

              # .select_performer_tf_header_columns()
              # .join_performer_tf_header_on_tf()

              # .select_performer_tf_transform_columns()
              # .join_performer_tf_transform()

              ###################
              # TF Data
              ###################
              # .select_tf_columns()

              # .join_tf_on_time_interval(begin_offset=-40)
              # .filter_tf_by_base_link()

              # .join_tf_on_participant()

              # .select_tf_header_columns()
              # .join_tf_header_on_tf()

              # .select_tf_transfrom_columns()
              # .join_tf_transfrom()

              .limit(100)
              ).get_result().df
        print(df)
        self.assertTrue(len(df) > 0)
