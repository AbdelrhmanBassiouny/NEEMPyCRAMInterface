from unittest import TestCase

import pandas as pd

from neem_query.enums import TaskType
from neem_query.neem_interface import NeemInterface
from neem_query.neems_database import *
from neem_query.query_result import QueryResult
from sqlalchemy import and_


class TestNeemInterface(TestCase):
    ni: NeemInterface
    all_neem_plans: QueryResult

    @classmethod
    def setUpClass(cls):
        # Connection to MariaDB NEEM database.
        cls.ni = NeemInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.all_neem_plans = cls.ni.query_plans().select_participant().get_result()
        cls.ni.reset()

    def tearDown(self):
        self.ni.reset()

    def test_get_neem_ids(self):
        neem_ids = self.all_neem_plans.get_neem_ids()
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)

    def test_filter_by_neem_id(self):
        neem_ids = self.all_neem_plans.get_neem_ids()
        neem_df = self.all_neem_plans.filter_by_neem_id([neem_ids[0]]).df
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_filter_by_participant_type(self):
        neem_df = self.all_neem_plans.filter_by_participant_type(['soma:DesignedContainer']).df
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_get_participants(self):
        participants = self.all_neem_plans.get_participant_types()
        self.assertIsInstance(participants, list)
        self.assertTrue(len(participants) > 0)

    def test_get_task_sequence_of_neem(self):
        df = self.ni.query_task_sequence_of_neem(2).get_result().df
        self.assertIsInstance(df, pd.DataFrame)

    def test_get_plan_of_neem(self):
        df = self.ni.query_plan_of_neem(2).get_result().df
        self.assertIsInstance(df, pd.DataFrame)

    def test_abstraction_levels(self):
        neem_id = 2
        df1 = self.ni.query_task_sequence_of_neem(neem_id).get_result().df

        self.ni.reset()

        df2 = (self.ni.select_task_type().
               select_time_columns().
               select_neem_id().
               select_from_tasks().
               join_task_types().
               join_neems().filter_by_sql_neem_id([neem_id]).
               join_task_time_interval().
               order_by_interval_begin()).get_result().df

        self.ni.reset()

        df3 = (self.ni.select(TaskType.o).
               select(SomaHasIntervalBegin.o).select(SomaHasIntervalEnd.o).
               select(DulExecutesTask.neem_id).
               select_from(DulExecutesTask).
               join(TaskType,
                    and_(TaskType.s == DulExecutesTask.dul_Task_o,
                         TaskType.neem_id == DulExecutesTask.neem_id,
                         TaskType.o != "owl:NamedIndividual")).
               join(Neem,
                    Neem._id == DulExecutesTask.neem_id).filter(Neem.ID == neem_id).
               join(DulHasTimeInterval,
                    and_(DulHasTimeInterval.dul_Event_s == DulExecutesTask.dul_Action_s,
                         DulHasTimeInterval.neem_id == DulExecutesTask.neem_id)).
               join(SomaHasIntervalBegin,
                    and_(SomaHasIntervalBegin.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                         SomaHasIntervalBegin.neem_id == DulHasTimeInterval.neem_id)).
               join(SomaHasIntervalEnd,
                    and_(SomaHasIntervalEnd.dul_TimeInterval_s == DulHasTimeInterval.dul_TimeInterval_o,
                         SomaHasIntervalEnd.neem_id == DulHasTimeInterval.neem_id)).
               order_by(SomaHasIntervalBegin.o)).get_result().df

        self.assertTrue(df1.equals(df2))
        self.assertTrue(df1.equals(df3))

    def test_get_task_semantic_data(self):
        df = self.ni.query_tasks_semantic_data(['Pick', 'Pour']).get_result().df
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df) > 0)

    def test_get_abhijit_pouring_tfs(self):
        (self.ni.select_tf_columns().select(Neem.created_by).select_participant_type().select_task_type().
         select_tf_transform_columns().
         select_from_tasks().
         join_task_time_interval().
         join_tf_on_time_interval(begin_offset=0).join_tf_transfrom().
         join_task_types().filter_by_task_types(['Pour'], regexp=True).
         join_all_task_participants_data().filter_tf_by_base_link().
         join_neems().
         filter(Neem.created_by.in_(['Abhijit Vyas','Abhijit'])).
         limit(100)
         )
        qr = self.ni.get_result()
        df = qr.df
        self.assertTrue(len(df) > 0)

