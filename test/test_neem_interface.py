from unittest import TestCase

import pandas as pd
from neem_query.neem_interface import NeemInterface
from neem_query.neems_database import *
from neem_query.enums import TaskType, ParticipantType, SubTask, SubTaskType
from neem_query.query_result import QueryResult


class TestNeemInterface(TestCase):
    ni: NeemInterface
    all_neem_plans: QueryResult

    @classmethod
    def setUpClass(cls):
        # Connection to MariaDB NEEM database.
        cls.ni = NeemInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.all_neem_plans = cls.ni.get_all_plans().select_participant().get_result()

    def test_get_neem_ids(self):
        neem_ids = self.all_neem_plans.get_neem_ids()
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)

    def test_filter_by_neem_id(self):
        neem_ids = self.all_neem_plans.get_neem_ids()
        neem_df = self.all_neem_plans.filter_by_neem_id(neem_ids[0]).df
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_filter_by_participant_type(self):
        neem_df = self.all_neem_plans.filter_by_participant_type('soma:DesignedContainer').df
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_get_participants(self):
        participants = self.all_neem_plans.get_participants()
        self.assertIsInstance(participants, list)
        self.assertTrue(len(participants) > 0)

    def test_get_task_sequence_of_neem(self):
        df = self.ni.get_task_sequence_of_neem(2).get_result().df
        self.assertIsInstance(df, pd.DataFrame)

    def test_get_plan_of_neem(self):
        df = self.ni.get_plan_of_neem(2).get_result().df
        self.assertIsInstance(df, pd.DataFrame)


