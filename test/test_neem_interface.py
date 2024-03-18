from unittest import TestCase

import pandas as pd
from neem_query.neem_interface import NeemInterface
from neem_query.neems_database import *
from neem_query.enums import TaskType, ParticipantType, SubTask, SubTaskType


class TestNeemInterface(TestCase):
    ni: NeemInterface
    all_neems_df: pd.DataFrame

    @classmethod
    def setUpClass(cls):
        # Connection to MariaDB NEEM database.
        cls.ni = NeemInterface('mysql+pymysql://newuser:password@localhost/test')
        cls.all_neems_df = cls.ni.get_all_plans().select_participant().get_result()

    def test_get_neem_ids(self):
        neem_ids = self.ni.get_neem_ids(self.all_neems_df)
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)

    def test_filter_by_neem_id(self):
        neem_ids = self.ni.get_neem_ids(self.all_neems_df)
        neem_df = self.ni.filter_by_neem_id(self.all_neems_df, neem_ids[0])
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_filter_by_participant_type(self):
        neem_df = self.ni.filter_by_participant_type(self.all_neems_df, 'soma:DesignedContainer')
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_get_participants(self):
        participants = self.ni.get_participants(self.all_neems_df)
        self.assertIsInstance(participants, list)
        self.assertTrue(len(participants) > 0)

    def test_get_task_sequence_of_neem(self):
        df = self.ni.get_task_sequence_of_neem(2).get_result()
        self.assertIsInstance(df, pd.DataFrame)

    def test_get_plan_of_neem(self):
        df = self.ni.get_plan_of_neem(2).get_result()
        self.assertIsInstance(df, pd.DataFrame)


