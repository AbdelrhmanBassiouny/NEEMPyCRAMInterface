from unittest import TestCase
from pycram import neem_loader as nl
from sqlalchemy import create_engine, Engine
import pandas as pd


class TestNeemLoader(TestCase):
    engine: Engine
    all_neems_df: pd.DataFrame

    @classmethod
    def setUpClass(cls):
        # Connection to MariaDB NEEM database.
        cls.engine = create_engine('mysql+pymysql://newuser:password@localhost/test')
        cls.all_neems_df = nl.get_dataframe_from_sql_query_file('../resources/get_pouring_neems.sql',
                                                                cls.engine)

    def test_get_neem_ids(self):
        neem_ids = nl.get_neem_ids(self.all_neems_df)
        self.assertIsInstance(neem_ids, list)
        self.assertTrue(len(neem_ids) > 0)

    def test_filter_by_neem_id(self):
        neem_ids = nl.get_neem_ids(self.all_neems_df)
        neem_df = nl.filter_by_neem_id(self.all_neems_df, neem_ids[0])
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_filter_by_participant_type(self):
        neem_df = nl.filter_by_participant_type(self.all_neems_df, 'soma:DesignedContainer')
        self.assertIsInstance(neem_df, pd.DataFrame)
        self.assertTrue(len(neem_df) > 0)

    def test_get_participants(self):
        participants = nl.get_participants(self.all_neems_df)
        self.assertIsInstance(participants, list)
        self.assertTrue(len(participants) > 0)


