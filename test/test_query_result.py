from unittest import TestCase
from neem_query.query_result import QueryResult
from neem_query.neem_interface import NeemInterface
from neem_query.enums import ColumnLabel as CL


class TestQueryResult(TestCase):
    def setUp(self):
        self.ni = NeemInterface('mysql+pymysql://newuser:password@localhost/test')

    def test_get_neem_ids(self):
        self.ni.query_task_sequence_of_neem(2)
        qr = self.ni.get_result()
        n_ids = qr.get_neem_ids()
        self.assertIsInstance(n_ids, list)
        self.assertTrue(len(n_ids) > 0)
        self.assertIsInstance(n_ids[0], str)

    def test_filter_dataframe(self):
        self.ni.query_task_sequence()
        qr = self.ni.get_result()
        df = qr.filter_dataframe({CL.task_type.value: ['soma:PickingUp', 'soma:Placing']}).df
        self.assertTrue(len(df) > 0)
        column_types = df[CL.task_type.value].tolist()
        self.assertTrue('soma:PickingUp' in column_types)
        self.assertTrue('soma:Placing' in column_types)
        self.assertTrue(all(t in ['soma:PickingUp', 'soma:Placing'] for t in column_types))

    def test_filter_by_sql_neem_id(self):
        self.ni.query_task_sequence().join_neems().select_sql_neem_id()
        qr = self.ni.get_result()
        neem_id = 2
        df = qr.filter_by_sql_neem_id([neem_id]).df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.neem_sql_id.value] == neem_id))

    def test_filter_by_participant_type(self):
        self.ni.query_task_sequence().join_task_participants().join_participant_types().select_participant_type()
        qr = self.ni.get_result()
        df = qr.filter_by_participant_type(['soma:DesignedContainer']).df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.participant_type.value] == 'soma:DesignedContainer'))
