import pandas as pd
from sqlalchemy import Select, select
from sqlalchemy.orm import Query

from pycram.neems.neem_loader_sqlalchemy import NeemLoader, TaskType, ParticipantType
from  pycram.neems.neems_database import *
from unittest import TestCase


class TestNeemSqlAlchemy(TestCase):
    nl: NeemLoader

    @classmethod
    def setUpClass(cls):
        cls.nl = NeemLoader("mysql+pymysql://newuser:password@localhost/test")

    def tearDown(self):
        self.nl.reset()

    def test_sql_like(self):
        tasks = (self.nl.session.query(DulExecutesTask).
                filter(DulExecutesTask.dul_Task_o.like("%Pour%")).first())
        self.assertIsNotNone(tasks)

    def test_get_task_data(self):
        task_data = self.nl.get_task_data("Pour", use_regex=True)
        self.assertIsNotNone(task_data)

    def test_get_task_data_using_joins(self):
        task_data = self.nl.get_task_data_using_joins("Pour", use_regexp=True)
        self.assertIsNotNone(task_data)

    def test_join_task_participants(self):
        task_participants = self.nl.join_task_participants(select_task=True)
        df = pd.read_sql_query(task_participants.statement, self.nl.engine)
        self.assertIsNotNone(df)

    def test_join_task_participant_types(self):
        participant_types = self.nl.join_participant_types(select_participants=True)
        df = pd.read_sql_query(participant_types.statement, self.nl.engine)
        self.assertIsNotNone(df)

    def test_join_task_types(self):
        task_types = self.nl.join_task_types(select_tasks=True)
        df = pd.read_sql_query(task_types.statement, self.nl.engine)
        self.assertIsNotNone(df)

    def test_multi_join(self):
        nl_query = (self.nl.select(DulExecutesTask.dul_Task_o).join_task_types().
                    join_task_participants().
                    join_participant_types())
        df = pd.read_sql_query(nl_query.statement, self.nl.engine)
        print(df)
        self.assertIsNotNone(df)

    def test_get_neem(self):
        neem = self.nl.session.query(Neem).first()
        self.assertIsNotNone(neem)

    def test_get_neem_ids(self):
        neem_ids = self.nl.session.query(Neem._id).all()
        self.assertIsNotNone(neem_ids)

