import pandas as pd

from pycram.neems.neem_loader_sqlalchemy import NeemLoader
import pycram.neems.neems_database as ndb
from unittest import TestCase


class TestNeemSqlAlchemy(TestCase):
    nl: NeemLoader

    @classmethod
    def setUpClass(cls):
        cls.nl = NeemLoader("mysql+pymysql://newuser:password@localhost/test")

    def test_sql_like(self):
        tasks = (self.nl.session.query(ndb.DulExecutesTask).
                filter(ndb.DulExecutesTask.dul_Task_o.like("%Pour%")).first())
        print(tasks.dul_Task_o)

    def test_get_task_data(self):
        task_data = self.nl.get_task_data("Pour", use_regex=True)
        print(task_data)
        self.assertIsNotNone(task_data)

    def test_get_task_data_using_joins(self):
        task_data = self.nl.get_task_data_using_joins("Pour", use_regexp=True)
        print(task_data)
        self.assertIsNotNone(task_data)

    def test_get_neem(self):
        neem = self.nl.session.query(ndb.Neem).first()
        self.assertIsNotNone(neem)

    def test_get_neem_ids(self):
        neem_ids = self.nl.session.query(ndb.Neem._id).all()
        self.assertIsNotNone(neem_ids)

