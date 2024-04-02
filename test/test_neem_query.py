from unittest import TestCase

import pandas as pd
from sqlalchemy import select, Alias, Subquery, table, ColumnClause
from sqlalchemy.orm import aliased
from sqlalchemy.sql.base import ReadOnlyColumnCollection

from neem_query import NeemQuery
from neem_query.enums import ColumnLabel as CL, ParticipantBaseLinkName, ParticipantBaseLink, PerformerBaseLinkName, \
    PerformerTfTransform, PerformerTf, PerformerTfHeader, ParticipantTfTransform, ParticipantTf, \
    ParticipantTfHeader
from neem_query.neems_database import *


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
        task_data = self.nq._get_task_data("Pour", use_regex=True)
        self.assertIsNotNone(task_data)

    def test_get_task_data_using_joins(self):
        task_data = self.nq._get_task_data_using_joins("Pour", use_regexp=True)
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
        df = (self.nq.select_stamp().select_participant_type()
              .select_tf_header_columns()
              .select_tf_columns()
              .select_tf_transform_columns()
              .select_from_tasks().
              join_task_types().
              join_task_participants().
              join_participant_types().
              join_participant_base_link().
              join_task_time_interval()
              .join_tf_on_time_interval()
              .join_tf_transform()
              .join_neems_metadata().join_neems_environment().
              filter_tf_by_participant_base_link().
              filter_by_task_types("Pour", regexp=True).order_by_stamp()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_get_neem(self):
        neem = self.nq.session.query(Neem).first()
        self.assertIsNotNone(neem)

    def test_get_neem_ids(self):
        neem_ids = self.nq.session.query(Neem._id).all()
        self.assertTrue(len(neem_ids) > 0)

    def test_query_changed(self):
        query = self.nq.select_from_tasks().join_neems_metadata()
        self.assertTrue(self.nq.query_changed)
        _ = query.get_result()
        self.assertFalse(query.query_changed)
        query.join_task_types()
        self.assertTrue(query.query_changed)

    def test_join_agent(self):
        df = (self.nq.select_agent().select(Neem.ID).select_from_tasks().join_neems_metadata()
              .join_is_task_of().join_agent()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_join_agent_types(self):
        df = ((self.nq.select_agent_type().select(Neem.ID).select_from_tasks().
               join_neems_metadata().join_is_task_of().join_agent().join_agent_type()).get_result().df)
        self.assertTrue(len(df) > 0)

    def test_join_is_performed_by(self):
        df = (self.nq.select_is_performed_by().select(Neem.ID).select_from_tasks().
              join_neems_metadata().join_task_is_performed_by()).get_result().df
        self.assertTrue(len(df) > 0)

    def test_join_object_mesh_path(self):
        df = (self.nq.select_participant_mesh_path().select_participant().select(Neem.ID).select_from(DulHasParticipant).
              join_neems_metadata().join_participant_mesh_path()).get_result().filter_dataframe(
            {CL.neem_sql_id.value: [5]}).df
        self.assertTrue(len(df) > 0)

    def test_get_unique_task_types(self):
        df = self.nq.select_task_type().select_from_tasks().join_task_types().distinct().get_result().df
        self.assertTrue(len(df) > 0)

    def test_performer_base_link_name_with_tf(self):
        df = (self.nq.select_is_performed_by()
              .select_performer_tf_columns()
              .select_performer_base_link_name()
              .join_performer_base_link_name()
              .join_performer_tf_on_base_link_name()
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.performer_base_link_name.value][i] == df[CL.performer_child_frame_id.value][i]
                            for i in range(len(df))))

    def test_participant_base_link_name_with_tf(self):
        df = (self.nq.select_participant()
              .select_participant_tf_columns()
              .select_participant_base_link_name()
              .join_participant_base_link_name()
              .join_participant_tf_on_base_link_name()
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.participant_base_link_name.value][i] == df[CL.participant_child_frame_id.value][i]
                            for i in range(len(df)))
                        )

    def test_join_participant_base_link(self):
        df = (self.nq.distinct()
              .select_participant()
              .select(ParticipantBaseLink.dul_PhysicalObject_s)
              .select_participant_base_link()
              .join_participant_base_link()
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.participant.value][i] == df["ParticipantBaseLink_dul_PhysicalObject_s"][i]
                            for i in range(len(df)))
                        )

    def test_join_performer_base_link_name(self):
        df = (self.nq.distinct()
              .select_is_performed_by()
              .select(PerformerBaseLinkName.dul_PhysicalObject_s)
              .select_performer_base_link_name()
              .join_performer_base_link_name()
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.is_performed_by.value][i] == df["PerformerBaseLinkName_dul_PhysicalObject_s"][i]
                            for i in range(len(df))))

    def test_join_participant_base_link_name(self):
        df = (self.nq.distinct()
              .select_participant()
              .select(ParticipantBaseLinkName.dul_PhysicalObject_s)
              .select_participant_base_link_name()
              .join_participant_base_link_name()
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.participant.value][i] == df["ParticipantBaseLinkName_dul_PhysicalObject_s"][i]
                            for i in range(len(df))))

    def test_join_performer_tf_on_time_interval(self):
        df = (self.nq.select_is_performed_by()
              .select_time_columns()
              .select_performer_tf_columns()
              .select_performer_tf_header_columns()
              .select_performer_base_link_name()
              .select_from_tasks()
              .join_task_time_interval()
              .join_task_is_performed_by()
              .join_performer_base_link_name()
              .join_performer_tf_on_time_interval(begin_offset=0)
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.performer_base_link_name.value][i] == df[CL.performer_child_frame_id.value][i]
                            for i in range(len(df)))
                        )
        self.assertTrue(all(
            df[CL.time_interval_begin.value][i] <= df[CL.performer_stamp.value][i] <= df[CL.time_interval_end.value][i]
            for i in range(len(df)))
        )

    def test_join_participant_tf_on_time_interval(self):
        df = (self.nq.select_all_participants_data().select_time_columns()
              .select_from_tasks()
              .join_task_time_interval()
              .join_all_participants_data()
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df[CL.participant_base_link_name.value][i] == df[CL.participant_child_frame_id.value][i]
                            for i in range(len(df)))
                        )
        self.assertTrue(all(
            df[CL.time_interval_begin.value][i] <= df[CL.participant_stamp.value][i] <= df[CL.time_interval_end.value][
                i]
            for i in range(len(df)))
        )

    def test_join_performer_tf_transform(self):
        df = (self.nq.select_all_performers_data()
              .select(PerformerTfTransform.ID)
              .select(PerformerTf.transform)
              .select(PerformerTfHeader.ID)
              .select(PerformerTf.header)
              .select_from_tasks()
              .join_task_time_interval()
              .join_all_performers_data()
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df['PerformerTfTransform_ID'][i] == df['PerformerTf_transform'][i]
                            for i in range(len(df)))
                        )
        self.assertTrue(all(df['PerformerTfHeader_ID'][i] == df['PerformerTf_header'][i]
                            for i in range(len(df)))
                        )

    def test_join_participant_tf_transform(self):
        df = (self.nq.select_all_participants_data()
              .select(ParticipantTfTransform.ID)
              .select(ParticipantTf.transform)
              .select(ParticipantTfHeader.ID)
              .select(ParticipantTf.header)
              .select_from_tasks()
              .join_task_time_interval()
              .join_all_participants_data()
              .limit(100)
              ).get_result().df
        self.assertTrue(len(df) > 0)
        self.assertTrue(all(df['ParticipantTfTransform_ID'][i] == df['ParticipantTf_transform'][i]
                            for i in range(len(df)))
                        )
        self.assertTrue(all(df['ParticipantTfHeader_ID'][i] == df['ParticipantTf_header'][i]
                            for i in range(len(df)))
                        )

    def test_subquery(self):
        subquery = (select(ParticipantTf.child_frame_id, ParticipantTf.neem_id, ParticipantTfHeader.stamp,
                           ParticipantTfHeader.frame_id, ParticipantTfHeader.seq)
                    .select_from(ParticipantTf)
                    .join(ParticipantTfHeader)
                    .join(ParticipantTfTransform)).subquery("TfData")

        # Give the subquery object the ability to be used as a table with i
        TfData = subquery.alias("TfData")
        for c in TfData.c:
            col = next(iter(c.base_columns))
            setattr(TfData, c.name, col)
        df = pd.read_sql_query(select(TfData), self.nq.engine)
        self.assertTrue(len(df) > 0)
        print(df)

    def test_performer_and_participant(self):
        df = (self.nq.select_task()
              .select_from_tasks()
              .select_time_columns()
              .join_task_time_interval()

              ###################
              # NEEM Data
              ###################
              # .select_sql_neem_id()
              # .join_neems_metadata()
              # .filter(not_(Neem.description.like("%VR%")))
              # .filter(Neem.description.like("%VR%"))

              ###################
              # Participants Data
              ###################
              # .select_all_participants_data()
              # .join_all_participants_data(is_outer=True)

              ###################
              # Performers Data
              ###################
              .select_all_performers_data()
              .join_all_performers_data(is_outer=True)
              # .filter(SomaIsPerformedBy.dul_Agent_o.like("%pr2%"))
              # .filter_by_performer_type(["Natural"], regexp=True)

              .limit(100)
              ).get_result().df
        print(df)
        self.assertTrue(len(df) > 0)
