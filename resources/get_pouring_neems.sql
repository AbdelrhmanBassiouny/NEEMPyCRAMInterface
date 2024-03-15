Select distinct task.dul_Task_o,
                task_type.o                    as executes_task_type,
                task.neem_id                   as neem_id,
                environment.environment_values as environment,
#                 n.name,
#                 n.created_by                 as creator,
                has_participant.dul_Object_o   as has_participant,
                participant_type.o             as has_participant_type,
                tf.child_frame_id              as child_frame_id,
                tfh.frame_id                   as frame_id,
                hib.o                          as start,
                tfh.stamp                      as stamp,
                hie.o                          as end,
                ttrans.x                       as tx,
                ttrans.y                       as ty,
                ttrans.z                       as tz,
                trot.x                         as rx,
                trot.y                         as ry,
                trot.z                         as rz,
                trot.w                         as rw

# Specify the tables you are interested in
From rdf_type as task_type,
     dul_executesTask as task,
     neems as n,
     dul_hasParticipant as has_participant,
     rdf_type as participant_type,
     dul_hasTimeInterval as hti,
     soma_hasIntervalBegin as hib,
     soma_hasIntervalEnd as hie,
#      tf_header_soma_hasIntervalBegin as tf_ib,
     tf_header as tfh,
     tf,
     tf_transform as tft,
     transform_rotation as trot,
     transform_translation as ttrans,
     urdf_hasBaseLink as base_link,
     neems_environment_index as environment

# Context Specification
WHERE task_type.s = task.dul_Task_o
  and task_type.o Like '%Pour%'
  and has_participant.dul_Event_s = task.dul_Action_s
  and participant_type.s = has_participant.dul_Object_o
#   and participant_type.o IN ('soma:DesignedContainer', 'soma:Hand')
  and participant_type.o != 'owl:NamedIndividual'
  and base_link.dul_PhysicalObject_s = has_participant.dul_Object_o
  and hti.dul_Event_s = task.dul_Action_s
  and hib.dul_TimeInterval_s = hti.dul_TimeInterval_o
  and hie.dul_TimeInterval_s = hib.dul_TimeInterval_s
#   and tf_ib.soma_hasIntervalBegin_ID = hib.ID
#   and tf_ib.soma_hasIntervalEnd_ID = hie.ID
  and tfh.stamp BETWEEN hib.o - 40 AND hie.o
  and tf.header = tfh.ID
  and tf.child_frame_id = SUBSTRING_INDEX(base_link.urdf_Link_o, ':', '-1')
  and tft.ID = tf.ID
  and trot.ID = tft.rotation
  and ttrans.ID = tft.translation

  # Just ensuring no mixing between sql_neems
  and task_type.neem_id = task.neem_id
  and n._id = task.neem_id
  and has_participant.neem_id = n._id
  and participant_type.neem_id = n._id
  and hti.neem_id = n._id
  and hib.neem_id = n._id
  and hie.neem_id = n._id
  and tf.neem_id = n._id
  and environment.neems_ID = n.ID

ORDER BY tfh.stamp;