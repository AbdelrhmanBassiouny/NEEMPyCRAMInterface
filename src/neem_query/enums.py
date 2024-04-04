from enum import Enum

from typing_extensions import Optional

from .neems_database import *
from sqlalchemy.orm import aliased

TaskType = aliased(RdfType, name='TaskType')
ParticipantType = aliased(RdfType, name='ParticipantType')
SubTask = aliased(DulExecutesTask, name='SubTask')
SubTaskType = aliased(RdfType, name='SubTaskType')
TaskParameterClassificationType = aliased(RdfType, name='TaskParameterClassificationType')
TaskParameterClassification = aliased(DulClassify, name='TaskParameterClassification')
Agent = aliased(DulClassify, name='Agent')
AgentType = aliased(RdfType, name='AgentType')
IsPerformedByType = aliased(RdfType, name='IsPerformedByType')
ParticipantBaseLink = aliased(UrdfHasBaseLink, name='ParticipantBaseLink')
PerformerBaseLink = aliased(UrdfHasBaseLink, name='PerformerBaseLink')
PerformerBaseLinkName = aliased(UrdfHasBaseLinkName, name='PerformerBaseLinkName')
ParticipantBaseLinkName = aliased(UrdfHasBaseLinkName, name='ParticipantBaseLinkName')
PerformerTf = aliased(Tf, name='PerformerTf')
ParticipantTf = aliased(Tf, name='ParticipantTf')
PerformerTfHeader = aliased(TfHeader, name='PerformerTfHeader')
ParticipantTfHeader = aliased(TfHeader, name='ParticipantTfHeader')
PerformerTfTransform = aliased(TfTransform, name='PerformerTfTransform')
ParticipantTfTransform = aliased(TfTransform, name='ParticipantTfTransform')
PerformerTransformTranslation = aliased(TransformTranslation, name='PerformerTransformTranslation')
PerformerTransformRotation = aliased(TransformRotation, name='PerformerTransformRotation')
ParticipantTransformTranslation = aliased(TransformTranslation, name='ParticipantTransformTranslation')
ParticipantTransformRotation = aliased(TransformRotation, name='ParticipantTransformRotation')

Initialized = None


class ColumnLabel(Enum):
    """
    Enum for column labels
    """
    task = "task"
    participant = "participant"
    task_type = "task_type"
    participant_type = "participant_type"
    subtask = "subtask"
    subtask_type = "subtask_type"
    translation_x = "x"
    translation_y = "y"
    translation_z = "z"
    orientation_x = "qx"
    orientation_y = "qy"
    orientation_z = "qz"
    orientation_w = "qw"
    frame_id = "frame_id"
    child_frame_id = "child_frame_id"
    stamp = "stamp"
    environment = "environment"
    neem_id = "neem_id"
    time_interval = "time_interval"
    time_interval_begin = "begin"
    time_interval_end = "end"
    task_parameter = "task_parameter"
    task_parameter_classification = "task_parameter_classification"
    task_parameter_classification_type = "task_parameter_classification_type"
    agent = "agent"
    agent_type = "agent_type"
    neem_sql_id = "neem_sql_id"
    is_performed_by = "is_performed_by"
    is_performed_by_type = "is_performed_by_type"
    object_mesh_path = "object_mesh_path"
    participant_base_link = "participant_base_link"
    performer_base_link = "performer_base_link"
    performer_base_link_name = "performer_base_link_name"
    participant_base_link_name = "participant_base_link_name"
    performer_frame_id = "performer_tf_frame_id"
    participant_frame_id = "participant_tf_frame_id"
    performer_child_frame_id = "performer_tf_child_frame_id"
    participant_child_frame_id = "participant_tf_child_frame_id"
    performer_stamp = "performer_tf_stamp"
    participant_stamp = "participant_tf_stamp"
    performer_translation_x = "performer_tf_x"
    performer_translation_y = "performer_tf_y"
    performer_translation_z = "performer_tf_z"
    performer_orientation_x = "performer_tf_qx"
    performer_orientation_y = "performer_tf_qy"
    performer_orientation_z = "performer_tf_qz"
    performer_orientation_w = "performer_tf_qw"
    participant_translation_x = "participant_tf_x"
    participant_translation_y = "participant_tf_y"
    participant_translation_z = "participant_tf_z"
    participant_orientation_x = "participant_tf_qx"
    participant_orientation_y = "participant_tf_qy"
    participant_orientation_z = "participant_tf_qz"
    participant_orientation_w = "participant_tf_qw"


column_to_label = {DulExecutesTask.dul_Task_o: ColumnLabel.task.value,
                   DulHasParticipant.dul_Object_o: ColumnLabel.participant.value,
                   TaskType.o: ColumnLabel.task_type.value,
                   ParticipantType.o: ColumnLabel.participant_type.value,
                   SubTask.dul_Task_o: ColumnLabel.subtask.value,
                   SubTaskType.o: ColumnLabel.subtask_type.value,
                   TransformTranslation.x: ColumnLabel.translation_x.value,
                   TransformTranslation.y: ColumnLabel.translation_y.value,
                   TransformTranslation.z: ColumnLabel.translation_z.value,
                   TransformRotation.x: ColumnLabel.orientation_x.value,
                   TransformRotation.y: ColumnLabel.orientation_y.value,
                   TransformRotation.z: ColumnLabel.orientation_z.value,
                   TransformRotation.w: ColumnLabel.orientation_w.value,
                   TfHeader.frame_id: ColumnLabel.frame_id.value,
                   Tf.child_frame_id: ColumnLabel.child_frame_id.value,
                   TfHeader.stamp: ColumnLabel.stamp.value,
                   NeemsEnvironmentIndex.environment_values: ColumnLabel.environment.value,
                   Neem._id: ColumnLabel.neem_id.value,
                   Neem.ID: ColumnLabel.neem_sql_id.value,
                   DulHasTimeInterval.dul_TimeInterval_o: ColumnLabel.time_interval.value,
                   SomaHasIntervalBegin.o: ColumnLabel.time_interval_begin.value,
                   SomaHasIntervalEnd.o: ColumnLabel.time_interval_end.value,
                   DulHasParameter.dul_Parameter_o: ColumnLabel.task_parameter_classification.value,
                   DulClassify.dul_Entity_o: ColumnLabel.task_parameter.value,
                   TaskParameterClassificationType.o: ColumnLabel.task_parameter_classification_type.value,
                   Agent.dul_Entity_o: ColumnLabel.agent.value,
                   AgentType.o: ColumnLabel.agent_type.value,
                   SomaIsPerformedBy.dul_Agent_o: ColumnLabel.is_performed_by.value,
                   IsPerformedByType.o: ColumnLabel.is_performed_by_type.value,
                   SomaHasFilePath.o: ColumnLabel.object_mesh_path.value,
                   ParticipantBaseLink.urdf_Link_o: ColumnLabel.participant_base_link.value,
                   PerformerBaseLink.urdf_Link_o: ColumnLabel.performer_base_link.value,
                   PerformerBaseLinkName.o: ColumnLabel.performer_base_link_name.value,
                   ParticipantBaseLinkName.o: ColumnLabel.participant_base_link_name.value,
                   PerformerTfHeader.frame_id: ColumnLabel.performer_frame_id.value,
                   ParticipantTfHeader.frame_id: ColumnLabel.participant_frame_id.value,
                   PerformerTf.child_frame_id: ColumnLabel.performer_child_frame_id.value,
                   ParticipantTf.child_frame_id: ColumnLabel.participant_child_frame_id.value,
                   PerformerTfHeader.stamp: ColumnLabel.performer_stamp.value,
                   ParticipantTfHeader.stamp: ColumnLabel.participant_stamp.value,
                   PerformerTransformTranslation.x: ColumnLabel.performer_translation_x.value,
                   PerformerTransformTranslation.y: ColumnLabel.performer_translation_y.value,
                   PerformerTransformTranslation.z: ColumnLabel.performer_translation_z.value,
                   PerformerTransformRotation.x: ColumnLabel.performer_orientation_x.value,
                   PerformerTransformRotation.y: ColumnLabel.performer_orientation_y.value,
                   PerformerTransformRotation.z: ColumnLabel.performer_orientation_z.value,
                   PerformerTransformRotation.w: ColumnLabel.performer_orientation_w.value,
                   ParticipantTransformTranslation.x: ColumnLabel.participant_translation_x.value,
                   ParticipantTransformTranslation.y: ColumnLabel.participant_translation_y.value,
                   ParticipantTransformTranslation.z: ColumnLabel.participant_translation_z.value,
                   ParticipantTransformRotation.x: ColumnLabel.participant_orientation_x.value,
                   ParticipantTransformRotation.y: ColumnLabel.participant_orientation_y.value,
                   ParticipantTransformRotation.z: ColumnLabel.participant_orientation_z.value,
                   ParticipantTransformRotation.w: ColumnLabel.participant_orientation_w.value}

# loop over the attributes of all classes in the neems_database module,
# and check if it has a neem_id attribute. If it does, add it to the column_to_label dictionary.
# for class_name in dir(neems_database):
#     attr = getattr(neems_database, class_name, None)
#     if hasattr(attr, 'neem_id'):
#         column_to_label[attr.neem_id] = ColumnLabel.neem_id.value
