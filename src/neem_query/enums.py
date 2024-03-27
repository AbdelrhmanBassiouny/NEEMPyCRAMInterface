from enum import Enum
from .neems_database import *
from sqlalchemy.orm import aliased


TaskType = aliased(RdfType, name='TaskType')
ParticipantType = aliased(RdfType, name='ParticipantType')
SubTask = aliased(DulExecutesTask, name='SubTask')
SubTaskType = aliased(RdfType, name='SubTaskType')
TaskParameterType = aliased(RdfType, name='TaskParameterType')
TaskParameterCategory = aliased(DulClassify, name='TaskParameterCategory')
Agent = aliased(DulClassify, name='Agent')
AgentType = aliased(RdfType, name='AgentType')
IsPerformedByType = aliased(RdfType, name='IsPerformedByType')

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
    participant_base_link = "participant_base_link"
    environment = "environment"
    neem_id = "neem_id"
    time_interval = "time_interval"
    time_interval_begin = "begin"
    time_interval_end = "end"
    task_parameter = "task_parameter"
    task_parameter_category = "task_parameter_category"
    task_parameter_type = "task_parameter_type"
    agent = "agent"
    agent_type = "agent_type"
    neem_sql_id = "neem_sql_id"
    is_performed_by = "is_performed_by"
    is_performed_by_type = "is_performed_by_type"
    object_mesh_path = "object_mesh_path"


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
                   UrdfHasBaseLink.urdf_Link_o: ColumnLabel.participant_base_link.value,
                   NeemsEnvironmentIndex.environment_values: ColumnLabel.environment.value,
                   Neem._id: ColumnLabel.neem_id.value,
                   Neem.ID: ColumnLabel.neem_sql_id.value,
                   DulHasTimeInterval.dul_TimeInterval_o: ColumnLabel.time_interval.value,
                   SomaHasIntervalBegin.o: ColumnLabel.time_interval_begin.value,
                   SomaHasIntervalEnd.o: ColumnLabel.time_interval_end.value,
                   DulHasParameter.dul_Parameter_o: ColumnLabel.task_parameter_category.value,
                   DulClassify.dul_Entity_o: ColumnLabel.task_parameter.value,
                   TaskParameterType.o: ColumnLabel.task_parameter_type.value,
                   Agent.dul_Entity_o: ColumnLabel.agent.value,
                   AgentType.o: ColumnLabel.agent_type.value,
                   SomaIsPerformedBy.dul_Agent_o: ColumnLabel.is_performed_by.value,
                   IsPerformedByType.o: ColumnLabel.is_performed_by_type.value,
                   SomaHasFilePath.o: ColumnLabel.object_mesh_path.value}


# loop over the attributes of all classes in the neems_database module,
# and check if it has a neem_id attribute. If it does, add it to the column_to_label dictionary.
# for class_name in dir(neems_database):
#     attr = getattr(neems_database, class_name, None)
#     if hasattr(attr, 'neem_id'):
#         column_to_label[attr.neem_id] = ColumnLabel.neem_id.value
