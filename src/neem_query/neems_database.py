# coding: utf-8
from sqlalchemy import Column, Float, ForeignKey, String, Text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Neem(Base):
    __tablename__ = 'neems'

    ID = Column(INTEGER(11), primary_key=True)
    _id = Column(String(24), nullable=False, unique=True)
    description = Column(String(255))
    image = Column(Text)
    name = Column(String(255))
    url = Column(String(255))
    created_at = Column(String(255))
    created_by = Column(String(255))
    visibility = Column(TINYINT(1))
    repo = Column(String(255))


class NeemsActivity(Base):
    __tablename__ = 'neems_activity'

    ID = Column(INTEGER(11), primary_key=True)
    name = Column(String(255))
    url = Column(String(255))


class NeemsProject(Base):
    __tablename__ = 'neems_projects'

    ID = Column(INTEGER(11), primary_key=True)
    name = Column(String(255))
    url = Column(String(255))


class TfHeader(Base):
    __tablename__ = 'tf_header'

    ID = Column(INTEGER(11), primary_key=True)
    seq = Column(INTEGER(11))
    stamp = Column(Float(asdecimal=True))
    frame_id = Column(String(255))


class TransformRotation(Base):
    __tablename__ = 'transform_rotation'

    ID = Column(INTEGER(11), primary_key=True)
    x = Column(Float(asdecimal=True))
    y = Column(Float(asdecimal=True))
    z = Column(Float(asdecimal=True))
    w = Column(Float(asdecimal=True))


class TransformTranslation(Base):
    __tablename__ = 'transform_translation'

    ID = Column(INTEGER(11), primary_key=True)
    x = Column(Float(asdecimal=True))
    y = Column(Float(asdecimal=True))
    z = Column(Float(asdecimal=True))


class DUL1IsRealizedBy(Base):
    __tablename__ = 'DUL1_isRealizedBy'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    DUL1_InformationObject_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SOMAOBJHasJointPositionMax(Base):
    __tablename__ = 'SOMA_OBJ_hasJointPositionMax'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    SOMA_OBJ_JointLimit_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SOMAOBJHasJointPositionMin(Base):
    __tablename__ = 'SOMA_OBJ_hasJointPositionMin'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    SOMA_OBJ_JointLimit_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SpecificationMetadataCopyright(Base):
    __tablename__ = 'SpecificationMetadata_copyright'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiCreator(Base):
    __tablename__ = 'dcmi_creator'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiDate(Base):
    __tablename__ = 'dcmi_date'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiDescription(Base):
    __tablename__ = 'dcmi_description'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiFormat(Base):
    __tablename__ = 'dcmi_format'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiIdentifier(Base):
    __tablename__ = 'dcmi_identifier'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiLanguage(Base):
    __tablename__ = 'dcmi_language'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiPublisher(Base):
    __tablename__ = 'dcmi_publisher'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiSubject(Base):
    __tablename__ = 'dcmi_subject'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DcmiTitle(Base):
    __tablename__ = 'dcmi_title'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DmMarketLeftMarker(Base):
    __tablename__ = 'dm_market_leftMarker'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DmMarketMarkerId(Base):
    __tablename__ = 'dm_market_markerId'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dm_market_DMShelfMarker_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DmMarketRightMarker(Base):
    __tablename__ = 'dm_market_rightMarker'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulClassify(Base):
    __tablename__ = 'dul_classifies'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Concept_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulDefine(Base):
    __tablename__ = 'dul_defines'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Description_s = Column(String(255), index=True)
    dul_Concept_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulDefinesTask(Base):
    __tablename__ = 'dul_definesTask'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Description_s = Column(String(255), index=True)
    dul_Task_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulExecutesTask(Base):
    __tablename__ = 'dul_executesTask'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Action_s = Column(String(255), index=True)
    dul_Task_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasComponent(Base):
    __tablename__ = 'dul_hasComponent'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasConstituent(Base):
    __tablename__ = 'dul_hasConstituent'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasLocation(Base):
    __tablename__ = 'dul_hasLocation'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasParameter(Base):
    __tablename__ = 'dul_hasParameter'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Concept_s = Column(String(255), index=True)
    dul_Parameter_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasPart(Base):
    __tablename__ = 'dul_hasPart'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasParticipant(Base):
    __tablename__ = 'dul_hasParticipant'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Event_s = Column(String(255), index=True)
    dul_Object_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasQuality(Base):
    __tablename__ = 'dul_hasQuality'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Quality_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasRegion(Base):
    __tablename__ = 'dul_hasRegion'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Region_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasRole(Base):
    __tablename__ = 'dul_hasRole'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Object_s = Column(String(255), index=True)
    dul_Role_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulHasTimeInterval(Base):
    __tablename__ = 'dul_hasTimeInterval'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Event_s = Column(String(255), index=True)
    dul_TimeInterval_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIncludesAction(Base):
    __tablename__ = 'dul_includesAction'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Situation_s = Column(String(255), index=True)
    dul_Action_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIncludesAgent(Base):
    __tablename__ = 'dul_includesAgent'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Situation_s = Column(String(255), index=True)
    dul_Agent_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIncludesObject(Base):
    __tablename__ = 'dul_includesObject'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Situation_s = Column(String(255), index=True)
    dul_Object_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIsAbout(Base):
    __tablename__ = 'dul_isAbout'

    ID = Column(INTEGER(11), primary_key=True)
    dul_InformationObject_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIsClassifiedBy(Base):
    __tablename__ = 'dul_isClassifiedBy'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Concept_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIsDescribedBy(Base):
    __tablename__ = 'dul_isDescribedBy'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Description_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIsSettingFor(Base):
    __tablename__ = 'dul_isSettingFor'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Situation_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulIsTaskOf(Base):
    __tablename__ = 'dul_isTaskOf'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Task_s = Column(String(255), index=True)
    dul_Role_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulPrecede(Base):
    __tablename__ = 'dul_precedes'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulRealize(Base):
    __tablename__ = 'dul_realizes'

    ID = Column(INTEGER(11), primary_key=True)
    dul_InformationRealization_s = Column(String(255), index=True)
    dul_InformationObject_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulSatisfy(Base):
    __tablename__ = 'dul_satisfies'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Situation_s = Column(String(255), index=True)
    dul_Description_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class DulUsesConcept(Base):
    __tablename__ = 'dul_usesConcept'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Description_s = Column(String(255), index=True)
    dul_Concept_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Inferred(Base):
    __tablename__ = 'inferred'

    ID = Column(INTEGER(11), primary_key=True)
    _id = Column(String(24), nullable=False)
    query = Column(String(255))
    module = Column(String(255))
    predicate = Column(String(255))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobDepthOfObject(Base):
    __tablename__ = 'knowrob_depthOfObject'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Float(asdecimal=True))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobFrameName(Base):
    __tablename__ = 'knowrob_frameName'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobHasLifetime(Base):
    __tablename__ = 'knowrob_hasLifetime'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobHasVisual(Base):
    __tablename__ = 'knowrob_hasVisual'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(TINYINT(1))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobHeightOfObject(Base):
    __tablename__ = 'knowrob_heightOfObject'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Float(asdecimal=True))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobQuaternion(Base):
    __tablename__ = 'knowrob_quaternion'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobTodo(Base):
    __tablename__ = 'knowrob_todo'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobTranslation(Base):
    __tablename__ = 'knowrob_translation'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobWidthOfObject(Base):
    __tablename__ = 'knowrob_widthOfObject'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Float(asdecimal=True))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class LinkFoodOnIntersectionOf(Base):
    __tablename__ = 'linkFoodOn_intersectionOf'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class LinkFoodOnOnProperty(Base):
    __tablename__ = 'linkFoodOn_onProperty'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class LinkFoodOnSomeValuesFrom(Base):
    __tablename__ = 'linkFoodOn_someValuesFrom'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class NeemsActivityIndex(Base):
    __tablename__ = 'neems_activity_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    neems_activity_ID = Column(ForeignKey('neems_activity.ID'), index=True)

    neem = relationship('Neem')
    neems_activity = relationship('NeemsActivity')


class NeemsAgentIndex(Base):
    __tablename__ = 'neems_agent_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    agent_values = Column(String(255))

    neem = relationship('Neem')


class NeemsEnvironmentIndex(Base):
    __tablename__ = 'neems_environment_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    environment_values = Column(String(255))

    neem = relationship('Neem')


class NeemsKeywordsIndex(Base):
    __tablename__ = 'neems_keywords_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    keywords_values = Column(String(255))

    neem = relationship('Neem')


class NeemsMailIndex(Base):
    __tablename__ = 'neems_mail_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    mail_values = Column(String(255))

    neem = relationship('Neem')


class NeemsModelVersionIndex(Base):
    __tablename__ = 'neems_model_version_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    model_version_values = Column(String(255))

    neem = relationship('Neem')


class NeemsProjectsIndex(Base):
    __tablename__ = 'neems_projects_index'

    ID = Column(INTEGER(11), primary_key=True)
    neems_ID = Column(ForeignKey('neems.ID'), index=True)
    list_index = Column(INTEGER(11))
    neems_projects_ID = Column(ForeignKey('neems_projects.ID'), index=True)

    neem = relationship('Neem')
    neems_project = relationship('NeemsProject')


class OboInOwlHasBroadSynonym(Base):
    __tablename__ = 'oboInOwl_hasBroadSynonym'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OboInOwlHasDbXref(Base):
    __tablename__ = 'oboInOwl_hasDbXref'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OboInOwlHasExactSynonym(Base):
    __tablename__ = 'oboInOwl_hasExactSynonym'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OboInOwlHasSynonym(Base):
    __tablename__ = 'oboInOwl_hasSynonym'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OboInOwlInSubset(Base):
    __tablename__ = 'oboInOwl_inSubset'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlAllValuesFrom(Base):
    __tablename__ = 'owl_allValuesFrom'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Restriction_s = Column(String(255), index=True)
    rdfs_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlAnnotatedProperty(Base):
    __tablename__ = 'owl_annotatedProperty'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlAnnotatedSource(Base):
    __tablename__ = 'owl_annotatedSource'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlAnnotatedTarget(Base):
    __tablename__ = 'owl_annotatedTarget'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlCardinality(Base):
    __tablename__ = 'owl_cardinality'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(INTEGER(11))
    owl_Restriction_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlComplementOf(Base):
    __tablename__ = 'owl_complementOf'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Class_s = Column(String(255), index=True)
    owl_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlDisjointWith(Base):
    __tablename__ = 'owl_disjointWith'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Class_s = Column(String(255), index=True)
    owl_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlEquivalentClas(Base):
    __tablename__ = 'owl_equivalentClass'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Class_s = Column(String(255), index=True)
    owl_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlHasKey(Base):
    __tablename__ = 'owl_hasKey'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlHasSelf(Base):
    __tablename__ = 'owl_hasSelf'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(TINYINT(1))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlHasValue(Base):
    __tablename__ = 'owl_hasValue'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    owl_Restriction_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlImport(Base):
    __tablename__ = 'owl_imports'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Ontology_s = Column(String(255), index=True)
    owl_Ontology_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlIntersectionOf(Base):
    __tablename__ = 'owl_intersectionOf'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Class_s = Column(String(255), index=True)
    rdf_List_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlInverseOf(Base):
    __tablename__ = 'owl_inverseOf'

    ID = Column(INTEGER(11), primary_key=True)
    owl_ObjectProperty_s = Column(String(255), index=True)
    owl_ObjectProperty_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlMaxQualifiedCardinality(Base):
    __tablename__ = 'owl_maxQualifiedCardinality'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(INTEGER(11))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlMember(Base):
    __tablename__ = 'owl_members'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlMinCardinality(Base):
    __tablename__ = 'owl_minCardinality'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(INTEGER(11))
    owl_Restriction_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlMinQualifiedCardinality(Base):
    __tablename__ = 'owl_minQualifiedCardinality'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Float(asdecimal=True))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlOnClas(Base):
    __tablename__ = 'owl_onClass'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlOnDataRange(Base):
    __tablename__ = 'owl_onDataRange'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlOnProperty(Base):
    __tablename__ = 'owl_onProperty'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Restriction_s = Column(String(255), index=True)
    rdf_Property_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlOneOf(Base):
    __tablename__ = 'owl_oneOf'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Class_s = Column(String(255), index=True)
    rdf_List_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlPropertyChainAxiom(Base):
    __tablename__ = 'owl_propertyChainAxiom'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlQualifiedCardinality(Base):
    __tablename__ = 'owl_qualifiedCardinality'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(INTEGER(11))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlSameA(Base):
    __tablename__ = 'owl_sameAs'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlSomeValuesFrom(Base):
    __tablename__ = 'owl_someValuesFrom'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Restriction_s = Column(String(255), index=True)
    rdfs_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlUnionOf(Base):
    __tablename__ = 'owl_unionOf'

    ID = Column(INTEGER(11), primary_key=True)
    owl_Class_s = Column(String(255), index=True)
    rdf_List_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlVersionIRI(Base):
    __tablename__ = 'owl_versionIRI'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class OwlVersionInfo(Base):
    __tablename__ = 'owl_versionInfo'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ProductTaxonomyHasDAN(Base):
    __tablename__ = 'product_taxonomy_has_DAN'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    v1_ProductOrService_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfFirst(Base):
    __tablename__ = 'rdf_first'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfRest(Base):
    __tablename__ = 'rdf_rest'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfType(Base):
    __tablename__ = 'rdf_type'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsComment(Base):
    __tablename__ = 'rdfs_comment'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Resource_s = Column(String(255), index=True)
    rdfs_Literal_o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsDomain(Base):
    __tablename__ = 'rdfs_domain'

    ID = Column(INTEGER(11), primary_key=True)
    rdf_Property_s = Column(String(255), index=True)
    rdfs_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsIsDefinedBy(Base):
    __tablename__ = 'rdfs_isDefinedBy'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Resource_s = Column(String(255), index=True)
    rdfs_Resource_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsLabel(Base):
    __tablename__ = 'rdfs_label'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Resource_s = Column(String(255), index=True)
    rdfs_Literal_o = Column(Text, index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsRange(Base):
    __tablename__ = 'rdfs_range'

    ID = Column(INTEGER(11), primary_key=True)
    rdf_Property_s = Column(String(255), index=True)
    rdfs_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsSeeAlso(Base):
    __tablename__ = 'rdfs_seeAlso'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Resource_s = Column(String(255), index=True)
    rdfs_Resource_o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsSubClassOf(Base):
    __tablename__ = 'rdfs_subClassOf'

    ID = Column(INTEGER(11), primary_key=True)
    rdfs_Class_s = Column(String(255), index=True)
    rdfs_Class_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class RdfsSubPropertyOf(Base):
    __tablename__ = 'rdfs_subPropertyOf'

    ID = Column(INTEGER(11), primary_key=True)
    rdf_Property_s = Column(String(255), index=True)
    rdf_Property_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopAdjacentLabelOfFacing(Base):
    __tablename__ = 'shop_adjacentLabelOfFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopArticleNumberOfLabel(Base):
    __tablename__ = 'shop_articleNumberOfLabel'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    shop_ShelfLabel_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopDan(Base):
    __tablename__ = 'shop_dan'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    shop_ArticleNumber_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopGtin(Base):
    __tablename__ = 'shop_gtin'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    shop_ArticleNumber_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopLabelOfFacing(Base):
    __tablename__ = 'shop_labelOfFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopLayerOfFacing(Base):
    __tablename__ = 'shop_layerOfFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopLeftSeparator(Base):
    __tablename__ = 'shop_leftSeparator'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopPreferredLabelOfFacing(Base):
    __tablename__ = 'shop_preferredLabelOfFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopProductInFacing(Base):
    __tablename__ = 'shop_productInFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    shop_Product_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopProductLabelOfFacing(Base):
    __tablename__ = 'shop_productLabelOfFacing'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class ShopRightSeparator(Base):
    __tablename__ = 'shop_rightSeparator'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaUsageGuideline(Base):
    __tablename__ = 'soma_UsageGuideline'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Text)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaContain(Base):
    __tablename__ = 'soma_contains'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Entity_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaDefinesBearer(Base):
    __tablename__ = 'soma_definesBearer'

    ID = Column(INTEGER(11), primary_key=True)
    soma_Affordance_s = Column(String(255), index=True)
    dul_Role_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaDefinesTrigger(Base):
    __tablename__ = 'soma_definesTrigger'

    ID = Column(INTEGER(11), primary_key=True)
    soma_Affordance_s = Column(String(255), index=True)
    dul_Role_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaFinishedBy(Base):
    __tablename__ = 'soma_finishedBy'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Event_s = Column(String(255), index=True)
    dul_Event_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasColor(Base):
    __tablename__ = 'soma_hasColor'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    soma_Color_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasDataFormat(Base):
    __tablename__ = 'soma_hasDataFormat'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_InformationRealization_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasDepth(Base):
    __tablename__ = 'soma_hasDepth'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    soma_ShapeRegion_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasDisposition(Base):
    __tablename__ = 'soma_hasDisposition'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Object_s = Column(String(255), index=True)
    soma_Disposition_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasExecutionState(Base):
    __tablename__ = 'soma_hasExecutionState'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Action_s = Column(String(255), index=True)
    soma_ExecutionStateRegion_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasFeature(Base):
    __tablename__ = 'soma_hasFeature'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    soma_Feature_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasFilePath(Base):
    __tablename__ = 'soma_hasFilePath'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_Entity_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasGoal(Base):
    __tablename__ = 'soma_hasGoal'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    dul_Goal_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasHeight(Base):
    __tablename__ = 'soma_hasHeight'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    soma_ShapeRegion_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasIntervalBegin(Base):
    __tablename__ = 'soma_hasIntervalBegin'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    dul_TimeInterval_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasIntervalEnd(Base):
    __tablename__ = 'soma_hasIntervalEnd'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    dul_TimeInterval_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasLocalization(Base):
    __tablename__ = 'soma_hasLocalization'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    soma_Localization_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasOrientationVector(Base):
    __tablename__ = 'soma_hasOrientationVector'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasPersistentIdentifier(Base):
    __tablename__ = 'soma_hasPersistentIdentifier'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_InformationRealization_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasPhysicalComponent(Base):
    __tablename__ = 'soma_hasPhysicalComponent'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    dul_PhysicalObject_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasPositionVector(Base):
    __tablename__ = 'soma_hasPositionVector'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasRGBValue(Base):
    __tablename__ = 'soma_hasRGBValue'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    soma_ColorRegion_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasShape(Base):
    __tablename__ = 'soma_hasShape'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    soma_Shape_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasTransparencyValue(Base):
    __tablename__ = 'soma_hasTransparencyValue'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(Float(asdecimal=True))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaHasWidth(Base):
    __tablename__ = 'soma_hasWidth'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    soma_ShapeRegion_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaIsPerformedBy(Base):
    __tablename__ = 'soma_isPerformedBy'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Action_s = Column(String(255), index=True)
    dul_Agent_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaIsProcessTypeOf(Base):
    __tablename__ = 'soma_isProcessTypeOf'

    ID = Column(INTEGER(11), primary_key=True)
    soma_ProcessType_s = Column(String(255), index=True)
    dul_Role_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaIsReificationOf(Base):
    __tablename__ = 'soma_isReificationOf'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_Description_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaStartedBy(Base):
    __tablename__ = 'soma_startedBy'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Event_s = Column(String(255), index=True)
    dul_Event_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class SomaSymbol(Base):
    __tablename__ = 'soma_symbol'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CapHasCapability(Base):
    __tablename__ = 'srdl2_cap_hasCapability'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Agent_s = Column(String(255), index=True)
    srdl2_cap_Capability_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompHasBodyPart(Base):
    __tablename__ = 'srdl2_comp_hasBodyPart'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalAgent_s = Column(String(255), index=True)
    dul_PhysicalObject_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompHasGripForceValue(Base):
    __tablename__ = 'srdl2_comp_hasGripForceValue'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    urdf_GripForceAttribute_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompHasMaximumSpeedValue(Base):
    __tablename__ = 'srdl2_comp_hasMaximumSpeedValue'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    srdl2_comp_MaximumSpeed_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompHasPayloadValue(Base):
    __tablename__ = 'srdl2_comp_hasPayloadValue'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(Float(asdecimal=True))
    srdl2_comp_PayloadAttribute_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompSphereInertiaMas(Base):
    __tablename__ = 'srdl2_comp_sphere_inertia_mass'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class Srdl2CompSphereInertiaRadiu(Base):
    __tablename__ = 'srdl2_comp_sphere_inertia_radius'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class TermsContributor(Base):
    __tablename__ = 'terms_contributor'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class TermsLicense(Base):
    __tablename__ = 'terms_license'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class TfTransform(Base):
    __tablename__ = 'tf_transform'

    ID = Column(INTEGER(11), primary_key=True)
    translation = Column(ForeignKey('transform_translation.ID'), index=True)
    rotation = Column(ForeignKey('transform_rotation.ID'), index=True)

    transform_rotation = relationship('TransformRotation')
    transform_translation = relationship('TransformTranslation')


class TripledbVersionString(Base):
    __tablename__ = 'tripledbVersionString'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class TrustSource(Base):
    __tablename__ = 'trust_source'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class TrustWikientry(Base):
    __tablename__ = 'trust_wikientry'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasBaseLink(Base):
    __tablename__ = 'urdf_hasBaseLink'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    urdf_Link_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasBaseLinkName(Base):
    __tablename__ = 'urdf_hasBaseLinkName'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasChildLink(Base):
    __tablename__ = 'urdf_hasChildLink'

    ID = Column(INTEGER(11), primary_key=True)
    urdf_Joint_s = Column(String(255), index=True)
    dul_PhysicalObject_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasEndLink(Base):
    __tablename__ = 'urdf_hasEndLink'

    ID = Column(INTEGER(11), primary_key=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    urdf_Link_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasEndLinkName(Base):
    __tablename__ = 'urdf_hasEndLinkName'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasNamePrefix(Base):
    __tablename__ = 'urdf_hasNamePrefix'

    ID = Column(INTEGER(11), primary_key=True)
    s = Column(String(255), index=True)
    o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasOrigin(Base):
    __tablename__ = 'urdf_hasOrigin'

    ID = Column(INTEGER(11), primary_key=True)
    dul_Entity_s = Column(String(255), index=True)
    soma_6DPose_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasParentLink(Base):
    __tablename__ = 'urdf_hasParentLink'

    ID = Column(INTEGER(11), primary_key=True)
    urdf_Joint_s = Column(String(255), index=True)
    dul_PhysicalObject_o = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class UrdfHasURDFName(Base):
    __tablename__ = 'urdf_hasURDFName'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    dul_PhysicalObject_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class V1HasEANUCC13(Base):
    __tablename__ = 'v1_hasEAN_UCC_13'

    ID = Column(INTEGER(11), primary_key=True)
    o = Column(String(255), index=True)
    v1_ProductOrService_s = Column(String(255), index=True)
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    neem = relationship('Neem')


class KnowrobQuaternionOIndex(Base):
    __tablename__ = 'knowrob_quaternion_o_index'

    ID = Column(INTEGER(11), primary_key=True)
    knowrob_quaternion_ID = Column(ForeignKey('knowrob_quaternion.ID'), index=True)
    list_index = Column(INTEGER(11))
    o_values = Column(Float(asdecimal=True))

    knowrob_quaternion = relationship('KnowrobQuaternion')


class KnowrobTranslationOIndex(Base):
    __tablename__ = 'knowrob_translation_o_index'

    ID = Column(INTEGER(11), primary_key=True)
    knowrob_translation_ID = Column(ForeignKey('knowrob_translation.ID'), index=True)
    list_index = Column(INTEGER(11))
    o_values = Column(Float(asdecimal=True))

    knowrob_translation = relationship('KnowrobTranslation')


class Tf(Base):
    __tablename__ = 'tf'

    ID = Column(INTEGER(11), primary_key=True)
    _id = Column(String(24), nullable=False)
    header = Column(ForeignKey('tf_header.ID'), index=True)
    child_frame_id = Column(String(255))
    transform = Column(ForeignKey('tf_transform.ID'), index=True)
    __recorded = Column(Float(asdecimal=True))
    __topic = Column(String(255))
    neem_id = Column(ForeignKey('neems._id'), nullable=False, index=True)

    tf_header = relationship('TfHeader')
    neem = relationship('Neem')
    tf_transform = relationship('TfTransform')


class TfHeaderSomaHasIntervalBegin(Base):
    __tablename__ = 'tf_header_soma_hasIntervalBegin'

    ID = Column(INTEGER(11), primary_key=True)
    tf_header_ID = Column(ForeignKey('tf_header.ID'), index=True)
    soma_hasIntervalBegin_ID = Column(ForeignKey('soma_hasIntervalBegin.ID'), index=True)
    soma_hasIntervalEnd_ID = Column(ForeignKey('soma_hasIntervalEnd.ID'), index=True)

    soma_hasIntervalBegin = relationship('SomaHasIntervalBegin')
    soma_hasIntervalEnd = relationship('SomaHasIntervalEnd')
    tf_header = relationship('TfHeader')
