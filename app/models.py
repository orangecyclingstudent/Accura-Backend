from sqlalchemy import Column, Integer, String, ForeignKey, Text, DateTime
from sqlalchemy.sql import func
from .database import Base

# This model matches the rich structure of your NAMASTE.csv
class NamasteCode(Base):
    __tablename__ = "namaste_codesystem"
    code = Column(String, primary_key=True, index=True)
    term = Column(String)
    short_definition = Column(String)

# This model matches your concept_map table
class ConceptMap(Base):
    __tablename__ = "concept_map"
    map_id = Column(Integer, primary_key=True, index=True)
    source_code = Column(String, ForeignKey("namaste_codesystem.code"), nullable=False, unique=True)
    target_code = Column(String, nullable=False)
    target_display = Column(String)
    equivalence = Column(String, nullable=False)

# This model logs confirmed diagnoses
class DiagnosisRecord(Base):
    __tablename__ = "diagnosis_log"
    id = Column(Integer, primary_key=True, index=True)
    patient_id = Column(String, nullable=False, index=True)
    doctor_id = Column(String, nullable=False)
    namaste_code = Column(String, nullable=False)
    namaste_term = Column(String)
    icd_code = Column(String)
    icd_display = Column(String)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
