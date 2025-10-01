from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

# Schema for the /search endpoint response
class NamasteTerm(BaseModel):
    code: str
    term: Optional[str] = None
    short_definition: Optional[str] = None

    class Config:
        from_attributes = True

# Schema for the /translate endpoint request body
class TranslateRequest(BaseModel):
    namaste_code: str

# Schema for the POST /diagnosis/confirm endpoint request body
class ConfirmDiagnosisRequest(BaseModel):
    patient_id: str
    namaste_code: str

# Base schema for diagnosis records
class DiagnosisRecordBase(BaseModel):
    patient_id: str
    doctor_id: str
    namaste_code: str
    namaste_term: str
    icd_code: str
    icd_display: str

# Schema for the GET /diagnosis/history/{patient_id} endpoint response
class DiagnosisRecordResponse(DiagnosisRecordBase):
    id: int
    timestamp: datetime

    class Config:
        from_attributes = True
