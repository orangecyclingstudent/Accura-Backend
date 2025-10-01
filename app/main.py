from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import or_
from typing import List, Dict, Any
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
import uuid
import httpx
import jwt
import datetime

from . import models, schemas
from .database import engine, get_db

# --- OAuth & App Configuration ---
ABHA_SERVER_URL = "http://127.0.0.1:8001"
CLIENT_ID = "accura_emr_client"
CLIENT_SECRET = "accura_emr_secret"
APP_SECRET_KEY = "a_very_secret_key_for_sessions"
PATIENT_CLIENT_REDIRECT_URI = "http://localhost:8000/consent/callback"
FRONTEND_CONSENT_SUCCESS_URI = "http://localhost:5173/add-patient/success"
MOCK_FHIR_ENDPOINT = "http://127.0.0.1:8001/fhir/bundle"

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Accura Terminology Service",
    description="A FHIR-compliant microservice for mapping NAMASTE and ICD-11 terminologies.",
    version="1.0.0"
)

app.add_middleware(SessionMiddleware, secret_key=APP_SECRET_KEY, same_site='lax', https_only=False)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Authentication Endpoints ---

@app.get("/auth/login", tags=["Authentication"])
def auth_login(request: Request):
    state = str(uuid.uuid4())
    request.session["oauth_state"] = state
    auth_url = f"{ABHA_SERVER_URL}/authorize?client_id={CLIENT_ID}&redirect_uri=http://localhost:8000/auth/callback&state={state}"
    return RedirectResponse(url=auth_url)

@app.get("/auth/callback", tags=["Authentication"])
async def auth_callback(request: Request, code: str, state: str):
    if state != request.session.get("oauth_state"):
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    async with httpx.AsyncClient() as client:
        token_response = await client.post(f"{ABHA_SERVER_URL}/token", data={"code": code})
    if token_response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to retrieve access token")
    token_data = token_response.json()
    access_token = token_data["access_token"]
    try:
        payload = jwt.decode(access_token, "mock_secret_key", algorithms=["HS256"])
        user_id = payload.get("sub")
        user_name = payload.get("name")
        if not user_id:
            raise HTTPException(status_code=400, detail="Invalid token: no sub claim")
    except jwt.PyJWTError:
        raise HTTPException(status_code=400, detail="Invalid token: could not decode")
    request.session["user_id"] = user_id
    request.session["user_name"] = user_name
    return RedirectResponse(url="http://localhost:5173/dashboard")

# --- Patient Consent Endpoints ---

@app.get("/consent/ask-patient", tags=["Patient Consent"])
def consent_ask_patient(request: Request):
    state = str(uuid.uuid4())
    request.session["oauth_state"] = state
    auth_url = f"{ABHA_SERVER_URL}/authorize?client_id={CLIENT_ID}&redirect_uri={PATIENT_CLIENT_REDIRECT_URI}&scope=patient_consent&state={state}"
    return RedirectResponse(url=auth_url)

@app.get("/consent/callback", tags=["Patient Consent"])
async def consent_callback(request: Request, code: str, state: str):
    if state != request.session.get("oauth_state"):
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    async with httpx.AsyncClient() as client:
        token_response = await client.post(f"{ABHA_SERVER_URL}/token", data={"code": code})
    if token_response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to retrieve patient token")
    token_data = token_response.json()
    request.session['consented_patient_token'] = token_data["access_token"]
    return RedirectResponse(url=FRONTEND_CONSENT_SUCCESS_URI)

@app.get("/api/consent/details", tags=["Consent"])
async def get_consent_details(request: Request):
    token = request.session.get("consented_patient_token")
    if not token:
        raise HTTPException(status_code=404, detail="No token found.")
    return {"access_token": token}

# --- Diagnosis & EMR Endpoints ---

@app.post("/diagnosis/confirm", status_code=201, tags=["Diagnosis"])
async def confirm_diagnosis(diag_request: schemas.ConfirmDiagnosisRequest, db: Session = Depends(get_db), request: Request = None):
    doctor_id = request.session.get("user_id")
    if not doctor_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    mapping = db.query(models.ConceptMap).filter(models.ConceptMap.source_code == diag_request.namaste_code).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping not found for NAMASTE code: {diag_request.namaste_code}")

    fhir_bundle = {
        "resourceType": "Bundle", "type": "transaction", "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "entry": [
            {"fullUrl": f"urn:uuid:{uuid.uuid4()}", "resource": {"resourceType": "Encounter", "status": "finished", "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB", "display": "ambulatory"}, "subject": {"reference": f"Patient/{diag_request.patient_id}"}, "participant": [{"individual": {"reference": f"Practitioner/{doctor_id}"}}]}, "request": {"method": "POST", "url": "Encounter"}},
            {"fullUrl": f"urn:uuid:{uuid.uuid4()}", "resource": {"resourceType": "Condition", "subject": {"reference": f"Patient/{diag_request.patient_id}"}, "code": {"text": mapping.target_display, "coding": [{"system": "NAMASTE", "code": mapping.source_code, "display": mapping.target_display}, {"system": "http://id.who.int/icd/release/11/mms", "code": mapping.target_code, "display": mapping.target_display}]}}, "request": {"method": "POST", "url": "Condition"}}
        ]
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(MOCK_FHIR_ENDPOINT, json=fhir_bundle)
        if response.status_code != 201:
            raise HTTPException(status_code=502, detail="Failed to save bundle to mock EMR.")

    log_entry = models.DiagnosisRecord(patient_id=diag_request.patient_id, doctor_id=doctor_id, namaste_code=mapping.source_code, namaste_term=mapping.target_display, icd_code=mapping.target_code, icd_display=mapping.target_display)
    db.add(log_entry)
    db.commit()

    return {"status": "success", "message": "Diagnosis confirmed and logged."}

@app.get("/diagnosis/history/{patient_id}", response_model=List[schemas.DiagnosisRecordResponse], tags=["Diagnosis"])
def get_diagnosis_history(patient_id: str, db: Session = Depends(get_db)):
    history = db.query(models.DiagnosisRecord).filter(models.DiagnosisRecord.patient_id == patient_id).order_by(models.DiagnosisRecord.timestamp.desc()).all()
    return history

# --- Terminology API Endpoints ---

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to the Accura Terminology Service API. Go to /docs for API documentation."}

@app.get("/terminology/names-only", response_model=List[str], tags=["Terminology"])
def get_all_namaste_names(db: Session = Depends(get_db)):
    results = db.query(models.NamasteCode.term).all()
    return [result[0] for result in results if result[0]]

@app.get("/search", response_model=List[schemas.NamasteTerm], tags=["Terminology"])
def search_terms(term: str, db: Session = Depends(get_db)):
    if not term:
        return []
    results = db.query(models.NamasteCode).filter(or_(models.NamasteCode.term.ilike(f"%{term}%"), models.NamasteCode.short_definition.ilike(f"%{term}%"))).limit(15).all()
    return results

@app.post("/translate", response_model=Dict[str, Any], tags=["Terminology"])
def translate_namaste_code(request: schemas.TranslateRequest, db: Session = Depends(get_db)):
    mapping = db.query(models.ConceptMap).filter(models.ConceptMap.source_code == request.namaste_code).first()
    if not mapping:
        raise HTTPException(status_code=404, detail=f"Mapping not found for NAMASTE code: {request.namaste_code}")
    fhir_parameters_response = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "result", "valueBoolean": True},
            {"name": "match", "part": [{"name": "equivalence", "valueCode": mapping.equivalence}, {"name": "concept", "valueCoding": {"system": "http://id.who.int/icd/release/11/mms", "code": mapping.target_code, "display": mapping.target_display}}]}
        ]
    }
    return fhir_parameters_response

# --- User Session Endpoint ---

@app.get("/api/users/me", tags=["Users"])
async def read_users_me(request: Request):
    user_id = request.session.get("user_id")
    user_name = request.session.get("user_name")
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"userId": user_id, "name": user_name}
