import csv
import os
from sqlalchemy.orm import Session
from . import models

def ingest_namaste_codes(db: Session):
    """
    Populates the namaste_codesystem table from NAMASTE.csv using SQLAlchemy.
    """
    CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'NAMASTE.csv')
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"NAMASTE.csv not found at {CSV_FILE_PATH}")

    inserted_rows = 0
    with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get('NAMC_CODE', '').strip()
            if not code:
                continue
            
            # Check if the code already exists
            exists = db.query(models.NamasteCode).filter(models.NamasteCode.code == code).first()
            if not exists:
                db_record = models.NamasteCode(
                    code=code,
                    term=row.get('NAMC_term', '').strip(),
                    short_definition=row.get('short_definition', '').strip()
                )
                db.add(db_record)
                inserted_rows += 1
    
    db.commit()
    return inserted_rows

def ingest_concept_map(db: Session):
    """
    Populates the concept_map table from ayurveda_icd_match.csv using SQLAlchemy.
    """
    CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'ayurveda_icd_match.csv')
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"ayurveda_icd_match.csv not found at {CSV_FILE_PATH}")

    inserted_rows = 0
    updated_rows = 0
    skipped_rows = 0
    
    # Pre-fetch all existing NAMASTE codes for faster foreign key checks
    namaste_codes = {code[0] for code in db.query(models.NamasteCode.code).all()}

    with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_code = row.get('ayurveda_code', '').strip()
            target_code = row.get('icd_code', '').strip()
            if not source_code or not target_code:
                skipped_rows += 1
                continue

            # Foreign key check
            if source_code not in namaste_codes:
                skipped_rows += 1
                continue

            # Check if a map for the source_code already exists
            existing_map = db.query(models.ConceptMap).filter(models.ConceptMap.source_code == source_code).first()
            
            if existing_map:
                # Update existing map
                existing_map.target_code = row.get('icd_code', '').strip()
                existing_map.target_display = row.get('icd_title', '').strip()
                existing_map.equivalence = 'relatedto'
                updated_rows += 1
            else:
                # Insert new map
                db_record = models.ConceptMap(
                    source_code=source_code,
                    target_code=row.get('icd_code', '').strip(),
                    target_display=row.get('icd_title', '').strip(),
                    equivalence='relatedto'
                )
                db.add(db_record)
                inserted_rows += 1

    db.commit()
    return {"inserted": inserted_rows, "updated": updated_rows, "skipped": skipped_rows}
