import csv
import psycopg2
import os

# --- Configuration ---
DB_NAME = "ayush_setu_db"
DB_USER = "postgres"
DB_PASS = "pass@123"
DB_HOST = "localhost"
DB_PORT = "5432"

CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'ayurveda_icd_match.csv')
# --------------------

def ingest_concept_map():
    """
    Connects to the PostgreSQL database and populates the concept_map table
    from the provided mappings CSV file. It is designed to be run multiple times;
    it will update existing mappings and insert new ones.
    """
    conn = None
    inserted_rows = 0
    updated_rows = 0
    skipped_rows = 0
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        print("Database connection successful.")

        if not os.path.exists(CSV_FILE_PATH):
            print(f"Error: CSV file not found at {CSV_FILE_PATH}")
            return

        print(f"Reading mapping data from {CSV_FILE_PATH}...")
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # This query is robust. If a mapping for a source_code already exists,
            # it will UPDATE it. If not, it will INSERT it.
            upsert_query = """
                INSERT INTO concept_map (source_code, target_code, target_display, equivalence) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_code) 
                DO UPDATE SET 
                    target_code = EXCLUDED.target_code,
                    target_display = EXCLUDED.target_display,
                    equivalence = EXCLUDED.equivalence
                RETURNING xmax; 
            """
            # xmax=0 for an insert, non-zero for an update
            
            # 'relatedto' is a safer, more technically correct default than 'equivalent'.
            equivalence_value = 'relatedto'

            for row in reader:
                # Use 'ayurveda_code' for the source, as per your CSV
                source_code = row.get('ayurveda_code', '').strip()
                target_code = row.get('icd_code', '').strip()
                target_display = row.get('icd_title', '').strip()

                if not source_code or not target_code:
                    skipped_rows += 1
                    continue

                try:
                    cur.execute(upsert_query, (source_code, target_code, target_display, equivalence_value))
                    result = cur.fetchone()[0]
                    if result == 0:
                        inserted_rows += 1
                    else:
                        updated_rows += 1
                except psycopg2.errors.ForeignKeyViolation:
                    # This error means the `source_code` from the map CSV does not exist
                    # in the `namaste_codesystem` table. We must skip it.
                    print(f"Warning: Skipping code '{source_code}' because it's not in the namaste_codesystem table.")
                    conn.rollback() # Rollback this specific failed transaction
                    skipped_rows += 1
                    continue
            
            conn.commit()
            print(f"\nConceptMap ingestion complete.")
            print(f"Successfully inserted {inserted_rows} new mappings.")
            print(f"Successfully updated {updated_rows} existing mappings.")
            if skipped_rows > 0:
                print(f"Skipped {skipped_rows} rows due to missing data or foreign key violations.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\nAn error occurred: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    ingest_concept_map()