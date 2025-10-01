import csv
import psycopg2
import os

# --- Configuration ---
DB_NAME = "ayush_setu_db"
DB_USER = "postgres"
DB_PASS = "pass@123"
DB_HOST = "localhost"
DB_PORT = "5432"

CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'NAMASTE.csv')
# --------------------

def ingest_namaste_codes():
    """
    Connects to the PostgreSQL database and populates the NEW namaste_codesystem
    table structure from the multi-column NAMASTE CSV file.
    """
    conn = None
    inserted_rows = 0
    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        print("Database connection successful.")

        if not os.path.exists(CSV_FILE_PATH):
            print(f"Error: CSV file not found at {CSV_FILE_PATH}")
            return

        print(f"Reading data from {CSV_FILE_PATH}...")
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
            # Use DictReader to easily access columns by header name
            reader = csv.DictReader(f)
            
            insert_query = """
                INSERT INTO namaste_codesystem (code, term, short_definition)
                VALUES (%s, %s, %s) ON CONFLICT (code) DO NOTHING;
            """
            
            for row in reader:
                # Extract data using header names, providing default empty strings
                code = row.get('NAMC_CODE', '').strip()
                term = row.get('NAMC_term', '').strip()
                short_def = row.get('short_definition', '').strip()
                cur.execute(insert_query, (code, term, short_def))
                inserted_rows += cur.rowcount

            conn.commit()
            print(f"\nIngestion complete.")
            print(f"Successfully inserted {inserted_rows} new NAMASTE codes.")

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
    ingest_namaste_codes()