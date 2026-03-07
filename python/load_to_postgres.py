r"""
load_to_postgres.py

Purpose: Load the cleaned claims CSV into PostgreSQL (table: claims_clean)

The why of its existence:
In a pipeline, the "load" step is where cleaned data becomes queryable inside the database.
Without this step, schema and indexes may exist, but the tables stay empty.

What this script does:
1) Confirm the clean CSV exists.
2) Connect to PostgreSQL using psycopg2.
3) Load the CSV into claims_clean using COPY (fast).
4) Print row counts before and after for verification.

How to run (from repo root)
python -u .\python\load_to_postgres.py

Optional environment variables (recommended later):
    setx DB_NAME "healthcare_claims"
    setx DB_USER "postgres"
    setx DB_PASSWORD ""
    setx DB_HOST "localhost"
    setx DB_PORT "5432"
"""
from __future__ import annotations

import os
from pathlib import Path

import uuid
from datetime import datetime

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

# ----------------------------------------------------
# Configuration (safe defaults)
# ----------------------------------------------------

# Repo root is assumed to be the parent folder of /python
REPO_ROOT = Path(__file__).resolve().parents[1]

# Input file produced by clean_transform.py
CLEAN_CSV_PATH = REPO_ROOT / "data" / "clean" / "claims_clean.csv"

# Rejects file produced by clean_transform.py
REJECTS_CSV_PATH = REPO_ROOT / "data" / "clean" / "claims_rejects.csv"

# Database connection settings.
# These default values match what we have been using in psql.
DB_NAME = os.getenv("DB_NAME", "healthcare_claims")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

def get_connection() -> PgConnection:
    """
    Create and return a psycopg2 connection.
    Note that: If your Postgres requires a password, set DB_PASSWORD via environment variable
    or fill it temporarily (do not commit real passwords).
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )

def file_exists_or_die(path: Path) -> None:
    """
    Ensure the required input file exists before attempting to load.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Clean CSV not found at: {path}\n"
            f"Expected it from clean_transform.py.\n"
            f"Try running: python -u .\\python\\clean_transform.py"
        )

def count_rows(cur: PgCursor, table_name: str) -> int:
    """
    Return row count for the given table.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")

    # fetchone() should never be None here, but we guard for type-checkers.
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"COUNT(*) returned no rows for table: {table_name}")
    
    return int(row[0])

def truncate_table(cur: PgCursor, table_name: str) -> None:
    """
    Optional: empty the table before loading to avoid duplicates.
    Why TRUNCATE?
    If you run the pipeline multiple times, you do not want to duplicate rows. 
    TRUNCATE is fast and resets the table contents.
    
    Comment this out if you'd like to preserve old data !
    """
    cur.execute(f"TRUNCATE TABLE {table_name};")

def copy_csv_into_claims_clean(cur: PgCursor, csv_path: Path) -> None:
    """ 
    Load CSV into claims_clean using PostgreSQL  COPY.
    Important: Your CSV must have a header row matching the table columns.
     This script assumes your clean CSV columns match claims_clean columns.
      
    COPY is very fast compared to row-by-row inserts.
    """
    copy_sql = """
        COPY claims_clean(
        claim_id,
        member_id,
        provider_id,
        service_date,
        cpt_code,
        dx_code,
        status,
        billed_amount,
        allowed_amount,
        paid_amount,
        member_state
    )
    FROM STDIN
    WITH (FORMAT csv, HEADER true);
    """
    # Open the CSV file and stream it to Postgres COPY (fastest common method).
    with csv_path.open("r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)

def main() -> None:
    """
    Run the full load step end-to-end.
    """
    print(f"[Load] Repo root: {REPO_ROOT}")
    print(f"[Load] Looking for clean CSV: {CLEAN_CSV_PATH}")

    # 1) Validate input
    file_exists_or_die(CLEAN_CSV_PATH)
    print("[Load] Found clean CSV. Proceeding...")

    # 2) Connect to DB
    print(
        f"[Load] Connecting to Postgres: db={DB_NAME} user={DB_USER} host={DB_HOST}:{DB_PORT}"
    )
    
    conn = get_connection()
    conn.autocommit = False # We control commit/rollback manually (partial loads wont stick).

    try:
        with conn.cursor() as cur:
            # 3) Show current count
            before = count_rows(cur, "claims_clean")
            print(f"[Load] claims_clean rows BEFORE load: {before}")

            # 4) Prevent duplicates
            truncate_table(cur, "claims_clean")
            print("[Load] Truncated claims_clean to avoid duplicate loads.")

            # 5) Load via COPY
            print("[Load] Copying CSV into claims_clean...")
            copy_csv_into_claims_clean(cur, CLEAN_CSV_PATH)
            print("[Load] COPY completed.")

            # 6) Verify count after 
            after = count_rows(cur, "claims_clean")
            print(f"[Load] claims_clean rows AFTER load: {after}")

        # 7) Commit the transaction
        conn.commit()
        print("[Load] Commit successful. Load step complete.")
    
    except Exception:
        # Rollback on any failure
        conn.rollback()
        print("[Load] ERROR occurred. Rolled back transaction.")
        raise

    finally:
        conn.close()
        print("[Load] Connection closed.")

if __name__ == "__main__":
    main()
