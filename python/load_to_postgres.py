r"""
load_to_postgres.py

Purpose: Load the cleaned claims CSV into PostgreSQL (table: claims_clean), 
and record every load as a "pipeline run".

The why of its existence:
In a pipeline, the "load" step is where cleaned data becomes queryable inside the database.
Without this step, schema and indexes may exist, but the tables stay empty.
Logging each run (and its rejects) makes the pipeline auditable: for any row in
the database, we can say which run loaded it and how many rows that run rejected.


What this script does:
1) Confirm the raw, clean, and rejects CSVs exist.
2) Reconcile counts: raw rows must equal clean rows + rejected rows (no row lost).
3) Connect to PostgreSQL using psycopg2.
4) Insert a row into pipeline_runs (status STARTED) and commit it right away.
5) In ONE transaction:
    - TRUNCATE + COPY the clean CSV into claim_cleans
    - stamp every loaded row with this run_id
    - insert the rejects into claims_rejects (tagged with run_id)
    - mark the run SUCCESS with its row counts
6) On any error: roll back the data, then mark the run FAILED with the error message.

Prerequisites (run once, in this order):
    psql -U postgres -d healthcare_claims -f sql/001_schema.sql
    psql -U postgres -d healthcare_claims -f sql/002_indexes.sql
    psql -U postgres -d healthcare_claims -f sql/003_portfolio_upgrade.sql
(003 creates pipeline_runs, claims_rejects, and the claims_clean.run_id column.
If you re-run 001, re-run 002 and 003 after it, because 001 recreates claim_clean.)

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

import csv
import os
from pathlib import Path
from typing import Optional
import uuid
from datetime import datetime

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

# ----------------------------------------------------
# Configuration (safe defaults)
# ----------------------------------------------------

# Repo root is assumed to be the parent folder of /python
REPO_ROOT = Path(__file__).resolve().parents[1]

# Input produced by generate_raw_data.py (used only to reconcile counts)
RAW_CSV_PATH = REPO_ROOT / "data" / "raw" / "claims_raw.csv"

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

def file_exists_or_die(path: Path, produced_by: str) -> None:
    """
    Ensure the required input file exists before attempting to load.
    produced_by: the script that creates this file (shown in the error message).
    """
    if not path.exists():
        raise FileNotFoundError(
            f"File not found at: {path}\n"
            f"Expected it from {produced_by}.\n"
            f"Try running: python -u .\\python\\{produced_by}"
        )

def count_csv_rows(path: Path) -> int:
    """
    Count data rows in a CSV (header excluded).

    We use the csv module instead of counting lines, because a quoted field
    could in theory contain a newline (the rejects file stores JSON payloads).
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  #skip header
        return sum(1 for _ in reader)

    
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
    run_id is not in the CSV; we fill it in a separate UPDATE right after.
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

def stamp_run_id(cur: PgCursor, run_id: str) -> None:
    """
    Tag every row in claims_clean with the current run_id.
    
    Safe because claims_clean was truncated at the start of this transaction,
    so every row in it was loaded by this run.
    """
    cur.execute("UPDATE claims_clean SET run_id = %s::uuid;", (run_id,))


def insert_rejects(cur: PgCursor, csv_path: Path, run_id: str) -> int:
    """
    Insert the rejects CSV into claims_rejects, tagged with run_id.
    
    Notes: 
    - claims_rejects keep HISTORY across runs (we do not truncate it).
      Filter by run_id (or join to the latest SUCCESS run) to see one run.
    - claim_id can be empty when the claim_id itself was missing -> store NULL.
    - raw_payload is a JSON string in the CSV -> cast to JSONB on insert.
    
    Returns the number of rejects inserted.
    """
    rows: list[tuple[str, Optional[str], str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            claim_id = r["claim_id"].strip() or None    # "" -> None (NULL)
            rows.append((run_id, claim_id, r["reject_reason"], r["raw_payload"]))


    if rows:
        # execute_values sends many rows in a few round trips (much faster
        # than one INSERT per row). The template casts each value to its type.
        execute_values(
            cur,
            """
            INSERT INTO claims_rejects (run_id, claim_id, reject_reason, raw_payload)
            VALUES %s
            """,
            rows,
            template="(%s::uuid, %s, %s, %s::jsonb)",
        )
    return len(rows)


def start_run(conn: PgConnection, run_id: str) -> None:
    """
    Record the start of a run and COMMIT immediately.
    
    Committing on its own means the run stays in pipeline_runs even if the
    load transaction later fails and rolls back (we then mark it FAILED).
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, status) VALUES (%s::uuid, 'STARTED');",
            (run_id,),
        )
    conn.commit()


def finish_run(cur: PgCursor, run_id: str, raw_rows: int, clean_rows: int, rejected_rows: int) -> None:
    """
    Mark the run SUCCESS with its row counts.
    Runs inside the load transaction, so SUCCESS is only saved if the data is too.
    """
    cur.execute(
        """
        UPDATE pipeline_runs
        SET finished_at     = now(),
            raw_rows        = %s,
            clean_rows      = %s,
            rejected_rows   = %s,
            status          = 'SUCCESS'
        WHERE run_id = %s::uuid;
        """,
        (raw_rows, clean_rows, rejected_rows, run_id),
    )


def fail_run(conn: PgConnection, run_id: str, error_message: str) -> None:
    """
    Mark the run FAILED with the error message, in its own transaction.
    Called after the load transaction was rolled back.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at     = now(),
                status          = 'FAILED',
                error_message   = %s
            WHERE run_id = %s::uuid;
            """,
            (error_message[:2000], run_id), # keep the message a reasonable size
        )
    conn.commit()

def main() -> None:
    """
    Run the full load step end-to-end.
    """
    print(f"[Load] Repo root: {REPO_ROOT}")
    

    # 1) Validate inputs
    file_exists_or_die(RAW_CSV_PATH, "generate_raw_data.py")
    file_exists_or_die(CLEAN_CSV_PATH, "clean_transform.py")
    file_exists_or_die(REJECTS_CSV_PATH, "clean_transform.py")
    print("[Load] Found raw, clean, and rejects CSVs. Proceeding...")

    # 2) Count file rows (used for reconciliation and the run log)
    raw_rows = count_csv_rows(RAW_CSV_PATH)
    clean_rows = count_csv_rows(CLEAN_CSV_PATH)
    rejected_rows = count_csv_rows(REJECTS_CSV_PATH)
    print(f"[Load] File rows: raw={raw_rows:,} clean={clean_rows:,} rejected={rejected_rows:,}")

    # 3) Connect to DB
    print(
        f"[Load] Connecting to Postgres: db={DB_NAME} user={DB_USER} host={DB_HOST}:{DB_PORT}"
    )
    
    conn = get_connection()
    conn.autocommit = False # We control commit/rollback manually (partial loads wont stick).

    # 4) Record the run (committed on its own)
    run_id = str(uuid.uuid4())
    start_run(conn, run_id)
    print(f"[Load] Started run: {run_id}")

    try:
        # Reconciliation: every raw row must end up either clean or rejected.
        # Checked inside the try so a mismatch is logged as a FAILED run.
        if raw_rows != clean_rows + rejected_rows:
            raise RuntimeError(
                f"Row reconciliation failed: raw {raw_rows:,} != "
                f"clean {clean_rows:,} + rejected {rejected_rows:,}. "
                f"Re-run clean_transform.py before loading."
            )
        print("[Load] Reconciliation OK: raw = clean + rejected.")

        with conn.cursor() as cur:
            # 5) Show current count
            before = count_rows(cur, "claims_clean")
            print(f"[Load] claims_clean rows BEFORE load: {before:,}")

            # 6) Prevent duplicates
            truncate_table(cur, "claims_clean")
            print("[Load] Truncated claims_clean to avoid duplicate loads.")

            # 7) Load via COPY, then tag rows with this run
            print("[Load] Copying CSV into claims_clean...")
            copy_csv_into_claims_clean(cur, CLEAN_CSV_PATH)
            stamp_run_id(cur, run_id)
            print("[Load] COPY completed and rows tagged with run_id.")

            # 8) Verify count after: the table must match the clean file exactly
            after = count_rows(cur, "claims_clean")
            print(f"[Load] claims_clean rows AFTER load: {after:,}")
            if after != clean_rows:
                raise RuntimeError(
                    f"Loaded {after:,} rows but clean CSV has {clean_rows:,}."
                )

            # 9) Load rejects for this run
            inserted = insert_rejects(cur, REJECTS_CSV_PATH, run_id)
            print(f"[Load] Inserted {inserted:,} rejects into claims_rejects.")

            # 10) Mark the run SUCCESS (same transaction as the data)
            finish_run(cur, run_id, raw_rows, clean_rows, rejected_rows)

        # 11) Commit the transaction
        conn.commit()
        print("[Load] Commit successful. Run {run_id} marked SUCCESS.")
    
    except Exception as exc:
        # Rollback on any failure, and record why the run failed
        conn.rollback()
        print("[Load] ERROR occurred. Rolled back transaction.")
        fail_run(conn, run_id, f"{type(exc).__name__}: {exc}")
        print(f"[Load] Run {run_id} marked FAILED.")
        raise

    finally:
        conn.close()
        print("[Load] Connection closed.")

if __name__ == "__main__":
    main()
