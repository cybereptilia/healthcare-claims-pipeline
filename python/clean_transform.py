"""
clean_transform.py

Stage: Clean (raw CSV -> clean CSV)

What this script does:
1) Reads a raw claims CSV from: data/raw/claims_raw.csv
2) Applies basic cleaning rules:
  - trims whitespace
  - standardizes empty values to None
  - converts numeric fields to floats
  - ensures claim_id is present
  - parses service_date if present (keeps as YYYY-MM-DD string)
3) Writes cleaned output to:  data/clean/claims_clean.csv

Why we do this:
  - Real pipelines separate "raw" from "clean" s downstream steps can trust formats.
  - The clean stage should be repeatable and should create its own output folder.
  """

from __future__ import annotations

import json
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# -----------------------------------------------
# Paths (relative to repo root)
# -----------------------------------------------
RAW_PATH: Path = Path("data") / "raw" / "claims_raw.csv"
CLEAN_PATH: Path = Path("data") / "clean" / "claims_clean.csv"

# Output file for rejected rows (portfolio-grade behavior)
REJECTS_PATH: Path = Path("data") / "clean" / "claims_rejects.csv"

# Rejects file columns
REJECTS_COLUMNS: List[str] = [
    "claim_id",
    "reject_reason",
    "raw_payload",
]

# -----------------------------------------------
# Columns we expect in the raw data
# Adjust this list if your generator uses different column names !
# -----------------------------------------------
EXPECTED_COLUMNS: List[str] = [
    "claim_id",
    "member_id",
    "provider_id",
    "service_date",
    "cpt_code",
    "dx_code",
    "status",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
    "member_state",
]
#-------------------------------------------------
# Fields required by the claims_clean table (NOT NULL columns)
# ------------------------------------------------

REQUIRED_FIELDS: List[str] = [
    "claim_id",
    "member_id",
    "provider_id",
    "service_date",
    "cpt_code",
    "dx_code",
    "status",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
    "member_state",
]

# ------------------------------------------------
# Helper functions
# ------------------------------------------------
def normalize_str(value: Optional[str]) -> Optional[str]:
    """
    Normalize a string field:
    - Convert empty strings and whitespace-only strings to None
    - Strip surrounding whitespace
    """
    if value is None:
        return None
    
    v = value.strip()
    return v if v != "" else None 

def to_float(value: Optional[str]) -> Optional[float]:
    """
    Convert a string to float safely.
    Returns None if value is missing or invalid.
    """
    v = normalize_str(value)
    if v is None:
        return None
    
    # Remove currency symbols and whitespace
    v = v.replace("$", "").strip()

    # Convert comma-decimal format to dot decimal format
    # Example: "123,45" -> "123.45"
    if "," in v and "." not in v:
        v = v.replace(",", ".")

    # Remove thousands separators if user ever adds them for example "1,234.56"
    if "," in v and "." in v:
        v = v.replace(",", "")
    try:
        return float(v)
    except ValueError:
        return None
    
def normalize_date(value: Optional[str]) -> Optional[str]:
    """
    Normalize service_date into ISO format YYYY-MM-DD.
    Our generator output (from last logged output) looked like: '02/14/2026'
    So we parse that and convert to ISO.
    
    Returns None if missing or invalid.
    """
    v = normalize_str(value)
    if v is None:
        return None
    
    # Accept the formats our generator can emit
    formats = (
        "%Y-%m-%d",            # 2026-02-26
        "%m/%d/%Y",            # 02/26/2026
        "%Y/%m/%d",            # 2026/02/26
        "%d-%b-%Y",            # 26-Feb-2025
        "%Y-%m-%d %H:%M:%S",   # 2026-02-26 11:43:00
    )
    
    # Try common date formats. Add more formats if your data varies.
    for fmt in formats:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # If we cannot parse it, we return None rather than crashing the pipeline.
    return None

def read_csv(path: Path) -> List[Dict[str, str]]:
    """
    Read a CSV into a list of dictionaries (raw string values).
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Raw file not found at {path}. "
            f"Run the generator first: python -u .\\python\\generate_raw_data.py (or your raw script)."
        )
    
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row. Cannot proceed.")
        
        rows: List[Dict[str, str]] = []
        for row in reader:
            # Each row is a dict[str, str] mapping column -> raw string value
            rows.append(row)

        return rows
    
def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    """
    Write a list of dictionaries out to a CSV.
    Ensures the output folder exists.
    """
    # Create the parent directory (data/clean) if it does not exist.
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def clean_rows(raw_rows: List[Dict[str, str]]) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Core cleaning logic.

    Rules:
    - Normalize all string fields (strip, empty -> None)
    - Convert billed/allowed/paid amounts to floats
    - Normalize service_date to YYYY-MM-DD
    - Drop rows that do not have a claim_id (primary_identifier)
    - Capture rejects instead of silently dropping them
    """
    cleaned: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    REQUIRED_FIELDS = ["claim_id", "member_id", "provider_id", "service_date", "billed_amount", "allowed_amount", "paid_amount", "member_state", "status"]

    # Track how many rows we drop and why (debugging signal)
    dropped_missing_required = 0
    dropped_bad_date = 0
    dropped_bad_amounts = 0

    for r in raw_rows:

        # Keep a copy of the raw row for reject logging
        raw_payload = dict(r)

        # Normalize string fields
        claim_id = normalize_str(r.get("claim_id"))
        member_id = normalize_str(r.get("member_id"))
        provider_id = normalize_str(r.get("provider_id"))

        service_date = normalize_date(r.get("service_date"))

        cpt_code = normalize_str(r.get("cpt_code"))
        dx_code = normalize_str(r.get("dx_code"))
        status = normalize_str(r.get("status"))
        member_state = normalize_str(r.get("member_state"))
        if member_state is not None:
            member_state = member_state.upper()

        billed_amount = to_float(r.get("billed_amount"))
        allowed_amount = to_float(r.get("allowed_amount"))
        paid_amount = to_float(r.get("paid_amount"))


        # ----------------------------------------------------------------
        # Reject rules 
        # ----------------------------------------------------------------
        # Rule 1: Missing required fields
        missing = []
        if claim_id is None: missing.append("claim_id")
        if member_id is None: missing.append("member_id")
        if provider_id is None: missing.append("provider_id")
        if service_date is None: missing.append("service_date")
        if cpt_code is None: missing.append("cpt_code")
        if dx_code is None: missing.append("dx_code")
        if billed_amount is None: missing.append("billed_amount")
        if allowed_amount is None: missing.append("allowed_amount")
        if paid_amount is None: missing.append("paid_amount")
        if member_state is None: missing.append("member_state")
        if status is None: missing.append("status")

        if missing:
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": f"missing_required_fields:{','.join(missing)}",
                "raw_payload": json.dumps(raw_payload),
            })
            continue
        
        # In this scenario, we will tell the typechecker that required fields do exist
        assert billed_amount is not None
        assert allowed_amount is not None
        assert paid_amount is not None
        assert service_date is not None
        assert member_state is not None
        assert status is not None
        assert member_id is not None
        assert provider_id is not None
        assert claim_id is not None

        # Rule 2: Enforce business logic on amounts
        if billed_amount < 0 or allowed_amount < 0 or paid_amount < 0:
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": "negative_amount",
                "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
            })
            continue

        if not (paid_amount <= allowed_amount <= billed_amount):
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": "amount_order_violation_paid_allowed_billed",
                "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
            })
            continue

        # Rule 3: Status whitelist
        allowed_status = {"PAID", "DENIED", "PENDED", "REVERSED"}
        if status not in allowed_status:
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": f"invalid_status:{status}",
                "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
            })
            continue

        # Rule $: Member state must be 2 letters A-Z
        if len(member_state) != 2 or not member_state.isalpha() or not member_state.isupper():
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": f"invalid_state:{member_state}",
                "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
            })
            continue

        # Standardize casing for fields if we want consistent downstream
        if status is not None:
            status = status.upper()
        
        # Enforcing require columns 
        if service_date is None:
            dropped_bad_date += 1
            continue

        if billed_amount is None or allowed_amount is None or paid_amount is None:
            rejected.append({
                "claim_id": claim_id,
                "reject_reason": "missing_required_fields:amount",
                "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
            })
            continue

        # Basic validation: claim_id is required
        if claim_id is None:
            # We skip bad rows instead of killing the whole pipeline.
            # In real pipelines, we'd also log these to a "rejects" file.
            continue

        # All required strings must exist
        required_string_fields = [
            claim_id,
            member_id,
            provider_id,
            cpt_code,
            dx_code,
            status,
            member_state,
        ]


        cleaned.append(
            {
                "claim_id": claim_id,
                "member_id": member_id,
                "provider_id": provider_id,
                "service_date": service_date,
                "cpt_code": cpt_code,
                "dx_code": dx_code,
                "status": status,
                "billed_amount": billed_amount,
                "allowed_amount": allowed_amount,
                "paid_amount": paid_amount,
                "member_state": member_state,
            }
        )
    return cleaned, rejected

def main() -> None:
    """
    Entry point for the clean stage.
    """
    print(f"[Clean] Reading raw claims from: {RAW_PATH}")
    raw_rows = read_csv(RAW_PATH)

    print(f"[Clean] Raw rows read: {len(raw_rows):,}")

    cleaned, rejected = clean_rows(raw_rows)

    print(f"[Clean] Clean rows produced: {len(cleaned):,}")
    print(f"[Clean] Rejected rows produced: {len(rejected):,}")
    
    print(f"[Clean] Writing clean claims to: {CLEAN_PATH}")
    write_csv(CLEAN_PATH, cleaned, fieldnames=EXPECTED_COLUMNS)

    print(f"[Clean] Writing rejects to: {REJECTS_PATH}")
    write_csv(REJECTS_PATH, rejected, fieldnames=REJECTS_COLUMNS)

    

    print("[Clean] Done.")
    print(f"[Clean] Output file: {CLEAN_PATH}")
    print(f"[Clean] Rejects file: {REJECTS_PATH}")

if __name__ == "__main__":
    main()
