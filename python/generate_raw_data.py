"""
generate_raw_data.py

Goal: 
- Generate "raw" healthcare claims data as a CSV file.
- Raw means: imperfect, messy, inconsistent on purpose (like the real world).

What this script produces:
- data/raw/claims_raw.csv

How to run (from repo root):
- python -u python/generate_raw_data.py

No database required just yet. This is step 1 (Generate raw)

"""

from __future__ import annotations # This will allow future-style hints on older python versions 

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

# -------------------------------------------------------------------
# Configuration 
# -------------------------------------------------------------------

# Where to write the raw output file(relative to repo root)
RAW_DIR = Path("data") / "raw"
RAW_FILE = RAW_DIR / "claims_raw.csv"

# How many rows of "raw claims" to generate
N_ROWS = 20_000

# Random seed for reproducibility:
# If this constant is kept, we get the same dataset every run, which is great for debugging
RANDOM_SEED = 42

# Percent of rows with some "mistakes" such as missing fields, odd formatting, etc..

MESS_RATE = 0.12

# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------


def random_date_within_days(days_back: int = 365) -> datetime:
    """
    Return a random datetime within the last 'days_back' days.
    """
    now = datetime.now()
    # Pick a random number of days + seconds back from now
    delta_days = random.randint(0, days_back)
    delta_seconds = random.randint(0, 24 * 60 * 60 - 1)
    return now - timedelta(days=delta_days, seconds=delta_seconds)


def maybe_none(value, probability: float) -> str | None:
    """
    Return None with probability 'probability', otherwise return the provided value.
    """
    return None if random.random() < probability else value


def messy_date_format(dt: datetime) -> str:
    """
    Return the same datetime in inconsistent string formats.
    """
    formats = [
        "%Y-%m-%d",      # 2026-02-26
        "%m/%d/%Y",      # 02/26/2026
        "%Y/%m/%d",      # 2026/02/26
        "%d-%b-%Y",      # 26-Feb-2026
        "%Y-%m-%d %H:%M:%S",     # 2026-02-26 11:43:00
    ]
    fmt = random.choice(formats)
    return dt.strftime(fmt)


def messy_money(amount: float) -> str:
    """
    Represent a numeric amount in inconsistent money formats.
    
    Examples: "123.45", "$123.45", "123,45", " 123.45  "
    """
    styles = ["plain", "dollar", "comma_decimal", "whitespace"]
    style = random.choice(styles)

    if style == "plain":
        return f"{amount:.2f}"
    if style == "dollar":
        return f"${amount:.2f}"
    if style == "comma_decimal":
        # Replace decimal dot with comma (common in some locales)
        return f"{amount:.2f}".replace(".", ",")
    # whitespace
    return f" {amount:.2f} "


def random_choice_weighted(options: list[tuple[str, float]]) -> str:
    """
    Choose a string from (value, weight) pairs.
    """
    values = [v for v, _w in options]
    weights = [w for _v, w in options]
    return random.choices(values, weights=weights, k=1)[0]



# --------------------------------------------------------------------
# Main generator
# --------------------------------------------------------------------


def generate_claim_row(claim_id: int) -> dict[str, str | None]:
    """
    Generate one raw claim row.
    
    We intentionally produce:
    - inconsistent date formatting
    - inconsistent money formatting
    - occasional missing values
    - occasional strange capitalization / whitespace 
    
    We'll clean this in the next pipeline step
    """

    # Basic identifiers
    member_id = f"M{random.randint(10000, 99999)}"
    provider_id = f"P{random.randint(1000, 9999)}"

    # Choose a service date
    service_dt = random_date_within_days(days_back=365)
    service_date_str = messy_date_format(service_dt)

    # Procedure and diagnosis codes (simplified)
    # CPT codes are usually 5 digits; ICD-10 diagnosis codes vary
    cpt_code = str(random.randint(10000, 99999))
    dx_code = random.choice(["E11.9", "I10", "J45.909", "M54.5", "F41.9", "K21.9"])

    # Claim status with weights (most are "PAID")

    status = random_choice_weighted([
        ("PAID", 0.78),
        ("DENIED", 0.12),
        ("PENDED", 0.06),
        ("REVERSED", 0.04),
    ])

    # Amounts
    # Billed is usually >= allowed; paid <= allowed
    billed = round(random.uniform(50, 2500), 2)
    allowed = round(billed * random.uniform(0.50, 0.95), 2)
    paid = round(allowed * random.uniform(0.00, 1.00), 2)

    billed_str = messy_money(billed)
    allowed_str = messy_money(allowed)
    paid_str = messy_money(paid)

    # Member state (simulate Puerto Rico + US mix)
    member_state = random_choice_weighted([
        ("PR", 0.55),
        ("FL", 0.12),
        ("NY", 0.10),
        ("TX", 0.08),
        ("NJ", 0.07),
        ("CA", 0.08),
    ])

    # Sometimes raw text comes with strange whitespace/case
    if random.random() < 0.15:
        member_state = member_state.lower() # "pr" instead of "PR"
    if random.random() < 0.10:
        member_state = f" {member_state} "  # " PR " instead of "PR"

    row: dict[str, str | None] = {
        "claim_id": str(claim_id),
        "member_id": member_id,
        "provider_id": provider_id,
        "service_date": service_date_str,
        "cpt_code": cpt_code,
        "dx_code": dx_code,
        "status": status,
        "billed_amount": billed_str,
        "allowed_amount": allowed_str,
        "paid_amount": paid_str,
        "member_state": member_state,
    }

    # Inject "mess" at the row level: randomly blank out some fields
    # This simulates missing values from real source systems.
    if random.random() < MESS_RATE:
        # Pick 1 to 3 fields to blank out
        keys_to_blank = random.sample(list(row.keys()), k=random.randint(1, 3))
        for k in keys_to_blank:
            # Avoid blanking claim_id too often given its a primary key concept !
            if k == "claim_id" and random.random() < 0.9:
                continue
            row[k] = None     
    return row 


def write_csv(rows: list[dict[str, str | None]], output_path: Path) -> None:
    """
    Write rows to any CSV file.
    
    We keep None values as empty strings in the CSV.
    """

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Field order is essential for stable outputs
    fieldnames = list(rows[0].keys())

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Always write a header row
        writer.writeheader()

        # Write each row; DictWriter will convert None to"" automatically
        for r in rows:
            writer.writerow(r)


def main() -> None:
    """
    Orchestrate generation of raw dataset.
    """
    random.seed(RANDOM_SEED)

    # Generate rows
    rows: list[dict[str, str | None]] = []
    for i in range(1, N_ROWS +1):
        rows.append(generate_claim_row(claim_id=i))

    # Write the CSV
    write_csv(rows, RAW_FILE)

    # Print a small success summary (for sanity checks)
    print(f"[OK] Wrote raw claims CSV: {RAW_FILE}")
    print(f"[OK] Rows: {len(rows):,}")
    print(f"[OK] Example row: {rows[0]}")

if __name__ == "__main__":
    main()




