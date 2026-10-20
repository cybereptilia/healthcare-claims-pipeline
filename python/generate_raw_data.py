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

Design notes (why the data looks the way it does):
- Service dates fall inside a FIXED window (START_DATE..END_DATE), not 
"the last 365 days from now". Together with RANDOM_SEED, this makes 
every run produce the exact same file, and every month in the window
is a full month.
- CPT codes come from a small weighted catalog, and each code has its 
own billed range. This gives each procedure enough claims (and a 
distinct price level) for per-procedure averages to be meaningful.
- Claims come from a fixed pool of providers with uneven volume (a few 
large providers, many small ones), like a real payer network. A small
share of providers is "high-denial", so denial rates actually differ 
between providers.
- A small share of rows breaks a business rule (negative amount,
paid > allowed, unknown status, invalid state), so every reject 
rule in clean_transform.py gets exercised, not only the 
"missing field" rule.
- Only PAID claims have a paid amount above zero. DENIED, PENDED, and 
  REVERSED claims are generated with paid = 0.00.
"""

from __future__ import annotations # This will allow future-style hints on older python versions 

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import TypedDict

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

# Fixed service-date window: 12 full months.
# Change these two dates if you want a different period.
START_DATE = datetime(2025, 10, 1)
END_DATE = datetime(2026, 9, 30)

# Percent of rows with some "mistakes" such as missing fields, odd formatting, etc..
MESS_RATE = 0.12

# Percent of rows that break a business rule (see inject_rule_violation)
RULE_VIOLATION_RATE = 0.02

# Provider network
N_PROVIDERS = 250

# Share of providers whose claims are denied more often than average
HIGH_DENIAL_PROVIDER_SHARE = 0.08

# Status weights for a typical provider (most claims are PAID)
STATUS_WEIGHTS_NORMAL: list[tuple[str, float]] = [
    ("PAID", 0.78),
    ("DENIED", 0.12),
    ("PENDED", 0.06),
    ("REVERSED", 0.04),
]

# Status weights for a high-denial provider (denials more than double)
STATUS_WEIGHTS_HIGH_DENIAL: list[tuple[str, float]] = [
    ("PAID", 0.60),
    ("DENIED", 0.30),
    ("PENDED", 0.06),
    ("REVERSED", 0.04),
]

# CPT catalog: (cpt_code, weight, billed_min, billed_max)
# - weight: how common the procedure is (office visits 
# and labs dominate real claims)
# Codes are real CPT numbers; amounts are illustrative, not real fee schedules.
CPT_CATALOG: list[tuple[str, float, float, float]] = [
    # Office / outpatient visits (very common, low cost)
    ("99213", 0.16, 90, 180),
    ("99214", 0.12, 130, 260),
    ("99203", 0.05, 110, 220),
    ("99204", 0.04, 170, 330),
    # Lab work (very common, very low cost)
    ("80053", 0.08, 25, 90),
    ("85025", 0.08, 15, 60),
    ("36415", 0.07, 10, 30),
    ("81002", 0.03, 10, 35),
    ("87880", 0.03, 20, 60),
    # Vaccines, ECG, therapy, behavioral health (moderate)
    ("90686", 0.04, 30, 70),
    ("93000", 0.04, 40, 110),
    ("97110", 0.05, 50, 140),
    ("90837", 0.04, 140, 260),
    # Imaging (less common, higher cost)
    ("71046", 0.03, 70, 200),
    ("70450", 0.02, 300, 900),
    ("73721", 0.02, 600, 1800),
    # Emergency, hospital, procedures (least common, highest cost)
    ("99285", 0.03, 700, 2200),
    ("99232", 0.03, 150, 350),
    ("20610", 0.02, 150, 450),
    ("45378", 0.02, 900, 2500),
]

class Provider(TypedDict):
    """ One provider in the synthetic network (built by built_provider_pool)."""
    provider_id: str        # e.g. "P4821"
    volume_weight: float    # relative claim volume (higher = more claims)
    high_denial: bool       # True = denies claims more often than average


# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------


def random_service_datetime() -> datetime:
    """
    Return a random datetime between START_DATE and END_DATE (inclusive).
    """
    total_days = (END_DATE - START_DATE).days
    delta_days = random.randint(0, total_days)
    delta_seconds = random.randint(0, 24 * 60 * 60 -1)
    return START_DATE + timedelta(days=delta_days, seconds=delta_seconds)


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

def build_provider_pool(n_providers: int) -> list[Provider]:
    """
    Build a fixed provider network.
    
    Each provider gets:
    - provider_id: a unique ID like "P4821"
    - volume_weight: how many claims it tends to submit, relative to others.
        Uses 1 / sqrt(rank): rank 1 is the biggest provider, and volume drops off
        gradually, so even the smallest provider still has dozens of claims.
    - high_denial: True for a small share of providers whose claims are denied
        more often (gives the denial-rate chart a real signal to find).
        
    Must be called AFTER random.seed() so the pool is reproducible.
    """
    # Unique 4-digit IDs, same format as before (P1000..P9999)
    id_numbers = random.sample(range(1000, 10000), k=n_providers)

    providers:list[Provider] = []
    for rank, id_number in enumerate(id_numbers, start=1):
        providers.append(
            {
                "provider_id": f"P{id_number}",
                "volume_weight": 1 / (rank ** 0.5),
                "high_denial": random.random() < HIGH_DENIAL_PROVIDER_SHARE,
            }
        )
    return providers

def inject_rule_violation(row: dict[str, str | None], allowed: float, paid: float) -> None:
    """
    Break ONE business rule in the row (modifies the row in place).
    
    Each violation matches a reject rule in clean_transform.py:
    - negative_amount       -> "negative_amount"
    - paid_above_allowed    -> "amount_order_violation_paid_allowed_billed"
    - unknown_status        -> "invalid_status:<value>"
    - invalid_state         -> "invalid_state:<value>"
    """
    violation = random.choice(
        ["negative_amount", "paid_above_allowed", "unknown_status", "invalid_state"]
    )

    if violation == "negative_amount":
        # Use -1.00 when paid is 0 so the value is actually negative
        row["paid_amount"] = messy_money(-paid if paid > 0 else -1.00)
    elif violation == "paid_above_allowed":
        row["paid_amount"] = messy_money(allowed + random.uniform(1, 50))
    elif violation == "unknown_status":
        # Typos and codes from a different source system
        row["status"] = random.choice(["PAYED", "UNKNOWN", "APPROVED"])
    else:
        # Not 2 letters A-Z after cleaning (3 letters, or contains a digit)
        row["member_state"] = random.choice(["PRR", "1A", "XYZ"])


# --------------------------------------------------------------------
# Main generator
# --------------------------------------------------------------------


def generate_claim_row(claim_id: int, providers: list[Provider]) -> dict[str, str | None]:
    """
    Generate one raw claim row.
    
    We intentionally produce:
    - inconsistent date formatting
    - inconsistent money formatting
    - occasional missing values
    - occasional strange capitalization / whitespace 
    - occasional business-rule violations
    
    We'll clean this in the next pipeline step
    """

    # Basic identifiers
    member_id = f"M{random.randint(10000, 99999)}"

    # Pick a provider from the network, weighted by volume
    provider = random.choices(
        providers, weights=[p["volume_weight"] for p in providers], k=1
    )[0]
    provider_id = provider["provider_id"]

    # Choose a service date
    service_dt = random_service_datetime()
    service_date_str = messy_date_format(service_dt)

    # Procedure code from the catalog (weighted), plus its billed range
    cpt_code, _weight, billed_min, billed_max = random.choices(
        CPT_CATALOG, weights=[c[1] for c in CPT_CATALOG], k=1
    )[0]

    # diagnosis codes (ICD-10, simplified)
    dx_code = random.choice(["E11.9", "I10", "J45.909", "M54.5", "F41.9", "K21.9"])

    # Claim status: high-denial providers use a different weight table
    if provider["high_denial"]:
        status = random_choice_weighted(STATUS_WEIGHTS_HIGH_DENIAL)
    else:
        status = random_choice_weighted(STATUS_WEIGHTS_NORMAL)

    # Amounts
    # Billed depends on the procedure; billed >= allowed >= paid
    billed = round(random.uniform(billed_min, billed_max), 2)
    allowed = round(billed * random.uniform(0.50, 0.95), 2)
    paid = round(allowed * random.uniform(0.00, 1.00), 2)

    # Only PAID claims have money paid out:
    # - DENIED:   the payer refused the claim -> nothing paid
    # - PENDED:   still under review, not paid yet
    # - REVERSED: the payment was taken back -> net paid is zero
    # We still draw the random number above for every row and THEN override it.
    # Skipping the draw would shift every later random call, so all other
    # fields (states, dates, rule violations, ...) would change too.
    if status != "PAID":
        paid = 0.00

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

    # Inject a business-rule violation in a small share of rows
    if random.random() < RULE_VIOLATION_RATE:
        inject_rule_violation(row, allowed=allowed, paid=paid)

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

        # Write each row; DictWriter will convert None to "" automatically
        for r in rows:
            writer.writerow(r)


def main() -> None:
    """
    Orchestrate generation of raw dataset.
    """
    random.seed(RANDOM_SEED)

    # Build the provider network once, so every claim draws from the same pool
    providers = build_provider_pool(N_PROVIDERS)

    # Generate rows
    rows: list[dict[str, str | None]] = []
    for i in range(1, N_ROWS +1):
        rows.append(generate_claim_row(claim_id=i, providers=providers))

    # Write the CSV
    write_csv(rows, RAW_FILE)

    # Print a small success summary (for sanity checks)
    n_high_denial = sum(1 for p in providers if p["high_denial"])
    print(f"[OK] Wrote raw claims CSV: {RAW_FILE}")
    print(f"[OK] Rows: {len(rows):,}")
    print(f"[OK] Service window: {START_DATE:%Y-%m-%d} to {END_DATE:%Y-%m-%d}")
    print(f"[OK] Providers: {N_PROVIDERS} ({n_high_denial} high-denial)")
    print(f"[OK] CPT codes in catalog: {len(CPT_CATALOG)}")
    print(f"[OK] Example row: {rows[0]}")

if __name__ == "__main__":
    main()




