"""
generate_fraud_labels.py

Purpose:
- Create synthetic fraud labels for the clean healthcare claims dataset.

Why this exists:
- Our claims pipeline currently ends at a clean analytical dataset.
To extend the project into machine learning, we need a target label 
that tells the model which claims are "fraud" and which are not.

Approach:
- Because this is synthetic data, we simulate fraud using interpretable rules:
    1) unusually high billing ratio
    2) unusually high provider claim volume
    3) unusually high paid amount
    4) a small random fraud component to avoid perfectly deterministic labels

Input: data/clean/claims_clean.csv
Output: data/clean/claims_labeled.csv

New columns added:
    billing_ratio
    provider_claim_count
    fraud_score
    is_fraud
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------

# Input file: clean claims produced by clean_transform.py
INPUT_PATH: Path = Path("data") / "clean" / "claims_clean.csv"

# Output file: clean claims + fraud labels
OUTPUT_PATH: Path = Path("data") / "clean" / "claims_labeled.csv"

# Random seed for reproducibility.
# This ensures that the tiny random component behaves the same each run.
RANDOM_SEED: int = 42

# Percentile thresholds used to define "unusually high" values.
# We use percentiles instead of hardcoded numeric thresholds because they adapt
# to the distribution of the current dataset.
BILLING_RATIO_PERCENTILE: float = 0.95
PROVIDER_VOLUME_PERCENTILE: float = 0.95
PAID_AMOUNT_PERCENTILE: float = 0.95

# Additional small random fraud rate.
# This is applied only to rows that are not already flagged as fraud.
# It keeps labels from being perfectly deterministic.
RANDOM_FRAUD_RATE: float = 0.01

# Fraud score threshold.
# A row with score >= this value will be labeled as fraud.
FRAUD_SCORE_THRESHOLD: int = 2

# Columns we must have available in the clean dataset.
REQUIRED_COLUMNS: List[str] = [
    "claim_id",
    "provider_id",
    "member_id",
    "service_date",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
]

# --------------------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------------------
def validate_columns(df: pd.DataFrame, required_columns: List[str]) -> None:
    """
    Ensure that the DataFrame contains all required columns.
    Why:
        If a required column is missing, feature generation will fail later
        in a confusing way. It is better to fail early with a clear error.
    """
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(
            f"Missing required columns in input dataset: {missing}"
        )

def safe_billing_ratio(df: pd.DataFrame) -> pd.Series:
    """
    Compute billed_amount / allowed_amount safely.
    
    Why this needs care:
    Division by zero would create infinite values or errors.
    In a real fraud workflow, claims with zero allowed amount
    could be treated separately. For this first version, we mark
    them as highly suspicious by assigning a very large ratio.
    
    Returns:
    A pandas series of billoing ratios.
    """

    # Create a copy of allowed_amount so we can safely replace zeros.
    allowed = df["allowed_amount"].copy()

    # Replace zeros with NaN temporarily to avoid division-by-zero explosions.
    allowed = allowed.replace(0, np.nan)

    # Compute the ratio. Claims with allowed_amount = 0 become NaN here.
    ratio = df["billed_amount"] / allowed

    # Replace NaN/infinite values with a very high suspicious value.
    # This makes zero-allowed claims naturally appear in the extreme tail.
    ratio = ratio.replace([np.inf, -np.inf], np.nan).fillna(999.0)

    return ratio

# --------------------------------------------------------------------------------------------
# Main labeling logic
# --------------------------------------------------------------------------------------------
def main () -> None:
    """ Generate fraud labels for the clean claims dataset.
    """
    # Seed numpy's random generator for reproducibility.
    np.random.seed(RANDOM_SEED)

    print(f"[Fraud Labels] Reading clean claims from: {INPUT_PATH}")

    # Load the clean claims dataset.
    # We assume clean_transform.py already standardized the data enough
    # for billed_amount / allowed_amount / paid_amount to be numeric.
    df = pd.read_csv(INPUT_PATH)

    print(f"[Fraud Labels] Rows loaded: {len(df):,}")

    # Validate that required columns are present.
    validate_columns(df, REQUIRED_COLUMNS)

    # ----------------------------------------------------------------------------------------
    # Feature 1: Billing ratio
    # ----------------------------------------------------------------------------------------
    # High billed_amount relative to allowed_amount may signal suspicious billing.
    df["billing_ratio"] = safe_billing_ratio(df)

    # ----------------------------------------------------------------------------------------
    # Feature 2: Provider claim volume
    #-----------------------------------------------------------------------------------------
    # Count how many claims each provider appears in.
    # This creates a provider-level behavior signal.
    provider_counts = df["provider_id"].value_counts()

    # Map the provider counts back onto each row.
    df["provider_claim_count"] = df["provider_id"].map(provider_counts)

    # ----------------------------------------------------------------------------------------
    # Threshold calculation using percentiles
    # ----------------------------------------------------------------------------------------
    billing_ratio_threshold = df["billing_ratio"].quantile(BILLING_RATIO_PERCENTILE)
    provider_volume_threshold = df["provider_claim_count"].quantile(PROVIDER_VOLUME_PERCENTILE)
    paid_amount_threshold = df["paid_amount"].quantile(PAID_AMOUNT_PERCENTILE)

    print(f"[Fraud Labels] Billing ratio threshold ({int(BILLING_RATIO_PERCENTILE * 100)}th percentile): {billing_ratio_threshold:.4f}")
    print(f"[Fraud Labels] Provider volume threshold ({int(PROVIDER_VOLUME_PERCENTILE * 100)}th percentile): {provider_volume_threshold:.0f}")
    print(f"[Fraud Labels] Paid amount threshold ({int(PAID_AMOUNT_PERCENTILE * 100)}th percentile): {paid_amount_threshold: .2f}")

    # ----------------------------------------------------------------------------------------
    # Suspicious flags
    # ----------------------------------------------------------------------------------------
    # Each flag is 1 if suspicious, 0 otherwise.
    df["flag_high_billing_ratio"] = (df["billing_ratio"] >= billing_ratio_threshold).astype(int)
    df["flag_high_provider_volume"] = (df["provider_claim_count"] >= provider_volume_threshold).astype(int)
    df["flag_high_paid_amount"] = (df["paid_amount"] >= paid_amount_threshold).astype(int)

    # ----------------------------------------------------------------------------------------
    # Fraud score
    # ----------------------------------------------------------------------------------------
    # Sum the suspicious indicators.
    # A higher score means the claim triggers more suspicious patterns.
    df["fraud_score"] = (
        df["flag_high_billing_ratio"]
        + df["flag_high_provider_volume"]
        + df["flag_high_paid_amount"]
    )

    # ----------------------------------------------------------------------------------------
    # Initial fraud label from rules
    # ----------------------------------------------------------------------------------------
    # Claims with enough suspish signals are labeled fraud.
    df["is_fraud"] = (df["fraud_score"] >= FRAUD_SCORE_THRESHOLD).astype(int)

    # ----------------------------------------------------------------------------------------
    # Small random fraud component 
    # ----------------------------------------------------------------------------------------
    # To avoid a perfectly deterministic rule system, randomly flip a tiny 
    # fraction of non-fraud rows to fraud.
    # Note: We only apply this to rows currently labeled 0.
    non_fraud_mask = df["is_fraud"] == 0

    # Random values in [0, 1) for every row.
    random_values = np.random.rand(len(df))

    # If a non-fraud row falls below the random fraud rate, flip it.
    random_fraud_mask = non_fraud_mask & (random_values < RANDOM_FRAUD_RATE)

    df.loc[random_fraud_mask, "is_fraud"] = 1

    # -----------------------------------------------------------------------------------------
    # Summary metrics
    # -----------------------------------------------------------------------------------------
    total_rows = len(df)
    fraud_rows = int(df["is_fraud"].sum())
    fraud_rate = (fraud_rows / total_rows) * 100 if total_rows > 0 else 0.0

    print(f"[Fraud Labels] Fraud rows: {fraud_rows:,}")
    print(f"[Fraud Labels] Fraud rate: {fraud_rate: .2f}%")

    # -----------------------------------------------------------------------------------------
    # Save output
    # -----------------------------------------------------------------------------------------
    # Create output directory if it does not exist.
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the labeled dataset to CSV
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"[Fraud Labels] Wrote labeled dataset to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()


