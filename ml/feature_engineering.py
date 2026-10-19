from __future__ import annotations

"""
feature_engineering.py

Purpose:
Create model-ready features from the fraud-labeled healthcare claims dataset.

The why of this files existence:
In a clean ML workflow, feature engineering should be its own step.

That gives us the following pipeline:

claims_clean.csv
    -> generate_fraud_labels.py
    -> claims_labeled.csv
    -> feature_engineering.py
    -> claims_features.csv
    -> train_fraud_model.py
    
What this script does:
1. Reads the labeled dataset.
2. Creates date-derived features.
3. Creates ratio-based numeric features.
4. Drops raw identifier columns that we do not want to model directly.
5. Wtites a model-ready dataset to disk.

How to run (from repo root):
    python -u ./ml/feature_engineering.py
"""
# --------------------------------------------------------------------------
# Standard library imports
# --------------------------------------------------------------------------
from pathlib import Path
# --------------------------------------------------------------------------
# Third-party imports
# --------------------------------------------------------------------------
import numpy as np
import pandas as pd
# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

# Input: output from generate_fraud_labels.py
INPUT_PATH: Path = Path("data") / "clean" / "claims_labeled.csv"

# Output: model-ready dataset for training 
OUTPUT_PATH: Path = Path("data") / "clean" / "claims_features.csv"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """
    Compute a ratio safely.
    Why this helper exists:
    Raw division can create:
    - ing
    - -inf
    - NaN
    
    We do not want those values contaminating our ML dataset.
    
    So we:
    1. divide
    2. replace
    3. fill NaN with 0.0
    
    Parameters
    ----------
    numerator : pd.Series
        Numerator values.
    denominator : pd.Series
        Denominator values.
        
    Returns
    ----------
    pd.Series
        A clean numeric ratio column.
    """
    ratio = numerator / denominator
    ratio = ratio.replace([np.inf, -np.inf], np.nan)
    ratio = ratio.fillna(0.0)
    return ratio

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Run the full feature engineering step.
    """
    print(f"[Features] Reading labeled claims from: {INPUT_PATH}")

    # ------------------------------------------------------------------------
    # Step 1: Ensure the labeled dataset exists
    # ------------------------------------------------------------------------
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Labeled dataset not found at: {INPUT_PATH}\n"
            f"Run generate_fraud_labels.py first."
        )
    # ------------------------------------------------------------------------
    # Step 2: Load the labeled dataset
    # ------------------------------------------------------------------------
    df = pd.read_csv(INPUT_PATH)

    print(f"[Features] Rows loaded: {len(df):,}")
    print(f"[Features] Columns loaded: {len(df.columns)}")

    # ------------------------------------------------------------------------
    # Step 3: Parse service_date as datetime
    # ------------------------------------------------------------------------
    # We convert service_date into a true datetime object so we can
    # derive calendar-based features from it.
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")

    # ------------------------------------------------------------------------
    # Step 4: Create date-derived features
    # ------------------------------------------------------------------------
    # These features may help the model capture temporal patterns.
    df["service_year"] = df["service_date"].dt.year
    df["service_month"] = df["service_date"].dt.month
    df["service_day"] = df["service_date"].dt.day
    df["service_dayofweek"] = df["service_date"].dt.dayofweek

    # ------------------------------------------------------------------------
    # Step 5: Create ratio-based numeric features
    # ------------------------------------------------------------------------
    # These are useful in fraud problems given suspicious billing often appears
    # in unusual ratios.
    df["billing_ratio"] = safe_ratio(df["billed_amount"], df["allowed_amount"])
    df["paid_ratio"] = safe_ratio(df["paid_amount"], df["allowed_amount"])
    df["paid_to_billed"] = safe_ratio(df["paid_amount"], df["billed_amount"])

    # -------------------------------------------------------------------------
    # Step 6: Drop raw identifiers we do not want to model directly
    # -------------------------------------------------------------------------
    # Why: 
    # - claim_id is just a unique record identifier
    # - member_id/provider_id can encourage memorization instead of learning.
    columns_to_drop = [
        "claim_id",
        "member_id",
        "provider_id",
        "service_date",  # raw date is replaced by derived date features
    ]

    # Drop only the columns that actually exist, just to be safe.
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    df = df.drop(columns=existing_columns_to_drop)

    # --------------------------------------------------------------------------
    # Step 7: Fill any remaining missing values in engineered numeric columns
    # --------------------------------------------------------------------------
    numeric_fill_cols = [
        "service_year",
        "service_month",
        "service_day",
        "service_dayofweek",
        "billing_ratio",
        "paid_ratio",
        "paid_to_billed",
    ]

    for col in numeric_fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # ---------------------------------------------------------------------------
    # Print a quick summary of the engineered dataset
    # ---------------------------------------------------------------------------
    print("[Features] Engineered feature columns added:")
    print("  - service_year")
    print("  - service_month")
    print("  - service_day")
    print("  - service_dayofweek")
    print("  - billing_ratio")
    print("  - paid_ratio")
    print("  - paid_to_billed")
    print ()

    if "is_fraud" in df.columns:
        fraud_count = int(df["is_fraud"].sum())
        fraud_rate = (fraud_count / len(df)) * 100 if len(df) > 0 else 0.0
        print(f"[Features] Fraud rows: {fraud_count:,}")
        print(f"[Features] Fraud rate: {fraud_rate: .2f}%")
        print()

    print(f"[Features] Final dataset columns: {len(df.columns)}")
    print(f"[Features] Final dataset rows: {len(df):,}")

    # ---------------------------------------------------------------------------
    # Step 9: Ensure the output folder exists, then save
    # ---------------------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"[Features] Wrote model-ready dataset to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
