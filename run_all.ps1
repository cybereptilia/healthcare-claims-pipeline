# run_all.ps1
# Purpose:
# Execute the full healthcare claims pipeline from end to end.

# What this script does:
# 1)  Ensures the target PostgreSQL database exists
# 2)  Builds the schema (001)
# 3)  Creates indexes (002)
# 4)  Adds run logging, the rejects table, and data quality constraints (003)
# 5)  Generates raw synthetic claims data
# 6)  Cleans and validates that data
# 7)  Loads clean data + rejects into PostgreSQL and logs the run
# 8)  Creates analytics views (010)
# 9)  Creates dashboard views (012)
# 10) Runs quality checks
# 11) Runs report queries
# 12) Generates synthetic fraud labels      (ML stage, skip with -SkipML)
# 13) Builds model-ready features           (ML stage, skip with -SkipML)
# 14) Trains and evaluates the fraud models (ML stage, skip with -SkipML)

# How to run:
# .\run_all.ps1             # full pipeline, including the ML stage
# .\run_all.ps1 -SkipML     # data pipeline only (steps 1-11)

# Notes: 
# - This script assumes PostgreSQL is installed in the default path below.
# - It also assumes your Python env has the packages in requirements.txt
# - Step 4 must run after Step 2 every time: 001_schema.sql recreates
#   claims_clean, which removes the run_id column and constraints that 003 adds.

# Script parameters (must come before any other statement in the script).
param(
    # Use -SkipML to stop after the data pipeline (steps 1-11).
    [switch]$SkipML
)

# -------------------------------------------------------------------------------
# DATABASE PASSWORD SETUP
# -------------------------------------------------------------------------------
# psql automatically reads the PGPASSWORD environment variable.
# load_to_postgres.py reads DB_PASSWORD via os.getenv().
# Programs launched in this script inherit its environment variables

if (-not $env:PGPASSWORD) {
    # Prompt securely: -AsSecureString hides the typed characters !
    $securePassword = Read-Host -Prompt "Enter Postgres password for user 'postgres'" -AsSecureString

    # Convert the SecureString to plain text for psql and Python to use it.
    $env:PGPASSWORD = [System.Net.NetworkCredential]::new("", $securePassword).Password

}

$env:DB_PASSWORD = $env:PGPASSWORD


# Stop inmediately if any command fails within Powershell, 
# Assert-StepSucceeded takes care of the external programs.
$ErrorActionPreference = "Stop"

# --------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------

# Path to the PostgreSQL command-line  client.
$psql = "C:\Program Files\PostgreSQL\18\bin\psql.exe"

# Path to the PostgreSQL createdb utility.
$createdb = "C:\Program Files\PostgreSQL\18\bin\createdb.exe"

# Database connection settings.
$dbName = "healthcare_claims"
$dbUser = "postgres"

function Assert-StepSucceeded {
    param([string]$StepName)
    # $LASTEXITCODE holds the exits code of the last external program (python, psql).
    if ($LASTEXITCODE -ne 0) {
        throw "$StepName failed with exit code $LASTEXITCODE"
    }
}

$pipelineFailed = $false
try {
    # --------------------------------------------------------------------------------
    # Step 1: Helper - create database if it does not already
    # --------------------------------------------------------------------------------
    Write-Host ""
    Write-Host "=== STEP 1: Ensure database exists ==="

    $dbExists = & $psql -U $dbUser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$dbName';"
    if ($dbExists -eq "1") {
        Write-Host "[INFO] Database already exists: $dbName"
    }
    else {
        #if it does not exist, create it.
        & $createdb -U $dbUser $dbName

        # stop the pipeline if createdb failed (non-zero exit code)
        Assert-StepSucceeded "Step 1: Create database"

        Write-Host "[OK] Database created: $dbName"
    }
    # --------------------------------------------------------------------------------
    # Step 2: Build schema
    # --------------------------------------------------------------------------------
    Write-Host ""
    Write-Host "=== STEP 2: APPLY schema ==="

    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f .\sql\001_schema.sql
    Assert-StepSucceeded "Step 2: Apply schema"
    Write-Host "[OK] Schema applied."

    # ---------------------------------------------------------------------------------
    # Step 3: Create indexes
    # ---------------------------------------------------------------------------------

    Write-Host ""
    Write-Host "=== STEP 3: Apply indexes ==="

    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\002_indexes.sql"
    Assert-StepSucceeded "Step 3: Apply indexes"
    Write-Host "[OK] Indexes applied."

    # ---------------------------------------------------------------------------------
    # Step 4: Run logging, rejects table, and data quality constraints
    # ---------------------------------------------------------------------------------
    # load_to_postgres.py writes to pipeline_runs and claims_rejects and fills
    # claims_clean.run_id, so this must run before Step 7.
 
    Write-Host ""
    Write-Host "=== STEP 4: Apply run logging and quality constraints ==="
 
    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\003_portfolio_upgrade.sql"
    Assert-StepSucceeded "Step 4: Apply run logging and quality constraints"
    Write-Host "[OK] Run logging tables and constraints applied."
 
    # ---------------------------------------------------------------------------------
    # Step 5: Generate raw data
    # ---------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== STEP 5: Generate raw data ==="
 
    python -u .\python\generate_raw_data.py
    Assert-StepSucceeded "Step 5: Generate raw data"
    Write-Host "[OK] Raw data generation complete."
 
    # ----------------------------------------------------------------------------------
    # Step 6: Clean raw data and write clean + rejects CSVs
    # ----------------------------------------------------------------------------------
    Write-Host ""
    Write-Host "=== STEP 6: Clean and validate raw data ==="
 
    python -u .\python\clean_transform.py
    Assert-StepSucceeded "Step 6: Clean and validate raw data"
    Write-Host "[OK] Clean transform complete."
    # -----------------------------------------------------------------------------------
    # Step 7: Load clean data + rejects into PostgreSQL (and log the run)
    # -----------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== STEP 7: Load clean data and rejects into PostgreSQL ==="
 
    python -u .\python\load_to_postgres.py
    Assert-StepSucceeded "Step 7: Load clean data and rejects into PostgreSQL"
    Write-Host "[OK] Load step complete."
 
    # -----------------------------------------------------------------------------------
    # Step 8: Create analytics views
    # -----------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== STEP 8: Create analytics views ==="
 
    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\010_analytics_views.sql"
    Assert-StepSucceeded "Step 8: Create analytics views"
    Write-Host "[OK] Analytics views created."

    # -----------------------------------------------------------------------------------
    # Step 9: Create dashboard views
    # -----------------------------------------------------------------------------------
    # Must run AFTER the load (Step 7): v_latest_run and the reject views read
    # pipeline_runs and claims_rejects, which the loader fills.

    Write-Host ""
    Write-Host "=== STEP 9: Create dashboard views ==="

    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\012_dashboard_views.sql"
    Assert-StepSucceeded "Step 9: Create dashboard views"
    Write-Host "[OK] Dashboard views created."
 
    # -----------------------------------------------------------------------------------
    # Step 10: Run quality checks
    # -----------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== STEP 10: Run quality checks ==="
 
    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\011_quality_checks.sql"
    Assert-StepSucceeded "Step 10: Run quality checks"
    Write-Host "[OK] Quality checks executed."
    # ------------------------------------------------------------------------------------
    # Step 11: Run report queries
    # ------------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== STEP 11: Run report queries ==="
 
    & $psql -U $dbUser -d $dbName -v ON_ERROR_STOP=1 -f ".\sql\020_report_queries.sql"
    Assert-StepSucceeded "Step 11: Run report queries"
    Write-Host "[OK] Report queries executed."
 
    # ------------------------------------------------------------------------------------
    # ML stage (Steps 12-14): skipped when the script is run with -SkipML
    # ------------------------------------------------------------------------------------
    if ($SkipML) {
        Write-Host ""
        Write-Host "[INFO] -SkipML set: skipping fraud labels, features, and model training."
    }
    else {
        # --------------------------------------------------------------------------------
        # Step 12: Generate synthetic fraud labels
        # --------------------------------------------------------------------------------
        Write-Host ""
        Write-Host "=== STEP 12: Generate fraud labels ==="
 
        python -u .\ml\generate_fraud_labels.py
        Assert-StepSucceeded "Step 12: Generate fraud labels"
        Write-Host "[OK] Fraud labels generated."
 
        # --------------------------------------------------------------------------------
        # Step 13: Build model-ready features
        # --------------------------------------------------------------------------------
        Write-Host ""
        Write-Host "=== STEP 13: Build model features ==="
 
        python -u .\ml\feature_engineering.py
        Assert-StepSucceeded "Step 13: Build model features"
        Write-Host "[OK] Feature dataset written."
 
        # --------------------------------------------------------------------------------
        # Step 14: Train and evaluate fraud models (writes charts to ml\artifacts)
        # --------------------------------------------------------------------------------
        Write-Host ""
        Write-Host "=== STEP 14: Train and evaluate fraud models ==="
 
        python -u .\ml\train_fraud_model.py
        Assert-StepSucceeded "Step 14: Train and evaluate fraud models"
        Write-Host "[OK] Model training and evaluation complete."
    }
 
    # ------------------------------------------------------------------------------------
    # Final success message
    # ------------------------------------------------------------------------------------
 
    Write-Host ""
    Write-Host "=== PIPELINE COMPLETE ==="
    Write-Host "[OK] End-to-end healthcare claims pipeline executed successfully!"
    }
    catch {
        # Runs only if a step threw an error
        # $_ holds the error; .Exception.Message is the text passed to throw.
        Write-Host ""
        Write-Host "[FAIL] $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "[FAIL] Pipeline stopped. Later steps were not run." -ForegroundColor Red
        $pipelineFailed = $true
    }
# Returning a non-zero exit code so anything calling this script knows it failed.
if ($pipelineFailed) { exit 1 }
 