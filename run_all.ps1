# run_all.ps1
# Purpose:
# Execute the full healthcare claims pipeline from end to end.

# What this script does:
# 1) Ensures the target PostgreSQL database exists
# 2) Builds the schema 
# 3) Creates indexes
# 4) Generates raw synthetic claims data
# 5) Cleans and validates that data 
# 6) Loads the clean CSV into PostgreSQL
# 7) Creates analytics views
# 8) Runs quality checks
# 9) Runs report queries 

# How to run:
# .\run_all.ps1

# Notes: 
# - This script assumes PostgreSQL is installed in the default path below.
# - It also assumes your Python env can import psycopg2.

# Stop inmediately if any command fails.
# This prevents the pipeline from silently continuing in a broken state.
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

# --------------------------------------------------------------------------------
# Helper: create database if it does not already
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

    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Database created: $dbName"
    }
    else {
        Write-Host "[ERROR] Failed to create database: $dbName"
        exit 1
    }
}
# --------------------------------------------------------------------------------
# Step 2: Build schema
# --------------------------------------------------------------------------------
Write-Host ""
Write-Host "=== STEP 2: APPLY schema ==="

& $psql -U $dbUser -d $dbName -f .\sql\001_schema.sql
Write-Host "[OK] Schema applied."

# ---------------------------------------------------------------------------------
# Step 3: Create indexes
# ---------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 3: Apply indexes ==="

& $psql -U $dbUser -d $dbName -f ".\sql\002_indexes.sql"
Write-Host "[OK] Indexes applied."

# ---------------------------------------------------------------------------------
# Step 4: Generate raw data
# ---------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 4: Generate raw data ==="

python -u .\python\generate_raw_data.py
Write-Host "[OK] Raw data generation complete."

# ----------------------------------------------------------------------------------
# Step 5: Clean raw data and write clean + rejects CSVs
# ----------------------------------------------------------------------------------
Write-Host ""
Write-Host "=== STEP 5: Clean and validate raw data ==="

python -u .\python\clean_transform.py
Write-Host "[OK] Clean transform complete."
# -----------------------------------------------------------------------------------
# Step 6: Load clean data into PostgreSQL
# -----------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 6: Load clean CSV into PostgreSQL ==="

python -u .\python\load_to_postgres.py

Write-Host "[OK] Load step complete."

# -----------------------------------------------------------------------------------
# Step 7: Create analytics views
# -----------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 7: Create analytics views ==="

& $psql -U $dbUser -d $dbName -f ".\sql\010_analytics_views.sql"

Write-Host "[OK] Analytics views created."

# -----------------------------------------------------------------------------------
# Step 8: Run quality checks
# -----------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 8: Run quality checks ==="

& $psql -U $dbUser -d $dbName -f ".\sql\011_quality_checks.sql"

Write-Host "[OK] Quality checks executed."
# ------------------------------------------------------------------------------------
# Step 9: Run report queries
# ------------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== STEP 9: Run report queries ==="

& $psql -U $dbUser -d $dbName -f ".\sql\020_report_queries.sql"

Write-Host "[OK] Report queries executed."
# ------------------------------------------------------------------------------------
# Final success message
# ------------------------------------------------------------------------------------

Write-Host ""
Write-Host "=== PIPELINE COMPLETE ==="
Write-Host "[OK] End-to-end healthcare claims pipeline executed successfully!."