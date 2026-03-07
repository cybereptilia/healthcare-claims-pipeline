Healthcare Claims Data Pipeline

An end-to-end data pipeline that generates synthetic healthcare claims data, cleans and validates it, loads it into PostgreSQL, and produces analytics-ready reports.
The project demonstrates core data engineering concepts including data cleaning, validation, database loading, indexing, analytics views, and automated pipeline execution.

Tech Stack:
- Python
- PostgreSQL
- SQL
- PowerShell

Libraries:
- psycopg2
- csv
- pathlib

Pipeline Overview
The pipeline runs through the following stages:
1. Generate synthetic raw healthcare claims data
2. Clean and validate the data 
3. Store rejected records separately
4. Load clean data into PostgreSQL
5. Create indexed analytics views
6. Run quality checks
7. Produce summary reports
- All steps run with one command

Running the Pipeline
.\run_all.ps1

This script:
- Ensures the database exists
- Builds schema
- Generates raw claims data
- Cleans and validates records
- Loads clean data into PostgreSQL
- Creates analytics views 
- Runs quality checks and reports

Example output:

Raw rows: 20,000
Clean rows: 17,704
Rejected rows: 2,296

Example Insights
Status distribution:

PAID - 77.8%
DENIED - 12.1%
PENDED - 5.8%
REVERSED - 4.3%
- Monthly claim payments range roughly between $600k-$700k, with lower totals in partial months.
- Provider denial rates are calculated with minimum claim thersholds to avoid misleading results from low sample sizes.

Project Structure

python/
  generate_raw_data.py
  clean_transform.py
  load_to_postgres.py

sql/
  001_schema.sql
  002_indexes.sql
  010_analytics_views.sql
  011_quality_checks.sql
  020_report_queries.sql

run_all.ps1

Concepts Demonstrated
- ETL pipeline design
- Data validation and rejection handling
- PostgreSQL schema and indexing
- SQL analytics views
- Automated pipeline execution

Author 
Rosaliz Garcia
Computer Science - Data Science Track

