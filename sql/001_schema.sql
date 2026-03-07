-- 001_schema.sql
-- Purpose: Create the database schema (tables) for the healthcare claims pipeline.
-- Run: psql -U postgres -d healthcare_claims -f sql/001_schema.sql

BEGIN;
-- Drop tables if you want a clean re-run during development.
-- We comment these out later if we want to preserve data.
DROP VIEW IF EXISTS v_avg_paid_by_cpt_min10;
DROP VIEW IF EXISTS v_denial_rate_by_provider_min5;
DROP VIEW IF EXISTS v_avg_paid_by_cpt;
DROP VIEW IF EXISTS v_denial_rate_by_provider;
DROP VIEW IF EXISTS v_paid_by_month;

DROP TABLE IF EXISTS claims_raw;
DROP TABLE IF EXISTS claims_clean;

-- Raw claims table
-- This mirrors "messy" input data: strings, missing values, loose constraints.
CREATE TABLE claims_raw (
    claim_id         TEXT,
    member_id        TEXT,
    provider_id      TEXT,
    service_date     TEXT,  -- raw text date (ex: 02/14/2026)
    cpt_code         TEXT,
    dx_code          TEXT,
    status           TEXT,  -- ex: PAID, DENIED, PENDING
    billed_amount    TEXT,  -- raw text numeric
    allowed_amount   TEXT,  -- raw text numeric
    paid_amount      TEXT,  -- raw text numeric
    member_state     TEXT,  -- ex: PR, FL, etc.
    ingest_ts        TIMESTAMPTZ DEFAULT NOW()  -- when we loaded this row
);

-- Clean claims table
-- This is the typed, analytics-ready version.
CREATE TABLE claims_clean (
    claim_id          BIGINT PRIMARY KEY,
    member_id         TEXT NOT NULL,
    provider_id       TEXT NOT NULL,
    service_date      DATE NOT NULL,
    cpt_code          TEXT NOT NULL,
    dx_code           TEXT NOT NULL,
    status            TEXT NOT NULL,
    billed_amount     NUMERIC(12,2) NOT NULL,
    allowed_amount    NUMERIC(12,2) NOT NULL,
    paid_amount       NUMERIC(12,2) NOT NULL,
    member_state      TEXT NOT NULL,
    load_ts           TIMESTAMPTZ DEFAULT NOW()
);

COMMIT;