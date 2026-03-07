-- 002_portfolio_upgrade.sql
-- Adds rejects table, run logging table, and first-class data quality constraints.

BEGIN;

-- We use UUIDs for run identifiers.
-- pgcrypto provides gen_random_uuid().

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -------------------------------------------------------------------------
-- 1) Pipeline run logging
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ NULL,

    raw_rows        INTEGER NULL,
    clean_rows      INTEGER NULL,
    rejected_rows   INTEGER NULL,

    status          TEXT NOT NULL DEFAULT 'STARTED', -- STARTED | SUCCESS | FAILED
    error_message   TEXT NULL
);

-- -------------------------------------------------------------------------
-- 2) Rejects table
-- We store: claim_id (if present), reject_reason, raw row payload, and run_id.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS claims_rejects (
    reject_id      BIGSERIAL PRIMARY KEY,
    run_id         UUID NOT NULL REFERENCES pipeline_runs(run_id),
    claim_id       TEXT NULL,
    reject_reason  TEXT NOT NULL,
    raw_payload    JSONB NOT NULL,
    rejected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful index for filtering rejects by run
CREATE INDEX IF NOT EXISTS idx_claims_rejects_run_id ON claims_rejects(run_id);
CREATE INDEX IF NOT EXISTS idx_claims_rejects_reason ON claims_rejects(reject_reason);

-- ---------------------------------------------------------------------------
-- 3) Add run_id to claims_clean so every row is traceable to a run
-- ---------------------------------------------------------------------------
ALTER TABLE claims_clean
ADD COLUMN IF NOT EXISTS run_id UUID;

-- If you want: enforce that future loads always include run_id
-- (We will set it in the loader.)
-- For now we won't make it NOT NULL to avoid breaking existing rows.

CREATE INDEX IF NOT EXISTS idx_claims_clean_run_id ON claims_clean(run_id);

-- ----------------------------------------------------------------------------
-- 4) First-class data quality constraints
-- These are the "truth rules" the DB enforces.
-- ----------------------------------------------------------------------------

-- Status must be one of these:
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_claims_clean_status'
    ) THEN
        ALTER TABLE claims_clean
        ADD CONSTRAINT chk_claims_clean_status
        CHECK (status IN ('PAID', 'DENIED', 'PENDED', 'REVERSED'));
    END IF;
END $$;

-- Member_state must be 2 uppercase letters (PR, FL, etc.)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_claims_clean_member_state'
    ) THEN
        ALTER TABLE claims_clean
        ADD CONSTRAINT chk_claims_clean_member_state
        CHECK (member_state ~ '^[A-Z]{2}$');
    END IF;
END $$;

-- Amounts must be non-negative and consistent
DO $$
BEGIN 
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_claims_clean_amounts_nonneg'
    ) THEN
        ALTER TABLE claims_clean
        ADD CONSTRAINT chk_claims_clean_amounts_nonneg
        CHECK (billed_amount >= 0 AND allowed_amount >= 0 AND paid_amount >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_claims_clean_amounts_order'
    ) THEN
        ALTER TABLE claims_clean
        ADD CONSTRAINT chk_claims_clean_amounts_order
        CHECK (paid_amount <= allowed_amount AND allowed_amount <= billed_amount);
    END IF;
END $$;
COMMIT;

