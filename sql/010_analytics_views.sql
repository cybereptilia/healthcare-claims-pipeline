-- 010_analytics_views.sql
-- Purpose : Create views for common analytics questions.
-- Run: psql -U postgres -d healthcare_claims -f sql/010_analytics_views.sql

BEGIN;

-- -------------------------------------------------------------------------------
-- Drop old views first
-- -------------------------------------------------------------------------------
DROP VIEW IF EXISTS v_avg_paid_by_cpt_min50;
DROP VIEW IF EXISTS v_denial_rate_by_provider_min30;

DROP VIEW IF EXISTS v_avg_paid_by_cpt CASCADE;
DROP VIEW IF EXISTS v_denial_rate_by_provider CASCADE;
DROP VIEW IF EXISTS v_paid_by_month CASCADE;

----------------------------------------------------------------------------------
-- View 1: Total paid by month (clean formatting)
-- Why: DATE_TRUNC returns a timestamp; we cast to DATE so output is prettier
----------------------------------------------------------------------------------
CREATE VIEW v_paid_by_month AS
SELECT
    DATE_TRUNC('month', service_date)::date AS month,
    SUM(paid_amount) AS total_paid
FROM claims_clean
GROUP BY 1
ORDER BY 1;

-- -------------------------------------------------------------------------------
-- Denial rate by provider (no minimum sample size)
-- -------------------------------------------------------------------------------
CREATE VIEW v_denial_rate_by_provider AS
SELECT
    provider_id,
    COUNT(*) AS total_claims,
-- Cast numerator to numeric so we get decimals.
-- NULLIF avoids division by zero
    COUNT(*) FILTER (WHERE status = 'DENIED') AS denied_claims,
    COUNT(*) FILTER (WHERE status = 'DENIED')::numeric / NULLIF(COUNT(*), 0) AS denial_rate  
FROM claims_clean
GROUP BY provider_id
ORDER BY denial_rate DESC NULLS LAST, total_claims DESC;

-- -------------------------------------------------------------------------------
-- View 2b (filtered): Denial rate by provider with a minimum sample size
-- Tweak the threshold depending on your dataset size (30 is a decent default in this case).
-- -------------------------------------------------------------------------------
CREATE VIEW v_denial_rate_by_provider_min5 AS
SELECT
    provider_id,
    total_claims,
    denied_claims,
    denial_rate
FROM v_denial_rate_by_provider
WHERE total_claims >= 5
ORDER BY denial_rate DESC, total_claims DESC;
-- -------------------------------------------------------------------------------
-- View 3 (Raw): Average paid amount by CPT code (no minimum sample size)
-- -------------------------------------------------------------------------------
CREATE VIEW v_avg_paid_by_cpt AS
SELECT
    cpt_code,
    COUNT(*) AS n_claims,
    AVG(paid_amount) AS avg_paid
FROM claims_clean
GROUP BY cpt_code
ORDER BY n_claims DESC, avg_paid DESC;

-- --------------------------------------------------------------------------------
-- View 3b (Filtered): Avg paid by CPT with a minimum sample size 
-- Why: this prevents averages based on tiny samples (like n_claims = 4).
-- Tweak threshold depending on dataset size (50 is a proficient default in this case).
-- --------------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_avg_paid_by_cpt_min10 AS
SELECT 
    cpt_code,
    n_claims,
    avg_paid
FROM v_avg_paid_by_cpt
WHERE n_claims >= 10
ORDER BY avg_paid DESC, n_claims DESC;

COMMIT;