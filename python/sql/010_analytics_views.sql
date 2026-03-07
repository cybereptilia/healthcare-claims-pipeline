-- 010_analytics_views.sql
-- Purpose : Create views for common analytics questions.
-- Run: psql -U postgres -d healthcare_claims -f sql/010_analytics_views.sql

BEGIN;

-- Total paid by month
DROP VIEW IF EXISTS v_paid_by_month;
CREATE OR REPLACE VIEW v_paid_by_month AS
SELECT
    DATE_TRUNC('month', service_date) AS month,
    SUM(paid_amount) AS total_paid
FROM claims_clean
GROUP BY 1
ORDER BY 1;

-- Denial rate by provider
DROP VIEW IF EXISTS v_denial_rate_by_provider;
CREATE OR REPLACE VIEW v_denial_rate_by_provider AS
SELECT
    provider_id,
    COUNT(*) FILTER (WHERE status = 'DENIED')::numeric / NULLIF(COUNT(*), 0) AS denial_rate,
    COUNT(*) AS total_claims
FROM claims_clean
GROUP BY provider_id
ORDER BY denial_rate DESC NULLS LAST;

-- Avg paid amount by CPT code
DROP VIEW IF EXISTS v_avg_paid_by_cpt;
CREATE OR REPLACE VIEW v_avg_paid_by_cpt AS
SELECT
    cpt_code,
    AVG(paid_amount) AS avg_paid,
    COUNT(*) AS n_claims
FROM claims_clean
GROUP BY cpt_code
ORDER BY n_claims DESC;

COMMIT;