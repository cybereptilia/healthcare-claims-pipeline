-- 012_dashboard_views.sql
-- Purpose: Views that feed the Streamlit dashboard (app/dashboard.py).
-- Run AFTER load_to_postgres.py and 010_analytics_views.sql:
--  psql -U postgres -d healthcare_claims -f sql/012_dashboard_views.sql
--
-- Views in this file:
--  1) v_claims_base                one row per clean claim + helper columns (filters apply here)
--  2) v_latest_run                 the most recent SUCCESS pipeline run, as one row
--  3) v_rejects_by_reason_latest    reject counts by reason, latest run only
--  4) v_missing_fields_latest      which required field was missing, latest run only
--
-- Important:
--  - v_claims_base depends on claims_clean. 001_schema.sql drops claims_clean,
--  so 001 must drop v_claims_base first (DROP VIEW IF EXISTS v_claims_base;).
--  - Views 2-4 read pipeline_runs and claims_rejects, which 001 never drops.
--  - claims_rejects keeps the rejects of EVERY run (history). Views 3 and 4
--  filter to the latest SUCCESS run so counts do not grow with each re-run.

BEGIN;

-- -----------------------------------------------------------------------------------------------
-- Drop old versions first.
-- Order matters: views 3 and 4 depend on v_latest_run, so they are dropped first.
-- -----------------------------------------------------------------------------------------------
DROP VIEW IF EXISTS v_missing_fields_latest;
DROP VIEW IF EXISTS v_rejects_by_reason_latest;
DROP VIEW IF EXISTS v_latest_run;
DROP VIEW IF EXISTS v_claims_base;

-- ------------------------------------------------------------------------------------------------
-- View 1: v_claims_base
-- One row per clean claim, plus helper columns the dashboard needs
-- Why: the sidebar filters (date range, member_state, status) are applied to this
-- single view, so every chart is computed from the same filtered set of claims.
-- Columns are listed explicitly (no SELECT *) so a schema change is a visible,
-- deliberate edit here instead of a silent change in the dashboard.
-- ------------------------------------------------------------------------------------------------
CREATE VIEW v_claims_base AS 
SELECT
    claim_id,
    member_id,
    provider_id,
    service_date,
    -- First day of the service month, as DATE (same convention as v_paid_by_month)
    DATE_TRUNC('month', service_date)::date AS service_month,
    cpt_code,
    dx_code,
    status,
    -- 1 = denied, 0 = any other status.
    -- SUM(is_denied) = denied claims; AVG(is_denied) = denial rate.
    CASE WHEN status = 'DENIED' THEN 1 ELSE 0 END AS is_denied,
    billed_amount,
    allowed_amount,
    paid_amount,
    member_state,
    run_id
FROM claims_clean;

-- ------------------------------------------------------------------------------------------------
-- View 2: v_latest_run
-- The most recent successful pipeline run, as a single row.
-- Why: the dashboard's Data Quality tab shows the run counts and reject rate of
-- the data currently loaded, and views 3-4 use this run_id as their filter.
-- FAILED runs are excluded: their data was rolled back, so it is not what is loaded.
-- ------------------------------------------------------------------------------------------------
CREATE VIEW v_latest_run AS
SELECT
    run_id,
    started_at,
    finished_at,
    raw_rows,
    clean_rows,
    rejected_rows,
    -- Share of raw rows that were rejected. NULLIF avoids division by zero.
    ROUND(100.0 * rejected_rows / NULLIF(raw_rows, 0), 2) AS reject_rate_pct
FROM pipeline_runs
WHERE status = 'SUCCESS'
ORDER BY finished_at DESC
LIMIT 1;


-- ------------------------------------------------------------------------------------------------
-- View 3: v_rejects_by_reason_latest
-- Reject counts by reason category for the latest successful run.
-- reject_reason looks like "missing_required_fields:member_id,status" or
-- "invalid_status:PAYED"; the category is the text before the first ':'.
-- ------------------------------------------------------------------------------------------------
CREATE VIEW v_rejects_by_reason_latest AS
SELECT
    SPLIT_PART(r.reject_reason, ':', 1) AS reason_category,
    COUNT(*) AS n_rejects,
    -- Share of this run's rejects. SUM(COUNT(*)) OVER () = total of rejects in the run.
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_rejects
FROM claims_rejects AS r
JOIN v_latest_run AS l
    ON r.run_id = l.run_id
GROUP BY 1
ORDER BY n_rejects DESC;

-- ------------------------------------------------------------------------------------------------
-- View 4: v_missing_fields_latest
-- For "missing_required_fields" rejects in the latest run: how often each field 
-- was missing.
-- How: take the comma-separated list after the ':' and turn it into one row per
-- field (STRING_TO_ARRAY + UNNEST), then count per field.
-- Note: one reject can be missing up to 3 fields, so the counts add up to MORE
-- than the number of missing-field rejects. pct_of_missing_rejects answers
-- "in what % of missing-field rejects was this field blank?"
-- -------------------------------------------------------------------------------------------------
CREATE VIEW v_missing_fields_latest AS
WITH missing_rejects AS (
    -- One row per missing-field reject in the latest run, with its field list
    SELECT
        r.reject_id,
        SPLIT_PART(r.reject_reason, ':', 2) AS field_list
        FROM claims_rejects AS r
        JOIN v_latest_run AS l
            ON r.run_id = l.run_id
        WHERE r.reject_reason LIKE 'missing_required_fields:%'
)
SELECT
    f.field_name,
    COUNT(*) AS n_rejects,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM missing_rejects), 2) AS pct_of_missing_rejects
FROM missing_rejects AS m
-- CROSS JOIN LATERAL runs UNNEST once per reject row: "a,b" -> two rows (a), (b)
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(m.field_list, ',')) AS f(field_name)
GROUP BY f.field_name
ORDER BY n_rejects DESC, f.field_name;

COMMIT;