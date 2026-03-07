-- 011_quality_checks.sql
-- Purpose: Data quality checks (run after load)
-- Run: psql -U postgres -d healthcare_claims -f sql/011_quality_checks.sql

-- 1) Row count
SELECT COUNT(*) AS clean_row_count FROM claims_clean;

-- 2) Primary key duplicates check (should be 0)
SELECT COUNT(*) AS duplicate_claim_ids
FROM (
    SELECT claim_id
    FROM claims_clean
    GROUP BY claim_id
    HAVING COUNT(*) > 1
) d;

-- 3) Null checks on required columns (should be 0 rows returned)
SELECT *
FROM claims_clean
WHERE
    claim_id IS NULL
 OR member_id IS NULL
 OR provider_id IS NULL
 OR service_date IS NULL
 OR billed_amount IS NULL
 OR allowed_amount IS NULL
 OR paid_amount IS NULL
LIMIT 25;

-- 4) Money sanity: paid <= allowed <= billed (real-world expectation)
-- This may not always be true in edge cases, but should mostly be true.
SELECT COUNT(*) AS violations
FROM claims_clean
WHERE NOT (paid_amount <= allowed_amount AND allowed_amount <= billed_amount);

-- 5) State code sanity (2 letters)
SELECT COUNT(*) AS bad_state_codes
FROM claims_clean
WHERE member_state !~ '^[A-Z]{2}$';