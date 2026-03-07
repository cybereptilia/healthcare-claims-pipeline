-- 020_report_queries.sql
-- Purpose: Final report queries for the healthcare claims pipeline.
-- This file is meant to be run AFTER:
-- 1) raw data generation
-- 2) cleaning
-- 3) loading into PostgreSQL
-- 4) view creation in 010_analytics_views.sql

-- The why: It provides a summary of the pipeline results 
-- without requiring dev to remember or write ad hoc queries.

-- Run with: psql -U postgres -d healthcare_claims -f sql/020_report_queries.sql

-- ======================================================================================
-- Report 1: Dataset coverage
-- Shows:
-- - earliest service date
-- - latest service date
-- - total number of cleaned claims loaded into the warehouse table
-- Why:
-- - confirms the date span of the dataset
-- - helps explain partial-month effects in monthly totals
-- ======================================================================================
SELECT
    MIN(service_date) AS min_service_date,
    MAX(service_date) AS max_service_date,
    COUNT(*) AS total_clean_claims
FROM claims_clean;


-- ======================================================================================
-- Report 2: Claim status distribution
-- Shows:
-- - number of claims by status
-- - percentage of total claims by status
-- Why:
-- - gives a fast operational overview of the claims population
-- =======================================================================================
SELECT
    status,
    COUNT(*) AS n_claims,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM claims_clean
GROUP BY status
ORDER BY n_claims DESC;


-- =======================================================================================
-- Report 3: Paid amount by month
-- Shows:
-- - total paid dollars by service month
-- Why:
-- - reveals seasonality, consistency, or partial-period effects
-- Note:
-- - This uses the view already created in 010_analytics_views.sql
-- =======================================================================================
SELECT 
    month,
    total_paid
FROM v_paid_by_month
ORDER BY month;


-- =======================================================================================
-- Report 4: Providers with highest denial rates
-- Business rule: 
-- - only include providers with at least 5 claims
-- Why:
-- - avoids misleading results from providers with only 1 claim
-- =======================================================================================
SELECT 
    provider_id,
    total_claims,
    denied_claims,
    ROUND(100.0 * denial_rate, 2) AS denial_rate_pct
FROM v_denial_rate_by_provider
WHERE total_claims >= 5
ORDER BY denial_rate DESC, total_claims DESC
LIMIT 10;


-- =======================================================================================
-- Report 5: Highest-volume providers and their denial rates
-- Shows:
-- - providers with the most claims
-- - denial rates for those higher-volume providers
-- Why:
-- - higher-volume entities are often more analytically meaningful
-- =======================================================================================
SELECT
    provider_id,
    total_claims,
    denied_claims,
    ROUND(100.0 * denial_rate, 2) AS denial_rate_pct
FROM v_denial_rate_by_provider
WHERE total_claims >= 5
ORDER BY total_claims DESC, denial_rate DESC
LIMIT 10;