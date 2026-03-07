-- 002_indexes.sql
-- Purpose: Add indexes to support analytics queries.
-- Run: psql -U postgres -d healthcare_claims -f sql/002_indexes.sql

BEGIN;

-- Common filtering: by service date ranges
CREATE INDEX IF NOT EXISTS idx_claims_clean_service_date
ON claims_clean(service_date);

-- Common grouping/filtering: by provider
CREATE INDEX IF NOT EXISTS idx_claims_clean_provider
ON claims_clean(provider_id);

-- Common grouping/filtering: by member
CREATE INDEX IF NOT EXISTS idx_claims_clean_member
ON claims_clean(member_id);

-- Common filtering: claim status
CREATE INDEX IF NOT EXISTS idx_claims_clean_status
ON claims_clean(status);

-- Sometimes you'll filter by diagnosis or procedure code
CREATE INDEX IF NOT EXISTS idx_claims_clean_dx_code
ON claims_clean(dx_code);

COMMIT;