-- Wipe script for gpay-postgres (auth-only DB).
-- Drops cdc_auth_service schema entirely. Re-seeded by
-- cdc-auth-service/migrations/001_auth_users.sql.
-- WARNING: DESTRUCTIVE.

BEGIN;
DROP SCHEMA IF EXISTS cdc_auth_service CASCADE;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO PUBLIC;
COMMENT ON SCHEMA public IS
  'Phase 01 split E2E — auth DB; public empty by convention.';
COMMIT;
