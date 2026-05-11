-- ============================================================
-- 001_dest_init — Phase 01 Split E2E (T-B4)
-- Destination DB foundation for gpay-postgres-dest.
--
-- Phase 39 invariant: schemas only in
--   - public  : kept empty by convention
--   - master  : grouping for master physical tables (optional)
--   - dw_<binding>: per-binding DW schemas (created on-demand by
--                   master DDL handler when Wizard registers binding)
-- ============================================================

BEGIN;

-- 1. Required extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2. Reserved schema for grouped master tables (optional usage).
--    Master DDL handler may create tables here OR in dw_<binding>
--    depending on Wizard config.
CREATE SCHEMA IF NOT EXISTS master;
COMMENT ON SCHEMA master IS
  'Phase 01 split E2E — grouping schema for master physical tables (optional).';

-- 3. Default search_path: public first (so apps connecting to dest
--    don''t accidentally write to master). Apps must qualify all writes.
ALTER ROLE gpay_admin SET search_path = public;

COMMIT;
