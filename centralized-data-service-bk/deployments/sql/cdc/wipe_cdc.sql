-- Wipe script for gpay-postgres-cdc (CDC control plane + shadow).
-- Drops cdc_system + all shadow_<src> schemas + recreates empty public.
-- WARNING: DESTRUCTIVE.

BEGIN;

-- 1. Drop all shadow_* schemas (one per source DB)
DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT schema_name FROM information_schema.schemata
     WHERE schema_name LIKE 'shadow\_%' ESCAPE '\'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', rec.schema_name);
  END LOOP;
END $$;

-- 2. Drop cdc_system entirely (DDL not all idempotent → fresh recreate)
DROP SCHEMA IF EXISTS cdc_system CASCADE;

-- 3. Drop cdc_internal if anything recreated it (Phase 39 invariant)
DROP SCHEMA IF EXISTS cdc_internal CASCADE;

-- 4. Public reset
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO PUBLIC;
COMMENT ON SCHEMA public IS
  'Phase 01 split E2E — CDC DB; public empty by convention.';

COMMIT;
