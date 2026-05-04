-- Wipe script for gpay-postgres-dest (destination/DW).
-- Drops master + all dw_<binding> schemas + recreates empty public.
-- WARNING: DESTRUCTIVE.

BEGIN;

-- 1. Drop all dw_* schemas (one per Wizard-registered binding)
DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT schema_name FROM information_schema.schemata
     WHERE schema_name LIKE 'dw\_%' ESCAPE '\'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', rec.schema_name);
  END LOOP;
END $$;

-- 2. Drop master schema (recreated by 001_dest_init)
DROP SCHEMA IF EXISTS master CASCADE;

-- 3. Public reset
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO PUBLIC;
COMMENT ON SCHEMA public IS
  'Phase 01 split E2E — destination DB; public empty by convention.';

COMMIT;
