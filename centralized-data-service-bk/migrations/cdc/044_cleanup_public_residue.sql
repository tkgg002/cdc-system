-- Migration 044 (Phase 39): Cleanup public schema residue.
-- After 037 moved cdc_activity_log + failed_sync_logs parents to cdc_system,
-- their child partitions (created by migration 010 with search_path=public)
-- remained orphaned in public. Migration 001/004 also seeded legacy test
-- source tables and partition helpers in public. Phase 39 invariant:
-- "public empty by convention; app tables only in cdc_system /
-- cdc_auth_service / shadow_<src> / dw_<binding>".

BEGIN;

-- 1. Drop orphan partitions from migration 010 (no longer attached to
--    cdc_system parents).
DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT table_name
      FROM information_schema.tables
     WHERE table_schema = 'public'
       AND (
            table_name LIKE 'cdc_activity_log_%'
         OR table_name LIKE 'failed_sync_logs_%'
       )
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', rec.table_name);
  END LOOP;
END $$;

-- 2. Drop legacy test source tables from migration 001 seed.
--    V2 reads from real upstream DBs via Debezium; these public.* test
--    tables are no longer the source of record.
DROP TABLE IF EXISTS public.orders CASCADE;
DROP TABLE IF EXISTS public.order_items CASCADE;
DROP TABLE IF EXISTS public.users CASCADE;
DROP TABLE IF EXISTS public.merchants CASCADE;
DROP TABLE IF EXISTS public.payments CASCADE;
DROP TABLE IF EXISTS public.refunds CASCADE;
DROP TABLE IF EXISTS public.wallets CASCADE;
DROP TABLE IF EXISTS public.wallet_transactions CASCADE;
DROP TABLE IF EXISTS public.legacy_payments CASCADE;
DROP TABLE IF EXISTS public.legacy_refunds CASCADE;

-- 3. Drop legacy CDC helper functions bound to public (from migration
--    001/004). These are V1 helpers; V2 uses cdc_system.* equivalents.
DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT p.oid::regprocedure AS sig
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'public'
       AND NOT EXISTS (
             SELECT 1 FROM pg_depend d
              WHERE d.objid = p.oid AND d.deptype = 'e'
           )
  LOOP
    EXECUTE format('DROP FUNCTION IF EXISTS %s CASCADE', rec.sig);
  END LOOP;
END $$;

-- 4. Verify: 0 user tables and 0 non-extension functions remain in public.
DO $$
DECLARE
  n_tables INT;
  n_funcs  INT;
BEGIN
  SELECT count(*) INTO n_tables
    FROM information_schema.tables
   WHERE table_schema = 'public';

  SELECT count(*) INTO n_funcs
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'public'
     AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
            WHERE d.objid = p.oid AND d.deptype = 'e'
         );

  IF n_tables > 0 OR n_funcs > 0 THEN
    RAISE EXCEPTION 'public schema not empty: tables=%, funcs=%', n_tables, n_funcs;
  END IF;
END $$;

COMMIT;
