-- ============================================================
-- bootstrap_dest_local.sql — Phase 01 split E2E (T-B6)
-- Run on: gpay-postgres-dest / goopay_dest
--
-- Phase 39 invariant: dest DB holds NO control-plane rows.
-- Master DDL handler creates dw_<binding> schemas + physical tables
-- on demand when Wizard registers a binding (via cdc-worker connect
-- as gpay_admin to goopay_dest).
--
-- This file is intentionally minimal — keeps the symmetry with
-- bootstrap_cdc_local.sql and gives a single hook to extend later
-- (e.g. seeding RLS roles, dest-side audit tables, etc.).
-- ============================================================

BEGIN;

-- 1. Sanity: master schema must exist (created by 001_dest_init).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name = 'master'
  ) THEN
    RAISE EXCEPTION 'master schema missing — run migrate-dest first (001_dest_init.sql).';
  END IF;
END $$;

-- 2. Sanity: public must be empty.
DO $$
DECLARE
  cnt INT;
BEGIN
  SELECT count(*) INTO cnt
    FROM information_schema.tables
   WHERE table_schema = 'public';
  IF cnt > 0 THEN
    RAISE EXCEPTION 'public schema has % tables — Phase 39 invariant violated.', cnt;
  END IF;
END $$;

-- 3. NOTICE confirmation
DO $$
BEGIN
  RAISE NOTICE 'dest bootstrap OK — master/ ready, public empty, dw_* will be auto-created on Wizard register.';
END $$;

COMMIT;
