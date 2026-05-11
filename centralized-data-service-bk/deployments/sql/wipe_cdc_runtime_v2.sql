-- Wipe script for CDC V2 reset
-- WARNING:
--   This script is DESTRUCTIVE.
--   It is intended only for the approved "wipe & bootstrap" flow.
--
-- What it does:
--   1. Drops master physical tables registered in cdc_system.master_binding
--   2. Drops every shadow schema matching shadow_%
--   3. Drops leftover legacy/public system tables if any still exist
--   4. Truncates all tables in cdc_system and resets identities
--   5. Drops cdc_internal if it still exists

BEGIN;

-- ============================================================================
-- 1. Drop master physical tables from current bindings
-- Uses control-plane metadata before metadata is truncated.
-- ============================================================================
DO $$
DECLARE
  rec RECORD;
  has_master_binding BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'cdc_system'
      AND table_name = 'master_binding'
  ) INTO has_master_binding;

  IF NOT has_master_binding THEN
    RETURN;
  END IF;

  FOR rec IN
    SELECT DISTINCT
      COALESCE(NULLIF(master_schema, ''), 'public') AS schema_name,
      master_table AS table_name
    FROM cdc_system.master_binding
    WHERE master_table IS NOT NULL
      AND master_table <> ''
      AND COALESCE(NULLIF(master_schema, ''), 'public') <> 'cdc_system'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE', rec.schema_name, rec.table_name);
  END LOOP;
END $$;

-- ============================================================================
-- 2. Drop empty master schemas created for projections
-- Keep public and cdc_system.
-- ============================================================================
DO $$
DECLARE
  rec RECORD;
  object_count INTEGER;
  has_master_binding BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'cdc_system'
      AND table_name = 'master_binding'
  ) INTO has_master_binding;

  IF NOT has_master_binding THEN
    RETURN;
  END IF;

  FOR rec IN
    SELECT DISTINCT master_schema AS schema_name
    FROM cdc_system.master_binding
    WHERE master_schema IS NOT NULL
      AND master_schema <> ''
      AND master_schema NOT IN ('public', 'cdc_system')
  LOOP
    SELECT COUNT(*) INTO object_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = rec.schema_name
      AND c.relkind IN ('r','p','v','m','S','f');

    IF object_count = 0 THEN
      EXECUTE format('DROP SCHEMA IF EXISTS %I', rec.schema_name);
    END IF;
  END LOOP;
END $$;

-- ============================================================================
-- 3. Drop all shadow schemas
-- Convention for this phase: every shadow schema is shadow_<source_db>
-- ============================================================================
DO $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name LIKE 'shadow\_%' ESCAPE '\'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', rec.schema_name);
  END LOOP;
END $$;

-- ============================================================================
-- 4. Nuke public schema entirely (Phase 39).
-- Rule absolute từ user: "public xoá mẹ đi". All app tables phải sống
-- trong cdc_system / cdc_auth_service / shadow_<src> / dw_<binding>.
-- Recreate empty để extension default (pgcrypto, …) vẫn install được.
-- ============================================================================
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO PUBLIC;
COMMENT ON SCHEMA public IS
  'Phase 39 — kept empty by convention. App tables: cdc_system / cdc_auth_service / shadow_<src> / dw_<binding>.';

-- ============================================================================
-- 5. Drop cdc_system entirely (Phase 39).
-- Rationale: TRUNCATE-only kept DDL but several legacy migrations are
-- not idempotent (CREATE INDEX without IF NOT EXISTS). Fresh DROP +
-- recreate ensures `make migrate` rebuilds from scratch deterministically.
-- ============================================================================
DROP SCHEMA IF EXISTS cdc_system CASCADE;

-- ============================================================================
-- 6. Drop cdc_auth_service entirely (Phase 39).
-- Re-seeded by cdc-auth-service/migrations/001_auth_users.sql with
-- admin/admin123 default. Local dev only.
-- ============================================================================
DROP SCHEMA IF EXISTS cdc_auth_service CASCADE;

-- ============================================================================
-- 7. Drop cdc_internal if anything recreated it.
-- ============================================================================
DROP SCHEMA IF EXISTS cdc_internal CASCADE;

COMMIT;
