-- Migration 037: move legacy system tables into cdc_system

BEGIN;

CREATE SCHEMA IF NOT EXISTS cdc_system;

ALTER TABLE IF EXISTS public.cdc_table_registry SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS public.cdc_mapping_rules SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS public.pending_fields SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS public.schema_changes_log SET SCHEMA cdc_system;

-- Phase 01 split E2E (T-B3): 010 now creates partitioned cdc_activity_log
-- and failed_sync_logs DIRECTLY in cdc_system. The legacy non-partitioned
-- copies left by 006/008 in public are now redundant — drop them so 044
-- has nothing to clean and Phase 39 invariant (public empty) holds.
DROP TABLE IF EXISTS public.cdc_activity_log CASCADE;
DROP TABLE IF EXISTS public.failed_sync_logs CASCADE;

ALTER TABLE IF EXISTS public.cdc_reconciliation_report SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS public.recon_runs SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS public.cdc_worker_schedule SET SCHEMA cdc_system;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'table_registry'
  ) THEN
    ALTER TABLE cdc_internal.table_registry SET SCHEMA cdc_system;
    ALTER TABLE cdc_system.table_registry RENAME TO table_registry_legacy;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'master_table_registry'
  ) THEN
    ALTER TABLE cdc_internal.master_table_registry SET SCHEMA cdc_system;
    ALTER TABLE cdc_system.master_table_registry RENAME TO master_table_registry_legacy;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'transmute_schedule'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'cdc_system' AND table_name = 'transmute_schedule'
    ) THEN
      DROP TABLE cdc_internal.transmute_schedule;
    ELSE
      ALTER TABLE cdc_internal.transmute_schedule SET SCHEMA cdc_system;
      ALTER TABLE cdc_system.transmute_schedule RENAME TO transmute_schedule_legacy;
    END IF;
  END IF;
END $$;

ALTER TABLE IF EXISTS cdc_internal.worker_registry SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.schema_proposal SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.enum_types SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.sources SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.cdc_wizard_sessions SET SCHEMA cdc_system;

COMMIT;
