-- ============================================================
-- Track D Hardening (post-P4) — DLQ schema drift fix.
--
-- Migration 012 (DLQ state machine) added next_retry_at + last_error
-- + idx_fsl_retry_poll on `public.failed_sync_logs`. Migration 037
-- moved system tables to schema `cdc_system` and dropped the legacy
-- public copy. Result: cdc_system.failed_sync_logs (created directly
-- by bootstrap_cdc_system_v2_local.sql) was never patched with the 2
-- DLQ columns, while internal/handler/dlq_state_machine.go queries
-- them. Worker boot logs `column "next_retry_at" does not exist
-- (SQLSTATE 42703)` every poll cycle (5 minutes).
--
-- This migration replays 012's ALTERs against the cdc_system copy.
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
-- Safe on partitioned parents — ALTER cascades to partitions.
-- ============================================================

BEGIN;

DO $fsl_cdc_cols$
DECLARE
    rel_kind CHAR;
BEGIN
    SELECT c.relkind INTO rel_kind
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'cdc_system';

    IF rel_kind IS NULL THEN
        RAISE NOTICE '[045_dlq_columns_in_cdc_system] cdc_system.failed_sync_logs does not exist - skipping';
        RETURN;
    END IF;

    EXECUTE 'ALTER TABLE cdc_system.failed_sync_logs
             ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ';

    EXECUTE 'ALTER TABLE cdc_system.failed_sync_logs
             ADD COLUMN IF NOT EXISTS last_error TEXT';

    RAISE NOTICE '[045_dlq_columns_in_cdc_system] cdc_system.failed_sync_logs columns ensured';
END $fsl_cdc_cols$;

DO $fsl_cdc_idx$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'cdc_system'
    ) THEN
        RETURN;
    END IF;

    BEGIN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fsl_retry_poll
                 ON cdc_system.failed_sync_logs (next_retry_at, status)';
    EXCEPTION WHEN others THEN
        RAISE WARNING '[045_dlq_columns_in_cdc_system] idx_fsl_retry_poll failed: %', SQLERRM;
    END;
END $fsl_cdc_idx$;

COMMIT;
