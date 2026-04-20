-- ============================================================
-- CDC Integration v3 — Phase 2/3 (T4)
-- DLQ state machine: add next_retry_at / status transitions on
-- `failed_sync_logs`. Migration 008 shipped a string status column
-- but no scheduling field; migration 010 partitioned the table by
-- created_at. We add the missing columns in a way that is safe for
-- partitioned parents (ALTER on the parent cascades to partitions).
--
-- State machine:
--   pending | failed    →  retrying (worker picks it up)
--   retrying           →  resolved | dead_letter
-- Rows with status='pending' or ('retrying' AND next_retry_at<NOW())
-- are eligible for pickup by the DLQ retry worker.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
-- ============================================================

BEGIN;

DO $fsl_cols$
DECLARE
    rel_kind CHAR;
BEGIN
    SELECT c.relkind INTO rel_kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'public';

    IF rel_kind IS NULL THEN
        RAISE NOTICE '[012_dlq_state_machine] failed_sync_logs does not exist - skipping';
        RETURN;
    END IF;

    -- next_retry_at controls scheduled retry pickup. NULL = pick up
    -- immediately (or on worker next tick).
    EXECUTE 'ALTER TABLE failed_sync_logs
             ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ';

    -- Expand the status value space beyond the original default.
    -- We do NOT add a CHECK constraint because the column is VARCHAR(20)
    -- and application logic owns the lifecycle. A CHECK would have to be
    -- replayed per partition which is error-prone across historical data.

    -- Keep retry_count column (already shipped in 008). Add last_error
    -- for diagnostic breadcrumbs — optional, safe to no-op if present.
    -- (No-op when absent: migration 008 only ships error_message.)
    EXECUTE 'ALTER TABLE failed_sync_logs
             ADD COLUMN IF NOT EXISTS last_error TEXT';

    RAISE NOTICE '[012_dlq_state_machine] failed_sync_logs columns ensured';
END $fsl_cols$;

-- Index supporting the worker poll query:
--   WHERE status IN ('pending','failed')
--     OR (status='retrying' AND next_retry_at < NOW())
-- A partial index keeps storage tight.
DO $fsl_idx$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'public'
    ) THEN
        RETURN;
    END IF;

    BEGIN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_fsl_retry_poll
                 ON failed_sync_logs (next_retry_at, status)';
    EXCEPTION WHEN others THEN
        RAISE WARNING '[012_dlq_state_machine] idx_fsl_retry_poll failed: %', SQLERRM;
    END;
END $fsl_idx$;

COMMIT;
