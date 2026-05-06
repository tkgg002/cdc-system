-- ----------------------------------------------------------------------
-- 052_create_cdc_jobs.sql — Phase 2 v2 / P3 (cdc-cms-service refactor).
-- Target DB: gpay-postgres-cdc (cdc_dw)
--
-- Creates the unified async-execution job tracker for the CommandBus
-- pattern:
--   1. CMS dispatches a Command (POST /api/.../<action>).
--   2. NATSCommandBus.Dispatch validates → INSERT row here (status='pending')
--      → publish NATS subject → return 202 + job_id.
--   3. Worker subscribes the cmd subject → executes → publishes
--      cdc.evt.<X>.completed.
--   4. JobMonitor (worker) subscribes wildcard cdc.evt.*.completed → UPDATE
--      this row (status, result, finished_at).
--   5. CMS surfaces progress via GET /api/jobs/:id (queries.GetJobQuery).
--
-- Schema mirrors internal/domain/job/job.go (Status enum: pending|running|
-- success|failed). idempotency_key UNIQUE for retry-dedup at dispatch time.
--
-- Re-runnable: IF NOT EXISTS guards everything.
-- ----------------------------------------------------------------------
BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

CREATE TABLE IF NOT EXISTS cdc_system.cdc_jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT        NOT NULL,          -- e.g. "master.swap", "recon.check"
    status          TEXT        NOT NULL DEFAULT 'pending',
    payload         JSONB       NOT NULL,
    result          JSONB,
    error_message   TEXT,
    idempotency_key TEXT        UNIQUE,            -- optional dedup at Dispatch
    created_by      TEXT        NOT NULL,
    correlation_id  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,

    CONSTRAINT cdc_jobs_status_chk
        CHECK (status IN ('pending', 'running', 'success', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_cdc_jobs_type_status
    ON cdc_system.cdc_jobs (type, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_cdc_jobs_correlation
    ON cdc_system.cdc_jobs (correlation_id)
    WHERE correlation_id IS NOT NULL;

COMMENT ON TABLE  cdc_system.cdc_jobs              IS 'Async job tracker for the CommandBus pattern (Phase 2 v2 / P3).';
COMMENT ON COLUMN cdc_system.cdc_jobs.type         IS 'Stable command type id, e.g. master.swap, recon.check.';
COMMENT ON COLUMN cdc_system.cdc_jobs.status       IS 'pending|running|success|failed.';
COMMENT ON COLUMN cdc_system.cdc_jobs.payload      IS 'Full Command payload (JSON-serialized).';
COMMENT ON COLUMN cdc_system.cdc_jobs.result       IS 'Worker-emitted result body (set by JobMonitor on completed evt).';
COMMENT ON COLUMN cdc_system.cdc_jobs.idempotency_key IS 'Optional unique key for client-side dedup (HTTP retry safety).';

-- Verification report.
SELECT
    (SELECT count(*) FROM information_schema.tables
       WHERE table_schema='cdc_system' AND table_name='cdc_jobs')      AS table_exists,
    (SELECT count(*) FROM pg_indexes
       WHERE schemaname='cdc_system' AND tablename='cdc_jobs')         AS index_count,
    (SELECT count(*) FROM information_schema.table_constraints
       WHERE table_schema='cdc_system' AND table_name='cdc_jobs'
         AND constraint_type='CHECK')                                   AS check_count;

COMMIT;
