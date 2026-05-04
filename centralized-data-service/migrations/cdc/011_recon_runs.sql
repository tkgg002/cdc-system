-- ============================================================
-- CDC Integration v3 — Phase 1 (T4)
-- `recon_runs` state table for window-based Recon orchestrator.
-- Tracks each Tier 1/2/3 execution with status transitions
-- running → success | failed | cancelled.
--
-- Partial UNIQUE index enforces AT MOST ONE 'running' row per
-- table — the DB-side safety net behind the Go advisory lock.
--
-- Idempotent: every CREATE guards with IF NOT EXISTS.
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS recon_runs (
    id UUID PRIMARY KEY,
    table_name TEXT NOT NULL,
    tier INT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running','success','failed','cancelled')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    docs_scanned BIGINT DEFAULT 0,
    windows_checked INT DEFAULT 0,
    mismatches_found INT DEFAULT 0,
    heal_actions INT DEFAULT 0,
    error_message TEXT,
    instance_id TEXT
);

-- At-most-one running row per table. Uses a PARTIAL unique index so
-- historical rows (status != 'running') don't block new executions.
CREATE UNIQUE INDEX IF NOT EXISTS recon_runs_one_running
    ON recon_runs (table_name)
    WHERE status = 'running';

-- Fast recent-run lookup by table — supports the CMS reporting page.
CREATE INDEX IF NOT EXISTS recon_runs_table_started
    ON recon_runs (table_name, started_at DESC);

-- Diagnostic index: find all failed runs in a time window.
CREATE INDEX IF NOT EXISTS recon_runs_status_started
    ON recon_runs (status, started_at DESC);

COMMIT;
