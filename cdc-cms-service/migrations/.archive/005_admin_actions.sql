-- Phase 4 (Security) — Audit log for destructive admin actions.
-- Plan ref: agent/memory/workspaces/feature-cdc-integration/02_plan_data_integrity_v3.md §13.
--
-- Partitioned by month on created_at. 3 initial partitions (current month ±1).
-- Primary key must include partition key → (created_at, id).

CREATE TABLE IF NOT EXISTS admin_actions (
    id              BIGSERIAL,
    user_id         TEXT        NOT NULL,
    action          TEXT        NOT NULL,            -- restart_connector | reset_offset | heal | trigger_snapshot | retry_failed | ...
    target          TEXT,                             -- connector name, table name, failed-log id, ...
    payload         JSONB,
    reason          TEXT        NOT NULL,
    result          TEXT,                             -- success | error:<message>
    idempotency_key TEXT,
    ip_address      TEXT,
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

-- Monthly partitions — 3 months forward from 2026-04.
CREATE TABLE IF NOT EXISTS admin_actions_2026_04 PARTITION OF admin_actions
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS admin_actions_2026_05 PARTITION OF admin_actions
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS admin_actions_2026_06 PARTITION OF admin_actions
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Default catch-all so inserts outside defined ranges do not fail (ops continuity).
CREATE TABLE IF NOT EXISTS admin_actions_default PARTITION OF admin_actions DEFAULT;

CREATE INDEX IF NOT EXISTS idx_admin_actions_user
    ON admin_actions (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_actions_action
    ON admin_actions (action, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_actions_idem
    ON admin_actions (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
