-- Migration 040 (Phase 39): Move admin_actions từ public sang cdc_system.
-- Replaces cdc-cms-service/migrations/005_admin_actions.sql (orphan, không có runner).
-- Audit log cho destructive admin actions trên CDC stack. Partitioned by month
-- on created_at. Primary key bắt buộc include partition key → (created_at, id).

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.admin_actions (
    id              BIGSERIAL,
    user_id         TEXT        NOT NULL,
    action          TEXT        NOT NULL,
    target          TEXT,
    payload         JSONB,
    reason          TEXT        NOT NULL,
    result          TEXT,
    idempotency_key TEXT,
    ip_address      TEXT,
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE TABLE IF NOT EXISTS cdc_system.admin_actions_2026_04
    PARTITION OF cdc_system.admin_actions
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS cdc_system.admin_actions_2026_05
    PARTITION OF cdc_system.admin_actions
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS cdc_system.admin_actions_2026_06
    PARTITION OF cdc_system.admin_actions
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS cdc_system.admin_actions_default
    PARTITION OF cdc_system.admin_actions DEFAULT;

CREATE INDEX IF NOT EXISTS idx_admin_actions_user
    ON cdc_system.admin_actions (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_actions_action
    ON cdc_system.admin_actions (action, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_actions_idem
    ON cdc_system.admin_actions (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMIT;
