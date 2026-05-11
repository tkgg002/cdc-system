-- Migration 041 (Phase 39): Move cdc_alerts từ public sang cdc_system.
-- Replaces cdc-cms-service/migrations/013_alerts.sql (orphan).
-- State store cho observability alerts (system_health_collector → alert_manager).

BEGIN;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS cdc_system.cdc_alerts (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint       TEXT        NOT NULL UNIQUE,
    name              TEXT        NOT NULL,
    severity          TEXT        NOT NULL,
    labels            JSONB,
    description       TEXT,
    status            TEXT        NOT NULL,
    fired_at          TIMESTAMPTZ NOT NULL,
    resolved_at       TIMESTAMPTZ,
    ack_by            TEXT,
    ack_at            TIMESTAMPTZ,
    silenced_by       TEXT,
    silenced_until    TIMESTAMPTZ,
    silence_reason    TEXT,
    occurrence_count  INT         NOT NULL DEFAULT 1,
    last_fired_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_alerts_status
    ON cdc_system.cdc_alerts (status, fired_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity_firing
    ON cdc_system.cdc_alerts (severity, status)
    WHERE status = 'firing';
CREATE INDEX IF NOT EXISTS idx_alerts_resolved_at
    ON cdc_system.cdc_alerts (resolved_at DESC)
    WHERE resolved_at IS NOT NULL;

COMMIT;
