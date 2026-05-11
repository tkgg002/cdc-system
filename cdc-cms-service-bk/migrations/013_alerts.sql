-- Phase 6 — Alert State Machine
--
-- `cdc_alerts` is the persistent state store for observability alerts produced
-- by `system_health_collector`. The fingerprint (hash of name + sorted labels)
-- is the natural key used for dedup across collector ticks: repeated fires of
-- the same condition increment `occurrence_count` + `last_fired_at` rather than
-- inserting a new row, so the FE banner never balloons during a flap.
--
-- Lifecycle: firing -> resolved (auto) / acknowledged (manual) / silenced (manual).
-- Background resolver (see alert_manager.go) re-opens silences whose deadline
-- has passed and ages out resolved rows older than 60s from the active list.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS cdc_alerts (
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

-- Primary query: "active alerts sorted by recency" (FE banner + API /active).
CREATE INDEX IF NOT EXISTS idx_alerts_status ON cdc_alerts (status, fired_at DESC);

-- Fast filter for firing-only rollups used by the collector (e.g. critical badge count).
CREATE INDEX IF NOT EXISTS idx_alerts_severity_firing
    ON cdc_alerts (severity, status)
    WHERE status = 'firing';

-- History queries (resolved_at range scans).
CREATE INDEX IF NOT EXISTS idx_alerts_resolved_at
    ON cdc_alerts (resolved_at DESC)
    WHERE resolved_at IS NOT NULL;
