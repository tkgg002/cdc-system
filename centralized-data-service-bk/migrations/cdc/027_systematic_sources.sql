-- Migration 027: Systematic Flow — Sources registry + Wizard state machine
-- Adds: cdc_internal.sources, cdc_internal.cdc_wizard_sessions
-- Depends on: 018_sonyflake_v125_foundation.sql (creates cdc_internal schema)

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_internal.sources (
  id                      BIGSERIAL PRIMARY KEY,
  connector_name          VARCHAR(200) NOT NULL UNIQUE,
  source_type             VARCHAR(32)  NOT NULL,
  connector_class         VARCHAR(200) NOT NULL,
  topic_prefix            VARCHAR(200),
  server_address          VARCHAR(500),
  database_include_list   VARCHAR(500),
  collection_include_list TEXT,
  raw_config_sanitized    JSONB,
  status                  VARCHAR(32)  NOT NULL DEFAULT 'created',
  created_by              VARCHAR(100),
  created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT sources_status_check CHECK (status IN ('created','running','paused','failed','deleted'))
);

CREATE INDEX IF NOT EXISTS idx_sources_status ON cdc_internal.sources(status);
CREATE INDEX IF NOT EXISTS idx_sources_type   ON cdc_internal.sources(source_type);

COMMENT ON TABLE  cdc_internal.sources IS
  'Systematic Flow: Connection Fingerprint. Persisted after POST /api/v1/system/connectors succeeds on Kafka Connect. Registry dropdown reads from here.';
COMMENT ON COLUMN cdc_internal.sources.status IS
  'Lifecycle: created -> running (after verified) -> paused/failed. Soft-delete: deleted.';

CREATE TABLE IF NOT EXISTS cdc_internal.cdc_wizard_sessions (
  id             UUID PRIMARY KEY,
  source_name    VARCHAR(200),
  connector_id   BIGINT REFERENCES cdc_internal.sources(id) ON DELETE SET NULL,
  registry_id    BIGINT,
  master_name    VARCHAR(200),
  current_step   INTEGER NOT NULL DEFAULT 0,
  status         VARCHAR(32) NOT NULL DEFAULT 'draft',
  step_payload   JSONB NOT NULL DEFAULT '{}'::jsonb,
  progress_log   JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_by     VARCHAR(100),
  created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT wizard_status_check CHECK (status IN ('draft','running','done','failed'))
);

CREATE INDEX IF NOT EXISTS idx_wizard_sessions_status ON cdc_internal.cdc_wizard_sessions(status);
CREATE INDEX IF NOT EXISTS idx_wizard_sessions_created_by ON cdc_internal.cdc_wizard_sessions(created_by);

COMMENT ON TABLE cdc_internal.cdc_wizard_sessions IS
  'Systematic Flow: Wizard State Machine. Persists draft + progress of Source->Master automation.';

COMMIT;
