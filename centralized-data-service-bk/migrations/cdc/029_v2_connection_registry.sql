-- Migration 029: V2 control-plane bootstrap + connection registry

BEGIN;

CREATE SCHEMA IF NOT EXISTS cdc_system;

CREATE TABLE IF NOT EXISTS cdc_system.connection_registry (
  id                BIGSERIAL PRIMARY KEY,
  connection_code   VARCHAR(100) NOT NULL UNIQUE,
  display_name      VARCHAR(200) NOT NULL,
  role_type         VARCHAR(32) NOT NULL
    CHECK (role_type IN ('source','shadow','master','system','mixed')),
  engine_type       VARCHAR(32) NOT NULL
    CHECK (engine_type IN ('postgresql','mariadb','mysql','mongodb','clickhouse')),
  host              VARCHAR(255),
  port              INTEGER,
  default_database  VARCHAR(255),
  default_schema    VARCHAR(255),
  secret_ref        VARCHAR(255) NOT NULL,
  options_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
  capabilities_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status            VARCHAR(32) NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','paused','failed','retired')),
  created_by        VARCHAR(100),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_v2_connection_role
  ON cdc_system.connection_registry(role_type);

CREATE INDEX IF NOT EXISTS idx_v2_connection_engine
  ON cdc_system.connection_registry(engine_type);

CREATE INDEX IF NOT EXISTS idx_v2_connection_status
  ON cdc_system.connection_registry(status);

COMMENT ON SCHEMA cdc_system IS
  'V2 control plane metadata schema. Stores registry, bindings, and runtime state. Does not store shadow/master payload tables.';

COMMENT ON TABLE cdc_system.connection_registry IS
  'Physical connection catalog used by source, shadow, master, and system layers.';

COMMIT;
