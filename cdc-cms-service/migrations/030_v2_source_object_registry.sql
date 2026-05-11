-- Migration 030: V2 source object registry

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.source_object_registry (
  id                        BIGSERIAL PRIMARY KEY,
  object_code               VARCHAR(150) NOT NULL UNIQUE,
  source_connection_id      BIGINT NOT NULL REFERENCES cdc_system.connection_registry(id),
  source_engine_type        VARCHAR(32) NOT NULL
    CHECK (source_engine_type IN ('postgresql','mariadb','mysql','mongodb','clickhouse')),
  source_database           VARCHAR(255),
  source_schema             VARCHAR(255),
  source_namespace          VARCHAR(255),
  source_object_name        VARCHAR(255) NOT NULL,
  source_object_type        VARCHAR(32) NOT NULL
    CHECK (source_object_type IN ('table','collection','view')),
  source_locator_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
  normalized_source_key     VARCHAR(500) NOT NULL UNIQUE,
  primary_key_field         VARCHAR(255) NOT NULL DEFAULT 'id',
  primary_key_type          VARCHAR(100),
  timestamp_field           VARCHAR(255),
  timestamp_candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  cdc_mode                  VARCHAR(32) NOT NULL DEFAULT 'incremental'
    CHECK (cdc_mode IN ('snapshot','incremental','full_refresh','hybrid')),
  sync_engine               VARCHAR(32) NOT NULL DEFAULT 'debezium'
    CHECK (sync_engine IN ('debezium','airbyte','both','custom')),
  is_active                 BOOLEAN NOT NULL DEFAULT TRUE,
  profile_status            VARCHAR(32) NOT NULL DEFAULT 'draft'
    CHECK (profile_status IN ('draft','pending_data','syncing','active','failed','paused')),
  notes                     TEXT,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_v2_source_object_connection
  ON cdc_system.source_object_registry(source_connection_id);

CREATE INDEX IF NOT EXISTS idx_v2_source_object_active
  ON cdc_system.source_object_registry(is_active);

CREATE INDEX IF NOT EXISTS idx_v2_source_object_profile_status
  ON cdc_system.source_object_registry(profile_status);

CREATE INDEX IF NOT EXISTS idx_v2_source_object_namespace
  ON cdc_system.source_object_registry(source_database, source_schema, source_namespace, source_object_name);

COMMENT ON TABLE cdc_system.source_object_registry IS
  'Canonical logical source objects. Replaces the table-centric identity assumptions in cdc_table_registry.';

COMMIT;
