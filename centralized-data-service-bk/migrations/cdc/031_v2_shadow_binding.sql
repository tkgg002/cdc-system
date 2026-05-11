-- Migration 031: V2 shadow bindings

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.shadow_binding (
  id                   BIGSERIAL PRIMARY KEY,
  binding_code         VARCHAR(150) NOT NULL UNIQUE,
  source_object_id     BIGINT NOT NULL REFERENCES cdc_system.source_object_registry(id) ON DELETE CASCADE,
  shadow_connection_id BIGINT NOT NULL REFERENCES cdc_system.connection_registry(id),
  shadow_database      VARCHAR(255),
  shadow_schema        VARCHAR(255) NOT NULL,
  shadow_table         VARCHAR(255) NOT NULL,
  physical_table_fqn   VARCHAR(600) NOT NULL,
  namespace_strategy   VARCHAR(32) NOT NULL DEFAULT 'preserve'
    CHECK (namespace_strategy IN ('preserve','prefix','flatten','custom')),
  write_mode           VARCHAR(32) NOT NULL DEFAULT 'upsert'
    CHECK (write_mode IN ('upsert','append','replace')),
  ddl_status           VARCHAR(32) NOT NULL DEFAULT 'pending'
    CHECK (ddl_status IN ('pending','created','failed','drifted')),
  is_active            BOOLEAN NOT NULL DEFAULT TRUE,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_object_id, shadow_connection_id, shadow_schema, shadow_table)
);

CREATE INDEX IF NOT EXISTS idx_v2_shadow_binding_source
  ON cdc_system.shadow_binding(source_object_id);

CREATE INDEX IF NOT EXISTS idx_v2_shadow_binding_connection
  ON cdc_system.shadow_binding(shadow_connection_id);

CREATE INDEX IF NOT EXISTS idx_v2_shadow_binding_active
  ON cdc_system.shadow_binding(is_active);

COMMENT ON TABLE cdc_system.shadow_binding IS
  'Per-source routing to shadow destinations. Preserves namespace ownership independently from the source object definition.';

COMMIT;
