-- Migration 032: V2 master bindings

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.master_binding (
  id                 BIGSERIAL PRIMARY KEY,
  binding_code       VARCHAR(150) NOT NULL UNIQUE,
  source_object_id   BIGINT NOT NULL REFERENCES cdc_system.source_object_registry(id) ON DELETE CASCADE,
  shadow_binding_id  BIGINT REFERENCES cdc_system.shadow_binding(id) ON DELETE SET NULL,
  master_connection_id BIGINT NOT NULL REFERENCES cdc_system.connection_registry(id),
  master_database    VARCHAR(255),
  master_schema      VARCHAR(255) NOT NULL,
  master_table       VARCHAR(255) NOT NULL,
  physical_table_fqn VARCHAR(600) NOT NULL,
  transform_type     VARCHAR(32) NOT NULL
    CHECK (transform_type IN ('copy_1_to_1','filter','aggregate','group_by','join','custom_sql')),
  transform_spec     JSONB NOT NULL DEFAULT '{}'::jsonb,
  schema_status      VARCHAR(32) NOT NULL DEFAULT 'pending_review'
    CHECK (schema_status IN ('pending_review','approved','rejected','failed','drifted')),
  is_active          BOOLEAN NOT NULL DEFAULT FALSE,
  schema_reviewed_by VARCHAR(100),
  schema_reviewed_at TIMESTAMPTZ,
  rejection_reason   TEXT,
  created_by         VARCHAR(100),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_connection_id, master_schema, master_table),
  CONSTRAINT v2_master_active_requires_approved
    CHECK (is_active = FALSE OR schema_status = 'approved')
);

CREATE INDEX IF NOT EXISTS idx_v2_master_binding_source
  ON cdc_system.master_binding(source_object_id);

CREATE INDEX IF NOT EXISTS idx_v2_master_binding_shadow
  ON cdc_system.master_binding(shadow_binding_id);

CREATE INDEX IF NOT EXISTS idx_v2_master_binding_active
  ON cdc_system.master_binding(is_active);

CREATE INDEX IF NOT EXISTS idx_v2_master_binding_status
  ON cdc_system.master_binding(schema_status);

COMMENT ON TABLE cdc_system.master_binding IS
  'Per-source or per-shadow master projections. Supports one source object fan-out to many master destinations.';

COMMIT;
