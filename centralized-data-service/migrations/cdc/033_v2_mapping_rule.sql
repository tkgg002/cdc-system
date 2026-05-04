-- Migration 033: V2 mapping rules

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.mapping_rule_v2 (
  id                BIGSERIAL PRIMARY KEY,
  source_object_id  BIGINT NOT NULL REFERENCES cdc_system.source_object_registry(id) ON DELETE CASCADE,
  master_binding_id BIGINT REFERENCES cdc_system.master_binding(id) ON DELETE CASCADE,
  source_field      VARCHAR(255) NOT NULL,
  source_path       VARCHAR(500),
  target_column     VARCHAR(255) NOT NULL,
  data_type         VARCHAR(100) NOT NULL,
  source_format     VARCHAR(32) NOT NULL DEFAULT 'raw'
    CHECK (source_format IN ('raw','jsonpath','expression')),
  transform_fn      VARCHAR(100),
  is_nullable       BOOLEAN NOT NULL DEFAULT TRUE,
  default_value     TEXT,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  status            VARCHAR(32) NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','approved','rejected')),
  notes             TEXT,
  created_by        VARCHAR(100),
  updated_by        VARCHAR(100),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_v2_mapping_rule_identity
  ON cdc_system.mapping_rule_v2 (
    source_object_id,
    COALESCE(master_binding_id, 0),
    target_column
  );

CREATE INDEX IF NOT EXISTS idx_v2_mapping_rule_source
  ON cdc_system.mapping_rule_v2(source_object_id);

CREATE INDEX IF NOT EXISTS idx_v2_mapping_rule_master
  ON cdc_system.mapping_rule_v2(master_binding_id);

CREATE INDEX IF NOT EXISTS idx_v2_mapping_rule_active_status
  ON cdc_system.mapping_rule_v2(is_active, status);

COMMENT ON TABLE cdc_system.mapping_rule_v2 IS
  'V2 rules bound to source objects and optionally to a concrete master binding, removing source_table-only ambiguity.';

COMMIT;
