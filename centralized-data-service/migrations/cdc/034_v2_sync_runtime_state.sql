-- Migration 034: V2 runtime state

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.sync_runtime_state (
  id                 BIGSERIAL PRIMARY KEY,
  source_object_id   BIGINT REFERENCES cdc_system.source_object_registry(id) ON DELETE CASCADE,
  shadow_binding_id  BIGINT REFERENCES cdc_system.shadow_binding(id) ON DELETE CASCADE,
  master_binding_id  BIGINT REFERENCES cdc_system.master_binding(id) ON DELETE CASCADE,
  runtime_scope      VARCHAR(32) NOT NULL
    CHECK (runtime_scope IN ('source','shadow','master')),
  last_success_at    TIMESTAMPTZ,
  last_error_at      TIMESTAMPTZ,
  last_error_message TEXT,
  last_cursor_json   JSONB,
  last_source_ts     BIGINT,
  last_recon_at      TIMESTAMPTZ,
  recon_drift_count  BIGINT NOT NULL DEFAULT 0,
  ddl_status         VARCHAR(32),
  stats_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT v2_runtime_requires_target_ref
    CHECK (
      (runtime_scope = 'source' AND source_object_id IS NOT NULL)
      OR (runtime_scope = 'shadow' AND shadow_binding_id IS NOT NULL)
      OR (runtime_scope = 'master' AND master_binding_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_v2_runtime_scope
  ON cdc_system.sync_runtime_state(runtime_scope);

CREATE INDEX IF NOT EXISTS idx_v2_runtime_source
  ON cdc_system.sync_runtime_state(source_object_id);

CREATE INDEX IF NOT EXISTS idx_v2_runtime_shadow
  ON cdc_system.sync_runtime_state(shadow_binding_id);

CREATE INDEX IF NOT EXISTS idx_v2_runtime_master
  ON cdc_system.sync_runtime_state(master_binding_id);

COMMENT ON TABLE cdc_system.sync_runtime_state IS
  'Separated runtime state for source, shadow, and master scopes. Keeps control-plane metadata independent from operational watermarks.';

COMMIT;
