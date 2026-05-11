-- Migration 036: move transmute schedule into cdc_system control plane

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_system.transmute_schedule (
  id                BIGSERIAL PRIMARY KEY,
  master_binding_id BIGINT NOT NULL REFERENCES cdc_system.master_binding(id) ON DELETE CASCADE,
  mode              TEXT NOT NULL CHECK (mode IN ('immediate','cron','post_ingest')),
  cron_expr         TEXT NULL,
  last_run_at       TIMESTAMPTZ NULL,
  next_run_at       TIMESTAMPTZ NULL,
  last_status       TEXT NULL CHECK (last_status IS NULL OR last_status IN ('success','failed','running','skipped')),
  last_error        TEXT NULL,
  last_stats        JSONB NULL,
  is_enabled        BOOLEAN NOT NULL DEFAULT false,
  created_by        TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_binding_id, mode),
  CONSTRAINT v2_schedule_cron_expr_required
    CHECK (mode != 'cron' OR cron_expr IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_v2_schedule_due
  ON cdc_system.transmute_schedule(next_run_at)
  WHERE is_enabled = true AND mode = 'cron';

CREATE INDEX IF NOT EXISTS idx_v2_schedule_master_binding
  ON cdc_system.transmute_schedule(master_binding_id);

COMMENT ON TABLE cdc_system.transmute_schedule IS
  'V2 cron + on-demand transmute schedules keyed by master_binding_id in the control plane.';

INSERT INTO cdc_system.transmute_schedule (
  master_binding_id,
  mode,
  cron_expr,
  last_run_at,
  next_run_at,
  last_status,
  last_error,
  last_stats,
  is_enabled,
  created_by,
  created_at,
  updated_at
)
SELECT
  mb.id,
  s.mode,
  s.cron_expr,
  s.last_run_at,
  s.next_run_at,
  s.last_status,
  s.last_error,
  s.last_stats,
  s.is_enabled,
  s.created_by,
  s.created_at,
  s.updated_at
FROM cdc_internal.transmute_schedule s
JOIN cdc_system.master_binding mb
  ON mb.master_table = s.master_table
ON CONFLICT (master_binding_id, mode) DO NOTHING;

COMMIT;
