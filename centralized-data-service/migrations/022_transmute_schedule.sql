-- Migration 022 — R7 Scheduler: cron-driven Transmute runs
-- Plan ref: §15 Execution Modes

CREATE TABLE IF NOT EXISTS cdc_internal.transmute_schedule (
  id              BIGSERIAL PRIMARY KEY,
  master_table    TEXT NOT NULL,
  mode            TEXT NOT NULL CHECK (mode IN ('immediate','cron','post_ingest')),
  cron_expr       TEXT NULL,
  last_run_at     TIMESTAMPTZ NULL,
  next_run_at     TIMESTAMPTZ NULL,
  last_status     TEXT NULL CHECK (last_status IS NULL OR last_status IN ('success','failed','running','skipped')),
  last_error      TEXT NULL,
  last_stats      JSONB NULL,
  is_enabled      BOOLEAN NOT NULL DEFAULT false,
  created_by      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (master_table, mode)
);

COMMENT ON TABLE cdc_internal.transmute_schedule IS
  'Cron + on-demand transmute schedules per master table. Scheduler polls rows WHERE is_enabled AND mode=''cron'' AND next_run_at <= NOW().';

CREATE INDEX IF NOT EXISTS idx_schedule_due
  ON cdc_internal.transmute_schedule(next_run_at)
  WHERE is_enabled = true AND mode = 'cron';

CREATE INDEX IF NOT EXISTS idx_schedule_master
  ON cdc_internal.transmute_schedule(master_table);

-- Guard: cron_expr must be non-null when mode='cron'
ALTER TABLE cdc_internal.transmute_schedule
  ADD CONSTRAINT schedule_cron_expr_required
  CHECK (mode != 'cron' OR cron_expr IS NOT NULL);
