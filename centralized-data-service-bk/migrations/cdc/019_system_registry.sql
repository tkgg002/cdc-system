-- Migration 019: System Registry for table sync status tracking
-- Status enum: pending_data | syncing | active | failed
-- User decision #3: cdc_internal namespace

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_internal.table_registry (
  target_table       TEXT PRIMARY KEY,
  source_db          TEXT NOT NULL,
  source_collection  TEXT NOT NULL,
  profile_status     TEXT NOT NULL DEFAULT 'pending_data'
    CHECK (profile_status IN ('pending_data', 'syncing', 'active', 'failed')),
  is_financial       BOOLEAN NOT NULL DEFAULT FALSE,
  schema_approved_at TIMESTAMPTZ,
  schema_approved_by TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_table_registry_status
  ON cdc_internal.table_registry (profile_status);

COMMENT ON TABLE cdc_internal.table_registry IS
  'v1.25 table lifecycle tracking. Empty streams stay pending_data, financial tables require admin approval before syncing→active.';

COMMIT;
