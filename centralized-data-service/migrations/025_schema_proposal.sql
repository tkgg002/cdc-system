-- Migration 025 — R9 Schema Approval Workflow
-- Plan ref: §18

CREATE TABLE IF NOT EXISTS cdc_internal.schema_proposal (
  id                     BIGSERIAL PRIMARY KEY,
  table_name             TEXT NOT NULL,
  table_layer            TEXT NOT NULL CHECK (table_layer IN ('shadow','master')),
  column_name            TEXT NOT NULL
                           CHECK (column_name ~ '^[a-z_][a-zA-Z0-9_]{0,62}$'),
  proposed_data_type     TEXT NOT NULL,
  proposed_jsonpath      TEXT NULL,
  proposed_transform_fn  TEXT NULL,
  proposed_is_nullable   BOOLEAN NOT NULL DEFAULT true,
  proposed_default_value TEXT NULL,
  sample_values          JSONB NULL,
  status                 TEXT NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','approved','rejected','auto_applied','failed')),
  submitted_by           TEXT NOT NULL,
  submitted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reviewed_by            TEXT,
  reviewed_at            TIMESTAMPTZ,
  applied_at             TIMESTAMPTZ,
  rejection_reason       TEXT,
  override_data_type     TEXT,
  override_jsonpath      TEXT,
  override_transform_fn  TEXT,
  error_message          TEXT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (table_name, table_layer, column_name, status)
    DEFERRABLE INITIALLY IMMEDIATE
);

COMMENT ON TABLE cdc_internal.schema_proposal IS
  'Schema change lifecycle. New fields auto-detected by SinkWorker/Transmuter create pending proposals; admin reviews + approves with override_* options OR rejects. Approval triggers ALTER TABLE + mapping_rule creation (master layer).';

CREATE INDEX IF NOT EXISTS idx_proposal_pending
  ON cdc_internal.schema_proposal(submitted_at DESC)
  WHERE status='pending';

CREATE INDEX IF NOT EXISTS idx_proposal_table
  ON cdc_internal.schema_proposal(table_layer, table_name);
