-- Migration 023 — R8 Data Warehouse: 1 shadow → N masters
-- Plan ref: §16

CREATE TABLE IF NOT EXISTS cdc_internal.master_table_registry (
  id                  BIGSERIAL PRIMARY KEY,
  master_name         TEXT NOT NULL UNIQUE
                        CHECK (master_name ~ '^[a-z_][a-z0-9_]{0,62}$'),
  source_shadow       TEXT NOT NULL,
  transform_type      TEXT NOT NULL
                        CHECK (transform_type IN ('copy_1_to_1','filter','aggregate','group_by','join')),
  spec                JSONB NOT NULL,
  is_active           BOOLEAN NOT NULL DEFAULT false,
  schema_status       TEXT NOT NULL DEFAULT 'pending_review'
                        CHECK (schema_status IN ('pending_review','approved','rejected','failed')),
  schema_reviewed_by  TEXT,
  schema_reviewed_at  TIMESTAMPTZ,
  rejection_reason    TEXT,
  created_by          TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (source_shadow) REFERENCES cdc_internal.table_registry(target_table) ON DELETE RESTRICT
);

COMMENT ON TABLE cdc_internal.master_table_registry IS
  'Declarative master table definitions. 1 shadow → N masters via transform_type (copy/filter/aggregate/group_by/join). Spec JSON validated per type.';

CREATE INDEX IF NOT EXISTS idx_master_active
  ON cdc_internal.master_table_registry(source_shadow, is_active);

CREATE INDEX IF NOT EXISTS idx_master_status
  ON cdc_internal.master_table_registry(schema_status);

-- Invariant: is_active=true requires schema_status='approved'
ALTER TABLE cdc_internal.master_table_registry
  ADD CONSTRAINT master_active_requires_approved
  CHECK (is_active = false OR schema_status = 'approved');
