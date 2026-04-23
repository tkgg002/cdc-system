-- Migration 020 — R0 foundation: mapping_rules JsonPath + enum_types + data_type CHECK
-- Plan ref: 02_plan_airbyte_removal_v2_command_center.md §14 (Type catalog), §R0
-- Table name: cdc_mapping_rules (plural, per 001_init_schema.sql)
-- Idempotent via IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

-- ============================================================
-- 20.1: cdc_mapping_rules — JsonPath + source_format + version + master_table
-- ============================================================
ALTER TABLE cdc_mapping_rules
  ADD COLUMN IF NOT EXISTS source_format TEXT NOT NULL DEFAULT 'debezium_after'
    CHECK (source_format IN ('debezium_after','debezium_full_envelope','airbyte_flat','raw_jsonb')),
  ADD COLUMN IF NOT EXISTS jsonpath TEXT NULL,
  ADD COLUMN IF NOT EXISTS transform_fn TEXT NULL,
  ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS previous_version_id INTEGER NULL
    REFERENCES cdc_mapping_rules(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS approved_by_admin BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS master_table TEXT NULL;

COMMENT ON COLUMN cdc_mapping_rules.jsonpath IS
  'gjson-compatible path extracting value from _raw_data. Example "after.fee" for Debezium envelope, "." for Airbyte flat. Takes precedence over source_field when non-null.';
COMMENT ON COLUMN cdc_mapping_rules.source_format IS
  'Anchor format: debezium_after reads _raw_data.after.<path>; debezium_full_envelope reads _raw_data.<path>; airbyte_flat reads column directly; raw_jsonb reads _raw_data.<path>';
COMMENT ON COLUMN cdc_mapping_rules.transform_fn IS
  'Optional transform applied after extraction. Whitelist only — see transmuter.go transformRegistry.';
COMMENT ON COLUMN cdc_mapping_rules.master_table IS
  'Target master table (public.<name>). NULL = legacy per-source_table behaviour.';

-- Index for Transmuter lookup by source+master combo
CREATE INDEX IF NOT EXISTS idx_mapping_rules_active_sorted
  ON cdc_mapping_rules (source_table, master_table, status, is_active)
  WHERE is_active = true AND status = 'approved';

-- Relax legacy unique (source_table, source_field) → allow same field → multiple target cols across masters
DO $$
DECLARE
  constraint_name_var TEXT;
BEGIN
  SELECT conname INTO constraint_name_var
    FROM pg_constraint
   WHERE conrelid = 'cdc_mapping_rules'::regclass
     AND contype = 'u'
     AND pg_get_constraintdef(oid) LIKE 'UNIQUE (source_table, source_field)';
  IF constraint_name_var IS NOT NULL THEN
    EXECUTE format('ALTER TABLE cdc_mapping_rules DROP CONSTRAINT %I', constraint_name_var);
  END IF;
END $$;

-- New broader unique: allow same (source_table, source_field) to map to different (master_table, target_column)
CREATE UNIQUE INDEX IF NOT EXISTS ux_mapping_rules_identity
  ON cdc_mapping_rules (source_table, COALESCE(master_table, ''), target_column);

-- ============================================================
-- 20.2: data_type CHECK — siết precision
-- ============================================================
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint
             WHERE conrelid = 'cdc_mapping_rules'::regclass
               AND conname = 'mapping_rules_data_type_chk')
  THEN
    ALTER TABLE cdc_mapping_rules DROP CONSTRAINT mapping_rules_data_type_chk;
  END IF;
END $$;

-- Normalise legacy bare NUMERIC → NUMERIC(20,4), legacy DECIMAL → DECIMAL(20,4)
-- Admin can refine precision per-column via CMS PATCH later.
UPDATE cdc_mapping_rules SET data_type = 'NUMERIC(20,4)' WHERE data_type = 'NUMERIC';
UPDATE cdc_mapping_rules SET data_type = 'DECIMAL(20,4)' WHERE data_type = 'DECIMAL';

ALTER TABLE cdc_mapping_rules
  ADD CONSTRAINT mapping_rules_data_type_chk
  CHECK (data_type ~ '^(SMALLINT|INTEGER|BIGINT|REAL|DOUBLE PRECISION|BOOLEAN|DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL|JSON|JSONB|UUID|INET|CIDR|MACADDR|BYTEA|TEXT|CHAR\([1-9][0-9]{0,7}\)|VARCHAR\([1-9][0-9]{0,7}\)|NUMERIC\([1-9][0-9]?,[0-9][0-9]?\)|DECIMAL\([1-9][0-9]?,[0-9][0-9]?\)|(SMALLINT|INTEGER|BIGINT|TEXT|UUID)\[\]|ENUM:[a-z_][a-z0-9_]{0,62})$');

-- ============================================================
-- 20.3: enum_types registry
-- ============================================================
CREATE TABLE IF NOT EXISTS cdc_internal.enum_types (
  name        TEXT PRIMARY KEY CHECK (name ~ '^[a-z_][a-z0-9_]{0,62}$'),
  values      TEXT[] NOT NULL CHECK (array_length(values, 1) BETWEEN 1 AND 100),
  is_active   BOOLEAN NOT NULL DEFAULT true,
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  description TEXT
);

COMMENT ON TABLE cdc_internal.enum_types IS
  'Named enum types referenced by mapping_rules.data_type=''ENUM:<name>''. Shared across master tables for consistency.';

INSERT INTO cdc_internal.enum_types (name, values, description, created_by) VALUES
  ('payment_state', ARRAY['PENDING','SUCCESS','FAILED','CANCELLED','REFUNDED'],
   'Payment lifecycle states (derived from payment_bills.state sample)', 'migration-020'),
  ('api_type', ARRAY['REDIRECT','QR','DIRECT','WEBHOOK'],
   'Payment API surface types', 'migration-020'),
  ('currency_iso', ARRAY['VND','USD','EUR','JPY','SGD','THB','KRW','CNY'],
   'Supported ISO currency codes', 'migration-020')
ON CONFLICT (name) DO NOTHING;
