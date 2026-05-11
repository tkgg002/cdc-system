-- Bootstrap template for CDC System V2 control-plane metadata
-- Usage:
--   1. Run schema migrations up to 038 first.
--   2. Copy this file to an environment-specific SQL file.
--   3. Replace the placeholder values below.
--   4. Execute manually against the SYSTEM database.
--
-- This file seeds only control-plane metadata in cdc_system.
-- It does NOT create physical shadow/master tables directly.
-- Runtime will create those from bindings when services start.

BEGIN;

-- ============================================================================
-- 0. OPTIONAL CLEANUP FOR A SINGLE DEMO FLOW
-- Uncomment only when you want to re-seed the same example codes.
-- ============================================================================
-- DELETE FROM cdc_system.transmute_schedule WHERE created_by = 'bootstrap-template';
-- DELETE FROM cdc_system.mapping_rule_v2 WHERE created_by = 'bootstrap-template';
-- DELETE FROM cdc_system.master_binding WHERE created_by = 'bootstrap-template';
-- DELETE FROM cdc_system.shadow_binding WHERE binding_code IN ('sb_mongo_billing_payments');
-- DELETE FROM cdc_system.source_object_registry WHERE object_code IN ('src_mongo_billing_payments');
-- DELETE FROM cdc_system.connection_registry
--  WHERE connection_code IN ('src_mongo_goopay', 'shadow_pg_default', 'master_pg_finance');

-- ============================================================================
-- 1. CONNECTIONS
-- Replace host/port/database/secret_ref with your real values.
-- secret_ref should match the secret resolution strategy used by your service.
-- ============================================================================
INSERT INTO cdc_system.connection_registry (
  connection_code,
  display_name,
  role_type,
  engine_type,
  host,
  port,
  default_database,
  default_schema,
  secret_ref,
  options_json,
  capabilities_json,
  status,
  created_by
)
VALUES
(
  'src_mongo_goopay',
  'Source Mongo Goopay',
  'source',
  'mongodb',
  'mongo-host',
  27017,
  'billing',
  NULL,
  'secret://cdc/source/mongo_goopay',
  '{"replicaSet":"rs0"}'::jsonb,
  '{"cdc":["snapshot","incremental"]}'::jsonb,
  'active',
  'bootstrap-template'
),
(
  'shadow_pg_default',
  'Shadow PostgreSQL Default',
  'shadow',
  'postgresql',
  'postgres-host',
  5432,
  'goopay_dw',
  'shadow_billing',
  'secret://cdc/shadow/postgres_default',
  '{}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active',
  'bootstrap-template'
),
(
  'master_pg_finance',
  'Master PostgreSQL Finance',
  'master',
  'postgresql',
  'postgres-host',
  5432,
  'goopay_dw',
  'dw_finance',
  'secret://cdc/master/postgres_finance',
  '{}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active',
  'bootstrap-template'
)
ON CONFLICT (connection_code) DO UPDATE
SET
  display_name = EXCLUDED.display_name,
  role_type = EXCLUDED.role_type,
  engine_type = EXCLUDED.engine_type,
  host = EXCLUDED.host,
  port = EXCLUDED.port,
  default_database = EXCLUDED.default_database,
  default_schema = EXCLUDED.default_schema,
  secret_ref = EXCLUDED.secret_ref,
  options_json = EXCLUDED.options_json,
  capabilities_json = EXCLUDED.capabilities_json,
  status = EXCLUDED.status,
  updated_at = NOW();

-- ============================================================================
-- 2. SOURCE OBJECT
-- Mongo example:
--   source_database = billing
--   source_object_name = payments
--   shadow target schema should become shadow_billing
-- ============================================================================
WITH src_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'src_mongo_goopay'
)
INSERT INTO cdc_system.source_object_registry (
  object_code,
  source_connection_id,
  source_engine_type,
  source_database,
  source_schema,
  source_namespace,
  source_object_name,
  source_object_type,
  source_locator_json,
  normalized_source_key,
  primary_key_field,
  primary_key_type,
  timestamp_field,
  timestamp_candidates_json,
  cdc_mode,
  sync_engine,
  is_active,
  profile_status,
  notes
)
SELECT
  'src_mongo_billing_payments',
  src_conn.id,
  'mongodb',
  'billing',
  NULL,
  'billing',
  'payments',
  'collection',
  '{
    "topic_prefix":"cdc.goopay",
    "database":"billing",
    "collection":"payments",
    "is_financial":true
  }'::jsonb,
  'mongodb:billing:payments',
  '_id',
  'VARCHAR(24)',
  'updatedAt',
  '["updatedAt","createdAt"]'::jsonb,
  'incremental',
  'debezium',
  TRUE,
  'active',
  'Bootstrap example for Mongo billing.payments'
FROM src_conn
ON CONFLICT (object_code) DO UPDATE
SET
  source_connection_id = EXCLUDED.source_connection_id,
  source_engine_type = EXCLUDED.source_engine_type,
  source_database = EXCLUDED.source_database,
  source_schema = EXCLUDED.source_schema,
  source_namespace = EXCLUDED.source_namespace,
  source_object_name = EXCLUDED.source_object_name,
  source_object_type = EXCLUDED.source_object_type,
  source_locator_json = EXCLUDED.source_locator_json,
  normalized_source_key = EXCLUDED.normalized_source_key,
  primary_key_field = EXCLUDED.primary_key_field,
  primary_key_type = EXCLUDED.primary_key_type,
  timestamp_field = EXCLUDED.timestamp_field,
  timestamp_candidates_json = EXCLUDED.timestamp_candidates_json,
  cdc_mode = EXCLUDED.cdc_mode,
  sync_engine = EXCLUDED.sync_engine,
  is_active = EXCLUDED.is_active,
  profile_status = EXCLUDED.profile_status,
  notes = EXCLUDED.notes,
  updated_at = NOW();

-- ============================================================================
-- 3. SHADOW BINDING
-- Convention for current phase:
--   source db name = billing
--   shadow schema = shadow_billing
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_mongo_billing_payments'
),
shadow_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'shadow_pg_default'
)
INSERT INTO cdc_system.shadow_binding (
  binding_code,
  source_object_id,
  shadow_connection_id,
  shadow_database,
  shadow_schema,
  shadow_table,
  physical_table_fqn,
  namespace_strategy,
  write_mode,
  ddl_status,
  is_active
)
SELECT
  'sb_mongo_billing_payments',
  src_obj.id,
  shadow_conn.id,
  'goopay_dw',
  'shadow_billing',
  'payments',
  'shadow_billing.payments',
  'preserve',
  'upsert',
  'pending',
  TRUE
FROM src_obj, shadow_conn
ON CONFLICT (binding_code) DO UPDATE
SET
  source_object_id = EXCLUDED.source_object_id,
  shadow_connection_id = EXCLUDED.shadow_connection_id,
  shadow_database = EXCLUDED.shadow_database,
  shadow_schema = EXCLUDED.shadow_schema,
  shadow_table = EXCLUDED.shadow_table,
  physical_table_fqn = EXCLUDED.physical_table_fqn,
  namespace_strategy = EXCLUDED.namespace_strategy,
  write_mode = EXCLUDED.write_mode,
  ddl_status = EXCLUDED.ddl_status,
  is_active = EXCLUDED.is_active,
  updated_at = NOW();

-- ============================================================================
-- 4. MASTER BINDING
-- Example:
--   shadow_billing.payments -> dw_finance.payment_fact
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_mongo_billing_payments'
),
shadow_bind AS (
  SELECT id FROM cdc_system.shadow_binding WHERE binding_code = 'sb_mongo_billing_payments'
),
master_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'master_pg_finance'
)
INSERT INTO cdc_system.master_binding (
  binding_code,
  source_object_id,
  shadow_binding_id,
  master_connection_id,
  master_database,
  master_schema,
  master_table,
  physical_table_fqn,
  transform_type,
  transform_spec,
  schema_status,
  is_active,
  schema_reviewed_by,
  schema_reviewed_at,
  created_by
)
SELECT
  'mb_finance_payment_fact',
  src_obj.id,
  shadow_bind.id,
  master_conn.id,
  'goopay_dw',
  'dw_finance',
  'payment_fact',
  'dw_finance.payment_fact',
  'copy_1_to_1',
  '{"mode":"shadow_to_master"}'::jsonb,
  'approved',
  TRUE,
  'bootstrap-template',
  NOW(),
  'bootstrap-template'
FROM src_obj, shadow_bind, master_conn
ON CONFLICT (binding_code) DO UPDATE
SET
  source_object_id = EXCLUDED.source_object_id,
  shadow_binding_id = EXCLUDED.shadow_binding_id,
  master_connection_id = EXCLUDED.master_connection_id,
  master_database = EXCLUDED.master_database,
  master_schema = EXCLUDED.master_schema,
  master_table = EXCLUDED.master_table,
  physical_table_fqn = EXCLUDED.physical_table_fqn,
  transform_type = EXCLUDED.transform_type,
  transform_spec = EXCLUDED.transform_spec,
  schema_status = EXCLUDED.schema_status,
  is_active = EXCLUDED.is_active,
  schema_reviewed_by = EXCLUDED.schema_reviewed_by,
  schema_reviewed_at = EXCLUDED.schema_reviewed_at,
  created_by = EXCLUDED.created_by,
  updated_at = NOW();

-- ============================================================================
-- 5. MAPPING RULES
-- One rule per destination column.
-- Add/remove rows here according to your real master projection.
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_mongo_billing_payments'
),
master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_finance_payment_fact'
)
INSERT INTO cdc_system.mapping_rule_v2 (
  source_object_id,
  master_binding_id,
  source_field,
  source_path,
  target_column,
  data_type,
  source_format,
  transform_fn,
  is_nullable,
  default_value,
  is_active,
  status,
  notes,
  created_by,
  updated_by
)
SELECT
  src_obj.id,
  master_bind.id,
  v.source_field,
  v.source_path,
  v.target_column,
  v.data_type,
  v.source_format,
  v.transform_fn,
  v.is_nullable,
  v.default_value,
  TRUE,
  'approved',
  v.notes,
  'bootstrap-template',
  'bootstrap-template'
FROM src_obj, master_bind,
(
  VALUES
    ('_id',        '$.after._id',        'source_id',       'VARCHAR(24)', 'jsonpath', NULL,                 FALSE, NULL, 'Original source document id'),
    ('amount',     '$.after.amount',     'amount',          'NUMERIC(20,4)', 'jsonpath', NULL,               FALSE, '0',  'Payment amount'),
    ('status',     '$.after.status',     'payment_status',  'TEXT',         'jsonpath', NULL,                 TRUE,  NULL, 'Payment state'),
    ('createdAt',  '$.after.createdAt',  'created_at',      'TIMESTAMPTZ',  'jsonpath', 'parse_rfc3339_time', TRUE, NULL, 'Creation time'),
    ('updatedAt',  '$.after.updatedAt',  'updated_at',      'TIMESTAMPTZ',  'jsonpath', 'parse_rfc3339_time', TRUE, NULL, 'Update time'),
    ('merchantId', '$.after.merchantId', 'merchant_id',     'VARCHAR(64)',  'jsonpath', NULL,                 TRUE,  NULL, 'Merchant id')
) AS v(source_field, source_path, target_column, data_type, source_format, transform_fn, is_nullable, default_value, notes)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- 6. TRANSMUTE SCHEDULE
-- You can choose:
--   - post_ingest: near-real-time after shadow ingest
--   - immediate: manual/command-driven
--   - cron: scheduled
-- ============================================================================
WITH master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_finance_payment_fact'
)
INSERT INTO cdc_system.transmute_schedule (
  master_binding_id,
  mode,
  cron_expr,
  is_enabled,
  created_by
)
SELECT
  master_bind.id,
  'post_ingest',
  NULL,
  TRUE,
  'bootstrap-template'
FROM master_bind
ON CONFLICT (master_binding_id, mode) DO UPDATE
SET
  is_enabled = EXCLUDED.is_enabled,
  updated_at = NOW();

COMMIT;
