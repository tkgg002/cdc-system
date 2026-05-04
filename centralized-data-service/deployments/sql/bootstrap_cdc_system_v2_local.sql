-- Local bootstrap seed for CDC System V2
-- Environment assumptions:
--   - PostgreSQL: localhost:5432 / goopay_dw
--   - MongoDB: localhost:17017
--   - Single local Postgres instance reused for system/shadow/master
--
-- Flow seeded by this file:
--   source  : Mongo goopay_payment.payments
--   shadow  : shadow_goopay_payment.payments
--   master  : dw_payment.payment_fact

BEGIN;

-- ============================================================================
-- 0. Cleanup this local bootstrap slice only
-- ============================================================================
DELETE FROM cdc_system.transmute_schedule
WHERE created_by = 'bootstrap-local';

DELETE FROM cdc_system.mapping_rule_v2
WHERE created_by = 'bootstrap-local';

DELETE FROM cdc_system.master_binding
WHERE binding_code IN ('mb_local_payment_fact');

DELETE FROM cdc_system.shadow_binding
WHERE binding_code IN ('sb_local_goopay_payment_payments');

DELETE FROM cdc_system.source_object_registry
WHERE object_code IN ('src_local_goopay_payment_payments')
   OR normalized_source_key IN ('mongodb:goopay_payment:payments');

DELETE FROM cdc_system.connection_registry
WHERE connection_code IN ('src_local_mongo_goopay', 'shadow_local_pg', 'master_local_pg');

-- ============================================================================
-- 1. Connections
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
  'src_local_mongo_goopay',
  'Local Source Mongo Goopay',
  'source',
  'mongodb',
  'localhost',
  17017,
  'goopay_payment',
  NULL,
  'env://mongodb.url',
  '{"directConnection":true}'::jsonb,
  '{"cdc":["snapshot","incremental"]}'::jsonb,
  'active',
  'bootstrap-local'
),
(
  'shadow_local_pg',
  'Local Shadow PostgreSQL',
  'shadow',
  'postgresql',
  'localhost',
  5432,
  'goopay_dw',
  'shadow_goopay_payment',
  'env://shadow.default',
  '{}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active',
  'bootstrap-local'
),
(
  'master_local_pg',
  'Local Master PostgreSQL',
  'master',
  'postgresql',
  'localhost',
  5432,
  'goopay_dw',
  'dw_payment',
  'env://master.default',
  '{}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active',
  'bootstrap-local'
);

-- ============================================================================
-- 2. Source object
-- Topic convention already seen in tests:
--   cdc.goopay.goopay_payment.payments
-- ============================================================================
WITH src_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'src_local_mongo_goopay'
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
  'src_local_goopay_payment_payments',
  src_conn.id,
  'mongodb',
  'goopay_payment',
  NULL,
  'goopay_payment',
  'payments',
  'collection',
  '{
    "topic_prefix":"cdc.goopay",
    "database":"goopay_payment",
    "collection":"payments",
    "topic_example":"cdc.goopay.goopay_payment.payments",
    "is_financial":true
  }'::jsonb,
  'mongodb:goopay_payment:payments',
  '_id',
  'VARCHAR(24)',
  'updatedAt',
  '["updatedAt","createdAt","completedAt"]'::jsonb,
  'incremental',
  'debezium',
  TRUE,
  'active',
  'Local bootstrap flow for goopay_payment.payments'
FROM src_conn;

-- ============================================================================
-- 3. Shadow binding
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_payment_payments'
),
shadow_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'shadow_local_pg'
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
  'sb_local_goopay_payment_payments',
  src_obj.id,
  shadow_conn.id,
  'goopay_dw',
  'shadow_goopay_payment',
  'payments',
  'shadow_goopay_payment.payments',
  'preserve',
  'upsert',
  'pending',
  TRUE
FROM src_obj, shadow_conn;

-- ============================================================================
-- 4. Master binding
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_payment_payments'
),
shadow_bind AS (
  SELECT id FROM cdc_system.shadow_binding WHERE binding_code = 'sb_local_goopay_payment_payments'
),
master_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'master_local_pg'
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
  'mb_local_payment_fact',
  src_obj.id,
  shadow_bind.id,
  master_conn.id,
  'goopay_dw',
  'dw_payment',
  'payment_fact',
  'dw_payment.payment_fact',
  'copy_1_to_1',
  '{"mode":"shadow_to_master","source":"local-bootstrap"}'::jsonb,
  'approved',
  TRUE,
  'bootstrap-local',
  NOW(),
  'bootstrap-local'
FROM src_obj, shadow_bind, master_conn;

-- ============================================================================
-- 5. Mapping rules
-- ============================================================================
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_payment_payments'
),
master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_local_payment_fact'
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
  'bootstrap-local',
  'bootstrap-local'
FROM src_obj, master_bind,
(
  VALUES
    ('_id',         '$.after._id',         'source_id',        'VARCHAR(24)',   'jsonpath', NULL,                 FALSE, NULL, 'Mongo source id'),
    ('amount',      '$.after.amount',      'amount',           'NUMERIC(20,4)', 'jsonpath', NULL,                 TRUE,  '0',  'Payment amount'),
    ('paidAmount',  '$.after.paidAmount',  'paid_amount',      'NUMERIC(20,4)', 'jsonpath', NULL,                 TRUE,  '0',  'Paid amount'),
    ('currency',    '$.after.currency',    'currency',         'TEXT',          'jsonpath', NULL,                 TRUE,  NULL, 'Payment currency'),
    ('status',      '$.after.status',      'payment_status',   'TEXT',          'jsonpath', NULL,                 TRUE,  NULL, 'Payment status'),
    ('merchantId',  '$.after.merchantId',  'merchant_id',      'VARCHAR(64)',   'jsonpath', NULL,                 TRUE,  NULL, 'Merchant id'),
    ('createdAt',   '$.after.createdAt',   'created_at',       'TIMESTAMPTZ',   'jsonpath', 'parse_rfc3339_time', TRUE,  NULL, 'Created timestamp'),
    ('updatedAt',   '$.after.updatedAt',   'updated_at',       'TIMESTAMPTZ',   'jsonpath', 'parse_rfc3339_time', TRUE,  NULL, 'Updated timestamp'),
    ('completedAt', '$.after.completedAt', 'completed_at',     'TIMESTAMPTZ',   'jsonpath', 'parse_rfc3339_time', TRUE,  NULL, 'Completed timestamp')
) AS v(source_field, source_path, target_column, data_type, source_format, transform_fn, is_nullable, default_value, notes)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- 6. Schedule
-- ============================================================================
WITH master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_local_payment_fact'
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
  'bootstrap-local'
FROM master_bind;

COMMIT;
