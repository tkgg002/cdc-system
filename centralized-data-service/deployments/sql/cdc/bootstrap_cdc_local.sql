-- ============================================================
-- bootstrap_cdc_local.sql — Phase 01 split E2E (T-B6)
-- Run on: gpay-postgres-cdc / cdc_dw
--
-- Seeds CDC control plane (cdc_system.*) for the multi-PG E2E test.
-- Flow:
--   source : gpay-postgres-source / goopay_source / public.orders   (PG, logical)
--   shadow : gpay-postgres-cdc    / cdc_dw        / shadow_goopay_source.orders
--   master : gpay-postgres-dest   / goopay_dest   / dw_orders.orders_fact
--
-- Hostnames are Docker bridge DNS names (compose service names).
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- 0. Cleanup local bootstrap slice (idempotent)
-- ------------------------------------------------------------
DELETE FROM cdc_system.transmute_schedule
 WHERE created_by = 'bootstrap-local';

DELETE FROM cdc_system.mapping_rule_v2
 WHERE created_by = 'bootstrap-local';

DELETE FROM cdc_system.master_binding
 WHERE binding_code IN ('mb_local_orders_fact');

DELETE FROM cdc_system.shadow_binding
 WHERE binding_code IN ('sb_local_goopay_source_orders');

DELETE FROM cdc_system.source_object_registry
 WHERE object_code IN ('src_local_goopay_source_orders')
    OR normalized_source_key IN ('postgresql:goopay_source:public.orders');

DELETE FROM cdc_system.connection_registry
 WHERE connection_code IN ('src_local_pg_source', 'shadow_local_pg_cdc', 'master_local_pg_dest');

-- ------------------------------------------------------------
-- 1. Connections (3 endpoints, 3 separate PG instances)
-- ------------------------------------------------------------
INSERT INTO cdc_system.connection_registry (
  connection_code, display_name, role_type, engine_type,
  host, port, default_database, default_schema,
  secret_ref, options_json, capabilities_json,
  status, created_by
) VALUES
(
  'src_local_pg_source',
  'Local Source PG (goopay_source)',
  'source', 'postgresql',
  'postgres-source', 5432, 'goopay_source', 'public',
  'env://source.default',
  '{"sslmode":"disable","wal_level":"logical"}'::jsonb,
  '{"cdc":["snapshot","incremental"],"logical_decoding":true}'::jsonb,
  'active', 'bootstrap-local'
),
(
  'shadow_local_pg_cdc',
  'Local Shadow PG (cdc_dw)',
  'shadow', 'postgresql',
  'postgres-cdc', 5432, 'cdc_dw', 'shadow_goopay_source',
  'env://shadow.default',
  '{"sslmode":"disable"}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active', 'bootstrap-local'
),
(
  'master_local_pg_dest',
  'Local Master PG (goopay_dest)',
  'master', 'postgresql',
  'postgres-dest', 5432, 'goopay_dest', 'dw_orders',
  'env://master.default',
  '{"sslmode":"disable"}'::jsonb,
  '{"ddl":true,"upsert":true}'::jsonb,
  'active', 'bootstrap-local'
);

-- ------------------------------------------------------------
-- 2. Source object — public.orders (BIGINT PK, updated_at ts)
-- ------------------------------------------------------------
WITH src_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'src_local_pg_source'
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
  'src_local_goopay_source_orders',
  src_conn.id,
  'postgresql',
  'goopay_source',
  'public',
  'public',
  'orders',
  'table',
  '{
    "topic_prefix":"cdc.gpay",
    "database":"goopay_source",
    "schema":"public",
    "table":"orders",
    "topic_example":"cdc.gpay.goopay_source.public.orders",
    "replica_identity":"FULL"
  }'::jsonb,
  'postgresql:goopay_source:public.orders',
  'id',
  'BIGINT',
  'updated_at',
  '["updated_at","created_at"]'::jsonb,
  'incremental',
  'debezium',
  TRUE,
  'active',
  'Phase 01 split E2E — PG source orders → shadow → master_dest'
FROM src_conn;

-- ------------------------------------------------------------
-- 3. Shadow binding (target: cdc_dw.shadow_goopay_source.orders)
-- ------------------------------------------------------------
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_source_orders'
),
shadow_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'shadow_local_pg_cdc'
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
  'sb_local_goopay_source_orders',
  src_obj.id,
  shadow_conn.id,
  'cdc_dw',
  'shadow_goopay_source',
  'orders',
  'shadow_goopay_source.orders',
  'preserve',
  'upsert',
  'pending',
  TRUE
FROM src_obj, shadow_conn;

-- ------------------------------------------------------------
-- 4. Master binding (target: goopay_dest.dw_orders.orders_fact)
-- ------------------------------------------------------------
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_source_orders'
),
shadow_bind AS (
  SELECT id FROM cdc_system.shadow_binding WHERE binding_code = 'sb_local_goopay_source_orders'
),
master_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'master_local_pg_dest'
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
  'mb_local_orders_fact',
  src_obj.id,
  shadow_bind.id,
  master_conn.id,
  'goopay_dest',
  'dw_orders',
  'orders_fact',
  'dw_orders.orders_fact',
  'copy_1_to_1',
  '{"mode":"shadow_to_master","source":"local-bootstrap-split"}'::jsonb,
  'approved',
  TRUE,
  'bootstrap-local',
  NOW(),
  'bootstrap-local'
FROM src_obj, shadow_bind, master_conn;

-- ------------------------------------------------------------
-- 5. Mapping rules (1-to-1 column copy)
-- ------------------------------------------------------------
WITH src_obj AS (
  SELECT id FROM cdc_system.source_object_registry WHERE object_code = 'src_local_goopay_source_orders'
),
master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_local_orders_fact'
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
    ('id',         '$.after.id',         'source_id',   'BIGINT',        'jsonpath', NULL,                 FALSE, NULL, 'PG source PK'),
    ('user_id',    '$.after.user_id',    'user_id',     'BIGINT',        'jsonpath', NULL,                 TRUE,  NULL, 'FK users.id'),
    ('amount',     '$.after.amount',     'amount',      'NUMERIC(15,2)', 'jsonpath', NULL,                 TRUE,  '0',  'Order total'),
    ('status',     '$.after.status',     'order_status','VARCHAR(32)',   'jsonpath', NULL,                 TRUE,  NULL, 'Order status'),
    ('notes',      '$.after.notes',      'notes',       'TEXT',          'jsonpath', NULL,                 TRUE,  NULL, 'Free-form notes'),
    ('created_at', '$.after.created_at', 'created_at',  'TIMESTAMPTZ',   'jsonpath', 'parse_rfc3339_time', TRUE,  NULL, 'Created ts'),
    ('updated_at', '$.after.updated_at', 'updated_at',  'TIMESTAMPTZ',   'jsonpath', 'parse_rfc3339_time', TRUE,  NULL, 'Updated ts')
) AS v(source_field, source_path, target_column, data_type, source_format, transform_fn, is_nullable, default_value, notes)
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- 6. Schedule (post_ingest = transmute right after each shadow batch)
-- ------------------------------------------------------------
WITH master_bind AS (
  SELECT id FROM cdc_system.master_binding WHERE binding_code = 'mb_local_orders_fact'
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
