-- Migration 035: Seed V2 metadata from legacy registry tables
-- Goal: bootstrap V2 without changing runtime behavior yet.
--
-- PARTIALLY DISABLED 2026-05-12 (revised from 2026-05-11):
--   LIVE: 3 connection_registry rows (legacy_system_db / legacy_shadow_default /
--         legacy_master_default). Đây là INFRASTRUCTURE, không phải demo —
--         register flow (V2SyncCommand.resolveSourceConnectionID +
--         resolveShadowConnectionID) bắt buộc phải có ít nhất 1 row
--         role_type='source' và 1 row role_type='shadow' active, nếu không
--         INSERT vào source_object_registry / shadow_binding sẽ fail.
--   DISABLED (block /* ... */ bên dưới): 4 INSERT...SELECT fan-out đọc từ
--         cdc_table_registry (đã disable 10 pilot rows ở 001) →
--         source_object_registry / shadow_binding / master_binding /
--         mapping_rule_v2. Đó mới là demo phụ thuộc pilot data.
--   Lesson: tách rạch ròi infrastructure seed vs demo seed trong cùng migration.
-- Ref: agent/memory/workspaces/feature-cdc-system-recreate-2026-05-11/10_gap_analysis_demo_seed_2026-05-11.md

BEGIN;

INSERT INTO cdc_system.connection_registry (
  connection_code,
  display_name,
  role_type,
  engine_type,
  default_database,
  default_schema,
  secret_ref,
  options_json,
  capabilities_json,
  status,
  created_by
)
SELECT
  'legacy_system_db',
  'Legacy System DB',
  'system',
  'postgresql',
  current_database(),
  'public',
  'env:DB_SINK_URL',
  '{}'::jsonb,
  '{"supports_schema": true, "supports_upsert": true, "supports_jsonb": true}'::jsonb,
  'active',
  'migration_035'
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.connection_registry WHERE connection_code = 'legacy_system_db'
);

INSERT INTO cdc_system.connection_registry (
  connection_code,
  display_name,
  role_type,
  engine_type,
  default_database,
  default_schema,
  secret_ref,
  options_json,
  capabilities_json,
  status,
  created_by
)
SELECT
  'legacy_shadow_default',
  'Legacy Shadow Default',
  'shadow',
  'postgresql',
  current_database(),
  'cdc_internal',
  'env:DB_SINK_URL',
  '{}'::jsonb,
  '{"supports_schema": true, "supports_upsert": true, "supports_jsonb": true}'::jsonb,
  'active',
  'migration_035'
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.connection_registry WHERE connection_code = 'legacy_shadow_default'
);

INSERT INTO cdc_system.connection_registry (
  connection_code,
  display_name,
  role_type,
  engine_type,
  default_database,
  default_schema,
  secret_ref,
  options_json,
  capabilities_json,
  status,
  created_by
)
SELECT
  'legacy_master_default',
  'Legacy Master Default',
  'master',
  'postgresql',
  current_database(),
  'public',
  'env:DB_SINK_URL',
  '{}'::jsonb,
  '{"supports_schema": true, "supports_upsert": true, "supports_jsonb": true}'::jsonb,
  'active',
  'migration_035'
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.connection_registry WHERE connection_code = 'legacy_master_default'
);

-- Demo fan-out disabled: cdc_table_registry pilot seed (migration 001 §7) đang OFF,
-- nên các INSERT...SELECT bên dưới sẽ rỗng anyway, nhưng giữ /* */ để rõ ý đồ:
-- "fan-out này thuộc demo path, đừng tự ý bật cho tới khi có chiến lược seed source data".
/*
WITH source_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'legacy_system_db'
),
shadow_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'legacy_shadow_default'
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
  'legacy_' || r.id,
  sc.id,
  CASE
    WHEN LOWER(r.source_type) = 'postgres' THEN 'postgresql'
    WHEN LOWER(r.source_type) = 'postgresql' THEN 'postgresql'
    WHEN LOWER(r.source_type) = 'mariadb' THEN 'mariadb'
    WHEN LOWER(r.source_type) = 'mysql' THEN 'mysql'
    WHEN LOWER(r.source_type) = 'mongodb' THEN 'mongodb'
    ELSE 'postgresql'
  END,
  r.source_db,
  CASE
    WHEN POSITION('.' IN r.source_table) > 0 THEN split_part(r.source_table, '.', 1)
    ELSE NULL
  END,
  r.source_db,
  CASE
    WHEN POSITION('.' IN r.source_table) > 0 THEN split_part(r.source_table, '.', 2)
    ELSE r.source_table
  END,
  CASE WHEN LOWER(r.source_type) = 'mongodb' THEN 'collection' ELSE 'table' END,
  jsonb_build_object(
    'legacy_registry_id', r.id,
    'legacy_target_table', r.target_table
  ),
  LOWER(
    COALESCE(r.source_type, 'unknown') || ':' ||
    COALESCE(r.source_db, 'unknown') || ':' ||
    COALESCE(r.source_table, 'unknown')
  ),
  COALESCE(r.primary_key_field, 'id'),
  r.primary_key_type,
  r.timestamp_field,
  COALESCE(r.timestamp_field_candidates, '[]'::jsonb),
  'incremental',
  COALESCE(NULLIF(r.sync_engine, ''), 'debezium'),
  COALESCE(r.is_active, TRUE),
  CASE
    WHEN COALESCE(r.is_active, TRUE) = FALSE THEN 'paused'
    ELSE 'active'
  END,
  r.notes
FROM cdc_table_registry r
CROSS JOIN source_conn sc
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.source_object_registry s
  WHERE s.normalized_source_key = LOWER(
    COALESCE(r.source_type, 'unknown') || ':' ||
    COALESCE(r.source_db, 'unknown') || ':' ||
    COALESCE(r.source_table, 'unknown')
  )
);

WITH shadow_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'legacy_shadow_default'
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
  'shadow_legacy_' || s.id,
  s.id,
  sc.id,
  current_database(),
  'cdc_internal',
  r.target_table,
  'cdc_internal.' || r.target_table,
  'flatten',
  'upsert',
  CASE WHEN COALESCE(r.is_table_created, FALSE) THEN 'created' ELSE 'pending' END,
  COALESCE(r.is_active, TRUE)
FROM cdc_table_registry r
JOIN cdc_system.source_object_registry s
  ON s.normalized_source_key = LOWER(
    COALESCE(r.source_type, 'unknown') || ':' ||
    COALESCE(r.source_db, 'unknown') || ':' ||
    COALESCE(r.source_table, 'unknown')
  )
CROSS JOIN shadow_conn sc
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.shadow_binding b WHERE b.binding_code = 'shadow_legacy_' || s.id
);

WITH master_conn AS (
  SELECT id FROM cdc_system.connection_registry WHERE connection_code = 'legacy_master_default'
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
  created_by
)
SELECT
  'master_legacy_' || m.id,
  s.id,
  sb.id,
  mc.id,
  current_database(),
  'public',
  m.master_name,
  'public.' || m.master_name,
  m.transform_type,
  COALESCE(m.spec, '{}'::jsonb),
  m.schema_status,
  m.is_active,
  m.created_by
FROM cdc_internal.master_table_registry m
JOIN cdc_system.shadow_binding sb
  ON sb.shadow_table = m.source_shadow
JOIN cdc_system.source_object_registry s
  ON s.id = sb.source_object_id
CROSS JOIN master_conn mc
WHERE NOT EXISTS (
  SELECT 1 FROM cdc_system.master_binding b WHERE b.binding_code = 'master_legacy_' || m.id
);

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
  s.id,
  mb.id,
  mr.source_field,
  mr.jsonpath,
  mr.target_column,
  mr.data_type,
  COALESCE(mr.source_format, 'raw'),
  mr.transform_fn,
  mr.is_nullable,
  mr.default_value,
  mr.is_active,
  mr.status,
  mr.notes,
  mr.created_by,
  mr.updated_by
FROM cdc_mapping_rules mr
JOIN cdc_system.shadow_binding sb
  ON sb.shadow_table = mr.source_table
JOIN cdc_system.source_object_registry s
  ON s.id = sb.source_object_id
LEFT JOIN cdc_system.master_binding mb
  ON mb.source_object_id = s.id
 AND (
   (mr.master_table IS NOT NULL AND mb.master_table = mr.master_table)
   OR (mr.master_table IS NULL AND mb.shadow_binding_id = sb.id)
 )
WHERE NOT EXISTS (
  SELECT 1
  FROM cdc_system.mapping_rule_v2 v2
  WHERE v2.source_object_id = s.id
    AND COALESCE(v2.master_binding_id, 0) = COALESCE(mb.id, 0)
    AND v2.target_column = mr.target_column
);
*/

COMMIT;
