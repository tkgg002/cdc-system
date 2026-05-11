-- Phase multi_engine_unified — L4 T4.4.
-- Seeds 1 source_object_registry row for the new MariaDB sample
-- table goopay_legacy_maria.legacy_orders. Inactive + draft so the
-- Toggle Auto/Manual flow (L2 + L3) can flip it on demand.
--
-- Idempotent: ON CONFLICT (object_code) DO NOTHING — re-runnable.
-- Connection row also seeded if missing.
--
-- DISABLED 2026-05-11: Demo seed cho L4 phase, không thuộc production.
-- 2 row (mariadb_legacy_default connection + legacy_orders source object).
-- Để restore cho dev: uncomment block /* ... */ bên dưới.
-- Ref: agent/memory/workspaces/feature-cdc-system-recreate-2026-05-11/10_gap_analysis_demo_seed_2026-05-11.md

BEGIN;
/*
-- 1. Connection registry row (Debezium MySql connector against MariaDB).
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
  status
)
VALUES (
  'mariadb_legacy_default',
  'MariaDB legacy (Phase multi_engine_unified)',
  'source',
  'mariadb',
  'mariadb',
  3306,
  'goopay_legacy_maria',
  NULL,
  'env:MARIADB_LEGACY_DEFAULT',
  jsonb_build_object(
    'topic_prefix', 'cdc.mariadb',
    'connector', 'io.debezium.connector.mysql.MySqlConnector'
  ),
  'active'
)
ON CONFLICT (connection_code) DO NOTHING;

-- 2. Source object registry row.
WITH conn AS (
  SELECT id FROM cdc_system.connection_registry
   WHERE connection_code = 'mariadb_legacy_default'
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
  cdc_mode,
  sync_engine,
  is_active,
  profile_status,
  provisioning_mode,
  provisioning_state,
  notes
)
SELECT
  'mariadb_legacy_orders_v1',
  conn.id,
  'mariadb',
  'goopay_legacy_maria',
  NULL,
  'goopay_legacy_maria',
  'legacy_orders',
  'table',
  jsonb_build_object(
    'topic_prefix', 'cdc.mariadb',
    'kafka_topic', 'cdc.mariadb.goopay_legacy_maria.legacy_orders'
  ),
  'mariadb:goopay_legacy_maria:legacy_orders',
  'id',
  'BIGINT',
  'updated_at',
  'incremental',
  'debezium',
  FALSE,
  'draft',
  'manual',
  'draft',
  'Seed for multi_engine_unified L4 — flip provisioning_mode=auto via cms-api/fe to kick orchestrator.'
FROM conn
ON CONFLICT (object_code) DO NOTHING;
*/

COMMIT;
