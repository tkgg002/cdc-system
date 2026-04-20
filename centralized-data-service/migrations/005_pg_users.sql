-- ============================================================
-- CDC Integration - v1.12: PostgreSQL User Separation
-- Run as superuser (postgres or user with CREATEROLE)
-- ============================================================

-- 1. CDC Worker: read/write on CDC tables + registry + mapping rules
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_worker') THEN
    CREATE ROLE cdc_worker WITH LOGIN PASSWORD 'cdc_worker_2026';
  END IF;
END $$;

GRANT CONNECT ON DATABASE goopay_dw TO cdc_worker;
GRANT USAGE ON SCHEMA public TO cdc_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO cdc_worker;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO cdc_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO cdc_worker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO cdc_worker;
-- Worker needs to execute functions (create_cdc_table, ensure_cdc_partition)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO cdc_worker;

-- 2. CMS Service: read/write registry + mapping + schema_changes, read CDC tables
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cms_service') THEN
    CREATE ROLE cms_service WITH LOGIN PASSWORD 'cms_service_2026';
  END IF;
END $$;

GRANT CONNECT ON DATABASE goopay_dw TO cms_service;
GRANT USAGE ON SCHEMA public TO cms_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON cdc_table_registry TO cms_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON cdc_mapping_rules TO cms_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON pending_fields TO cms_service;
GRANT SELECT, INSERT, UPDATE, DELETE ON schema_changes_log TO cms_service;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO cms_service;
-- CMS needs SELECT on CDC tables for transform-status and reconciliation
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cms_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cms_service;
-- CMS needs to execute functions for standardize/discover relay
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO cms_service;

-- 3. Readonly: reports and dashboards only
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'cdc_readonly') THEN
    CREATE ROLE cdc_readonly WITH LOGIN PASSWORD 'readonly_2026';
  END IF;
END $$;

GRANT CONNECT ON DATABASE goopay_dw TO cdc_readonly;
GRANT USAGE ON SCHEMA public TO cdc_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_readonly;
