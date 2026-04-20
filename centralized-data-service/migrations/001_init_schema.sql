-- ============================================================
-- CDC Integration - Phase 1: Initial Schema
-- Scale: ~30 source databases, ~200 tables/collections
-- ============================================================

BEGIN;

-- ============================================================
-- 1. TABLE REGISTRY
-- ============================================================

CREATE TABLE IF NOT EXISTS cdc_table_registry (
    id SERIAL PRIMARY KEY,

    -- Source info
    source_db VARCHAR(100) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    source_table VARCHAR(200) NOT NULL,
    target_table VARCHAR(200) NOT NULL,

    -- Sync config
    sync_engine VARCHAR(20) NOT NULL DEFAULT 'airbyte',
    sync_interval VARCHAR(20) DEFAULT '1h',
    priority VARCHAR(10) DEFAULT 'normal',

    -- Primary key config
    primary_key_field VARCHAR(100) DEFAULT 'id',
    primary_key_type VARCHAR(50) DEFAULT 'VARCHAR(36)',

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_table_created BOOLEAN DEFAULT FALSE,

    -- Airbyte integration
    airbyte_connection_id VARCHAR(100),
    airbyte_source_id VARCHAR(100),

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    notes TEXT,

    UNIQUE(source_db, source_table),
    CONSTRAINT ctr_check_source_type CHECK (source_type IN ('mongodb', 'mysql', 'postgresql')),
    CONSTRAINT ctr_check_sync_engine CHECK (sync_engine IN ('airbyte', 'debezium', 'both')),
    CONSTRAINT ctr_check_priority CHECK (priority IN ('critical', 'high', 'normal', 'low'))
);

CREATE INDEX IF NOT EXISTS idx_registry_source_db ON cdc_table_registry(source_db);
CREATE INDEX IF NOT EXISTS idx_registry_sync_engine ON cdc_table_registry(sync_engine);
CREATE INDEX IF NOT EXISTS idx_registry_priority ON cdc_table_registry(priority);
CREATE INDEX IF NOT EXISTS idx_registry_active ON cdc_table_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_registry_target ON cdc_table_registry(target_table);

-- ============================================================
-- 2. CDC MAPPING RULES
-- ============================================================

CREATE TABLE IF NOT EXISTS cdc_mapping_rules (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(200) NOT NULL,
    source_field VARCHAR(100) NOT NULL,
    target_column VARCHAR(100) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    is_enriched BOOLEAN DEFAULT FALSE,
    is_nullable BOOLEAN DEFAULT TRUE,
    default_value TEXT,
    enrichment_function VARCHAR(100),
    enrichment_params JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    updated_by VARCHAR(100),
    notes TEXT,
    UNIQUE(source_table, source_field)
);

CREATE INDEX IF NOT EXISTS idx_mapping_rules_table ON cdc_mapping_rules(source_table);
CREATE INDEX IF NOT EXISTS idx_mapping_rules_active ON cdc_mapping_rules(is_active);

-- ============================================================
-- 3. PENDING FIELDS (Schema Drift Detection)
-- ============================================================

CREATE TABLE IF NOT EXISTS pending_fields (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(200) NOT NULL,
    source_db VARCHAR(100),
    field_name VARCHAR(100) NOT NULL,
    sample_value TEXT,
    sample_values_json JSONB,
    suggested_type VARCHAR(50) NOT NULL,
    final_type VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    detected_at TIMESTAMP DEFAULT NOW(),
    reviewed_at TIMESTAMP,
    approved_at TIMESTAMP,
    applied_at TIMESTAMP,
    reviewed_by VARCHAR(100),
    approval_notes TEXT,
    rejection_reason TEXT,
    target_column_name VARCHAR(100),
    detection_count INTEGER DEFAULT 1,
    UNIQUE(table_name, field_name),
    CONSTRAINT pf_check_status CHECK (status IN ('pending', 'approved', 'rejected', 'applied'))
);

CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_fields(status);
CREATE INDEX IF NOT EXISTS idx_pending_table ON pending_fields(table_name);
CREATE INDEX IF NOT EXISTS idx_pending_source_db ON pending_fields(source_db);
CREATE INDEX IF NOT EXISTS idx_pending_detected ON pending_fields(detected_at DESC);

-- ============================================================
-- 4. SCHEMA CHANGES LOG (Audit Trail)
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_changes_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(200) NOT NULL,
    source_db VARCHAR(100),
    change_type VARCHAR(50) NOT NULL,
    field_name VARCHAR(100),
    old_definition TEXT,
    new_definition TEXT,
    sql_executed TEXT NOT NULL,
    execution_duration_ms INTEGER,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    pending_field_id INTEGER REFERENCES pending_fields(id),
    executed_by VARCHAR(100) NOT NULL,
    executed_at TIMESTAMP DEFAULT NOW(),
    rollback_sql TEXT,
    airbyte_source_id VARCHAR(100),
    airbyte_refresh_triggered BOOLEAN DEFAULT FALSE,
    airbyte_refresh_status VARCHAR(50),
    CONSTRAINT scl_check_status CHECK (status IN ('pending', 'executing', 'success', 'failed', 'rolled_back'))
);

CREATE INDEX IF NOT EXISTS idx_schema_log_table ON schema_changes_log(table_name);
CREATE INDEX IF NOT EXISTS idx_schema_log_status ON schema_changes_log(status);
CREATE INDEX IF NOT EXISTS idx_schema_log_executed ON schema_changes_log(executed_at DESC);

-- ============================================================
-- 5. DYNAMIC TABLE CREATION FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION create_cdc_table(
    p_target_table VARCHAR,
    p_primary_key_field VARCHAR DEFAULT 'id',
    p_primary_key_type VARCHAR DEFAULT 'VARCHAR(36)'
)
RETURNS VOID AS $$
DECLARE
    v_sql TEXT;
    v_pk_field VARCHAR;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = p_target_table
    ) THEN
        RAISE NOTICE 'Table % already exists, skipping', p_target_table;
        UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;
        RETURN;
    END IF;

    -- Normalize PK field name: MongoDB _id → id in PostgreSQL
    v_pk_field := CASE WHEN p_primary_key_field = '_id' THEN 'id' ELSE p_primary_key_field END;

    v_sql := format(
        'CREATE TABLE %I (
            %I %s PRIMARY KEY,
            _raw_data JSONB NOT NULL,
            _source VARCHAR(20) NOT NULL DEFAULT ''airbyte'',
            _synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            _version BIGINT NOT NULL DEFAULT 1,
            _hash VARCHAR(64),
            _deleted BOOLEAN DEFAULT FALSE,
            _created_at TIMESTAMP DEFAULT NOW(),
            _updated_at TIMESTAMP DEFAULT NOW()
        )',
        p_target_table,
        v_pk_field,
        p_primary_key_type
    );
    EXECUTE v_sql;

    -- Create standard indexes
    EXECUTE format('CREATE INDEX %I ON %I(_synced_at)',
        'idx_' || p_target_table || '_synced', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I(_source)',
        'idx_' || p_target_table || '_source', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I USING GIN(_raw_data)',
        'idx_' || p_target_table || '_raw', p_target_table);

    UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;

    RAISE NOTICE 'Created CDC table: %', p_target_table;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 6. BATCH TABLE CREATION FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION create_all_pending_cdc_tables()
RETURNS INTEGER AS $$
DECLARE
    v_record RECORD;
    v_count INTEGER := 0;
BEGIN
    FOR v_record IN
        SELECT target_table, primary_key_field, primary_key_type
        FROM cdc_table_registry
        WHERE is_active = TRUE AND is_table_created = FALSE
    LOOP
        PERFORM create_cdc_table(v_record.target_table, v_record.primary_key_field, v_record.primary_key_type);
        v_count := v_count + 1;
    END LOOP;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 7. SEED DATA — Pilot tables
-- ============================================================

INSERT INTO cdc_table_registry
    (source_db, source_type, source_table, target_table, sync_engine, sync_interval, priority, primary_key_field, primary_key_type)
VALUES
    ('goopay_wallet', 'mongodb', 'wallet_transactions', 'wallet_transactions', 'airbyte', '15m', 'critical', '_id', 'VARCHAR(24)'),
    ('goopay_wallet', 'mongodb', 'wallets', 'wallets', 'airbyte', '1h', 'high', '_id', 'VARCHAR(24)'),
    ('goopay_payment', 'mongodb', 'payments', 'payments', 'airbyte', '15m', 'critical', '_id', 'VARCHAR(24)'),
    ('goopay_payment', 'mongodb', 'refunds', 'refunds', 'airbyte', '1h', 'high', '_id', 'VARCHAR(24)'),
    ('goopay_order', 'mongodb', 'orders', 'orders', 'airbyte', '15m', 'critical', '_id', 'VARCHAR(24)'),
    ('goopay_order', 'mongodb', 'order_items', 'order_items', 'airbyte', '1h', 'normal', '_id', 'VARCHAR(24)'),
    ('goopay_main', 'mongodb', 'users', 'users', 'airbyte', '1h', 'high', '_id', 'VARCHAR(24)'),
    ('goopay_main', 'mongodb', 'merchants', 'merchants', 'airbyte', '1h', 'normal', '_id', 'VARCHAR(24)'),
    ('goopay_legacy', 'mysql', 'legacy_payments', 'legacy_payments', 'airbyte', '1h', 'normal', 'id', 'BIGINT'),
    ('goopay_legacy', 'mysql', 'legacy_refunds', 'legacy_refunds', 'airbyte', '4h', 'low', 'id', 'BIGINT')
ON CONFLICT (source_db, source_table) DO NOTHING;

-- Auto-create CDC tables for seed data
SELECT create_all_pending_cdc_tables();

COMMIT;
