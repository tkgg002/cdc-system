-- ============================================================
-- CDC Integration - v1.12: Sonyflake ID + Dual-PK Strategy
-- New CDC tables use BIGINT as PK + source_id for dedup
-- SQL bridge uses per-table SEQUENCE for ID generation
-- Go Worker will replace with Sonyflake when using pgx.Batch
-- Existing tables are NOT altered (backward compatible)
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Update create_cdc_table() — Sonyflake-compatible schema
-- ============================================================

CREATE OR REPLACE FUNCTION create_cdc_table(
    p_target_table VARCHAR,
    p_primary_key_field VARCHAR DEFAULT 'id',
    p_primary_key_type VARCHAR DEFAULT 'BIGINT'
)
RETURNS VOID AS $$
DECLARE
    v_sql TEXT;
    v_seq_name VARCHAR;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = p_target_table
    ) THEN
        RAISE NOTICE 'Table % already exists, skipping', p_target_table;
        UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;
        RETURN;
    END IF;

    -- Create per-table sequence for BIGINT PK (temporary until Go Sonyflake takes over)
    v_seq_name := 'seq_' || p_target_table || '_id';
    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I START WITH 1 INCREMENT BY 1', v_seq_name);

    v_sql := format(
        'CREATE TABLE %I (
            id BIGINT PRIMARY KEY DEFAULT nextval(%L),
            source_id VARCHAR(200) NOT NULL,
            _raw_data JSONB NOT NULL,
            _source VARCHAR(20) NOT NULL DEFAULT ''airbyte'',
            _synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            _version BIGINT NOT NULL DEFAULT 1,
            _hash VARCHAR(64),
            _deleted BOOLEAN DEFAULT FALSE,
            _created_at TIMESTAMP DEFAULT NOW(),
            _updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(source_id)
        )',
        p_target_table,
        v_seq_name
    );
    EXECUTE v_sql;

    -- Standard indexes
    EXECUTE format('CREATE INDEX %I ON %I(_synced_at)',
        'idx_' || p_target_table || '_synced', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I(_source)',
        'idx_' || p_target_table || '_source', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I USING GIN(_raw_data)',
        'idx_' || p_target_table || '_raw', p_target_table);

    UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;

    RAISE NOTICE 'Created CDC table (v1.12 schema): %', p_target_table;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 2. Update registry default primary_key_type
-- ============================================================

ALTER TABLE cdc_table_registry
    ALTER COLUMN primary_key_type SET DEFAULT 'BIGINT';

COMMIT;
