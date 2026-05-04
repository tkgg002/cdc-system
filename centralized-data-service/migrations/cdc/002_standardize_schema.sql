-- ============================================================
-- CDC Integration - Phase 1.6: Legacy Support & Standardization
-- ============================================================

BEGIN;

-- 1. Helper function to check if a column exists
CREATE OR REPLACE FUNCTION column_exists(p_table text, p_column text)
RETURNS boolean AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = p_table AND column_name = p_column
    );
END;
$$ LANGUAGE plpgsql;

-- 2. Function to standardize an existing table
CREATE OR REPLACE FUNCTION standardize_cdc_table(p_target_table VARCHAR)
RETURNS VOID AS $$
BEGIN
    -- Add metadata columns if missing
    IF NOT column_exists(p_target_table, '_raw_data') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _raw_data JSONB NOT NULL DEFAULT ''{}''::jsonb', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_source') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _source VARCHAR(20) NOT NULL DEFAULT ''airbyte''', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_synced_at') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _synced_at TIMESTAMP NOT NULL DEFAULT NOW()', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_version') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _version BIGINT NOT NULL DEFAULT 1', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_hash') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _hash VARCHAR(64)', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_deleted') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _deleted BOOLEAN DEFAULT FALSE', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_created_at') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _created_at TIMESTAMP DEFAULT NOW()', p_target_table);
    END IF;

    IF NOT column_exists(p_target_table, '_updated_at') THEN
        EXECUTE format('ALTER TABLE %I ADD COLUMN _updated_at TIMESTAMP DEFAULT NOW()', p_target_table);
    END IF;

    -- Ensure GIN index on _raw_data
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_' || p_target_table || '_raw' AND n.nspname = 'public'
    ) THEN
        EXECUTE format('CREATE INDEX %I ON %I USING GIN(_raw_data)', 'idx_' || p_target_table || '_raw', p_target_table);
    END IF;

    RAISE NOTICE 'Standardized CDC table: %', p_target_table;
END;
$$ LANGUAGE plpgsql;

-- 3. Redefine create_cdc_table to using standardization
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
        -- If table exists, standardize it instead of skipping
        PERFORM standardize_cdc_table(p_target_table);
        UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;
        RETURN;
    END IF;

    -- Normalize PK field name
    v_pk_field := CASE WHEN p_primary_key_field = '_id' THEN 'id' ELSE p_primary_key_field END;

    v_sql := format(
        'CREATE TABLE %I (
            %I %s PRIMARY KEY,
            _raw_data JSONB NOT NULL DEFAULT ''{}''::jsonb,
            _source VARCHAR(20) NOT NULL DEFAULT ''airbyte'',
            _synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            _version BIGINT NOT NULL DEFAULT 1,
            _hash VARCHAR(64),
            _deleted BOOLEAN DEFAULT FALSE,
            _created_at TIMESTAMP DEFAULT NOW(),
            _updated_at TIMESTAMP DEFAULT NOW()
        )',
        p_target_table, v_pk_field, p_primary_key_type
    );
    EXECUTE v_sql;

    -- Create standard indexes
    EXECUTE format('CREATE INDEX %I ON %I(_synced_at)', 'idx_' || p_target_table || '_synced', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I(_source)', 'idx_' || p_target_table || '_source', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I USING GIN(_raw_data)', 'idx_' || p_target_table || '_raw', p_target_table);

    UPDATE cdc_table_registry SET is_table_created = TRUE WHERE target_table = p_target_table;
    RAISE NOTICE 'Created CDC table: %', p_target_table;
END;
$$ LANGUAGE plpgsql;

COMMIT;
