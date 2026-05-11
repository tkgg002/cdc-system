-- ============================================================
-- CDC Integration - v1.12: Declarative Partitioning Support
-- Monthly partitions by _synced_at for high-volume tables
-- ============================================================

BEGIN;

-- ============================================================
-- 1. Registry columns for partitioning config
-- ============================================================

ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS is_partitioned BOOLEAN DEFAULT false;
ALTER TABLE cdc_table_registry ADD COLUMN IF NOT EXISTS partition_key VARCHAR DEFAULT '_synced_at';

-- ============================================================
-- 2. Partitioned CDC table creation function
-- ============================================================

CREATE OR REPLACE FUNCTION create_cdc_table_partitioned(
    p_target_table VARCHAR
)
RETURNS VOID AS $$
DECLARE
    v_current_month VARCHAR;
    v_next_month VARCHAR;
    v_current_start DATE;
    v_next_start DATE;
    v_month_after DATE;
    v_seq_name VARCHAR;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = p_target_table
    ) THEN
        RAISE NOTICE 'Partitioned table % already exists, skipping', p_target_table;
        UPDATE cdc_table_registry SET is_table_created = TRUE, is_partitioned = TRUE WHERE target_table = p_target_table;
        RETURN;
    END IF;

    -- Sequence for BIGINT PK
    v_seq_name := 'seq_' || p_target_table || '_id';
    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I START WITH 1 INCREMENT BY 1', v_seq_name);

    -- Parent table (partitioned by RANGE on _synced_at)
    -- Note: PK must include partition key for Postgres requirement
    EXECUTE format(
        'CREATE TABLE %I (
            id BIGINT DEFAULT nextval(%L),
            source_id VARCHAR(200) NOT NULL,
            _raw_data JSONB NOT NULL,
            _source VARCHAR(20) NOT NULL DEFAULT ''airbyte'',
            _synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            _version BIGINT NOT NULL DEFAULT 1,
            _hash VARCHAR(64),
            _deleted BOOLEAN DEFAULT FALSE,
            _created_at TIMESTAMP DEFAULT NOW(),
            _updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (id, _synced_at),
            UNIQUE (source_id, _synced_at)
        ) PARTITION BY RANGE (_synced_at)',
        p_target_table,
        v_seq_name
    );

    -- Create partitions for current month and next month
    v_current_start := date_trunc('month', NOW())::DATE;
    v_next_start := (v_current_start + INTERVAL '1 month')::DATE;
    v_month_after := (v_current_start + INTERVAL '2 months')::DATE;

    v_current_month := to_char(v_current_start, 'YYYY_MM');
    v_next_month := to_char(v_next_start, 'YYYY_MM');

    -- Current month partition
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        p_target_table || '_' || v_current_month,
        p_target_table,
        v_current_start,
        v_next_start
    );

    -- Next month partition
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        p_target_table || '_' || v_next_month,
        p_target_table,
        v_next_start,
        v_month_after
    );

    -- Indexes on parent (auto-inherited by partitions)
    EXECUTE format('CREATE INDEX %I ON %I(_source)',
        'idx_' || p_target_table || '_source', p_target_table);
    EXECUTE format('CREATE INDEX %I ON %I USING GIN(_raw_data)',
        'idx_' || p_target_table || '_raw', p_target_table);

    UPDATE cdc_table_registry SET is_table_created = TRUE, is_partitioned = TRUE WHERE target_table = p_target_table;

    RAISE NOTICE 'Created partitioned CDC table: % (partitions: %_%, %_%)',
        p_target_table, p_target_table, v_current_month, p_target_table, v_next_month;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 3. Helper: ensure partition exists for a given month
-- Called by Worker periodic scheduler before month boundary
-- ============================================================

CREATE OR REPLACE FUNCTION ensure_cdc_partition(
    p_target_table VARCHAR,
    p_month DATE
)
RETURNS VOID AS $$
DECLARE
    v_partition_name VARCHAR;
    v_month_start DATE;
    v_month_end DATE;
BEGIN
    v_month_start := date_trunc('month', p_month)::DATE;
    v_month_end := (v_month_start + INTERVAL '1 month')::DATE;
    v_partition_name := p_target_table || '_' || to_char(v_month_start, 'YYYY_MM');

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = v_partition_name
    ) THEN
        RETURN; -- Already exists
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        v_partition_name,
        p_target_table,
        v_month_start,
        v_month_end
    );

    RAISE NOTICE 'Created partition: %', v_partition_name;
END;
$$ LANGUAGE plpgsql;

COMMIT;
