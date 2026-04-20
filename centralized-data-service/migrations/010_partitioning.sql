-- ============================================================
-- CDC Integration v3 — Phase 0 (T0-3)
-- Partition `failed_sync_logs` (monthly) and `cdc_activity_log`
-- (daily) by created_at. Online migration pattern:
--   1. build new partitioned table (<name>_new)
--   2. copy rows from legacy non-partitioned table
--   3. swap names inside a single transaction
--   4. rebuild indexes required by the app
--
-- If the legacy table is already partitioned (PostgreSQL
-- relkind = 'p') the migration is a no-op for that table.
-- If the legacy table does not exist we simply CREATE the
-- partitioned table directly.
--
-- No pg_cron / pg_partman dependency. Partition retention is
-- handled by a worker goroutine (Phase 1 task). A TODO notice
-- is raised if neither extension is installed.
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- Detect pg_cron / pg_partman availability (non-fatal)
-- ------------------------------------------------------------
DO $extcheck$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_cron') THEN
        RAISE NOTICE '[010_partitioning] pg_cron extension not available — drop-old-partitions must be handled by worker goroutine';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_partman') THEN
        RAISE NOTICE '[010_partitioning] pg_partman extension not available — partition creation must be handled by worker goroutine';
    END IF;
END $extcheck$;

-- ============================================================
-- failed_sync_logs → partitioned by RANGE(created_at), monthly
-- ============================================================

DO $fsl$
DECLARE
    legacy_kind CHAR;
    legacy_exists BOOLEAN;
BEGIN
    SELECT c.relkind, TRUE INTO legacy_kind, legacy_exists
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'public';

    IF legacy_exists IS NULL THEN
        legacy_exists := FALSE;
    END IF;

    IF legacy_exists AND legacy_kind = 'p' THEN
        RAISE NOTICE '[010_partitioning] failed_sync_logs already partitioned — skip';
        RETURN;
    END IF;

    -- Build new partitioned parent
    CREATE TABLE IF NOT EXISTS failed_sync_logs_new (
        id              BIGSERIAL,
        target_table    VARCHAR(200) NOT NULL,
        source_table    VARCHAR(200),
        source_db       VARCHAR(200),
        record_id       VARCHAR(200),
        operation       VARCHAR(10),
        raw_json        JSONB,
        error_message   TEXT NOT NULL,
        error_type      VARCHAR(50),
        kafka_topic     VARCHAR(200),
        kafka_partition INT,
        kafka_offset    BIGINT,
        retry_count     INT DEFAULT 0,
        max_retries     INT DEFAULT 3,
        status          VARCHAR(20) DEFAULT 'failed',
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_retry_at   TIMESTAMPTZ,
        resolved_at     TIMESTAMPTZ,
        resolved_by     VARCHAR(100),
        PRIMARY KEY (id, created_at)
    ) PARTITION BY RANGE (created_at);

    -- Indexes on parent (propagate to partitions)
    CREATE INDEX IF NOT EXISTS idx_fsl_new_table     ON failed_sync_logs_new (target_table);
    CREATE INDEX IF NOT EXISTS idx_fsl_new_status    ON failed_sync_logs_new (status);
    CREATE INDEX IF NOT EXISTS idx_fsl_new_created   ON failed_sync_logs_new (created_at DESC, id);

    -- Create partitions for current and next 3 months
    -- Using date_trunc to align boundaries to month start.
    PERFORM 1;
END $fsl$;

-- Create monthly partitions (current + next 3) for failed_sync_logs_new
DO $fsl_parts$
DECLARE
    i INT;
    start_d DATE;
    end_d   DATE;
    part_name TEXT;
BEGIN
    -- Only run if the parent exists (created above)
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'failed_sync_logs_new' AND n.nspname = 'public'
    ) THEN
        RETURN;
    END IF;

    FOR i IN 0..3 LOOP
        start_d := date_trunc('month', NOW()::DATE + make_interval(months => i))::DATE;
        end_d   := (start_d + INTERVAL '1 month')::DATE;
        part_name := format('failed_sync_logs_y%sm%s',
            to_char(start_d, 'YYYY'),
            to_char(start_d, 'MM'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF failed_sync_logs_new
             FOR VALUES FROM (%L) TO (%L)',
            part_name, start_d, end_d
        );
    END LOOP;

    -- DEFAULT partition catches anything outside the prepared range
    EXECUTE 'CREATE TABLE IF NOT EXISTS failed_sync_logs_default PARTITION OF failed_sync_logs_new DEFAULT';
END $fsl_parts$;

-- Copy data from legacy → new and swap names (only if legacy exists
-- and isn't already partitioned).
DO $fsl_swap$
DECLARE
    legacy_kind CHAR;
    row_count BIGINT;
BEGIN
    SELECT c.relkind INTO legacy_kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'failed_sync_logs' AND n.nspname = 'public';

    IF legacy_kind IS NULL THEN
        -- No legacy table. Just rename _new → target.
        ALTER TABLE failed_sync_logs_new RENAME TO failed_sync_logs;
        RAISE NOTICE '[010_partitioning] failed_sync_logs created as partitioned (no legacy data)';
        RETURN;
    END IF;

    IF legacy_kind = 'p' THEN
        -- Already partitioned, drop the scaffolding _new table.
        DROP TABLE IF EXISTS failed_sync_logs_new CASCADE;
        RETURN;
    END IF;

    -- Copy data
    EXECUTE 'INSERT INTO failed_sync_logs_new
             (id, target_table, source_table, source_db, record_id, operation,
              raw_json, error_message, error_type, kafka_topic, kafka_partition,
              kafka_offset, retry_count, max_retries, status, created_at,
              last_retry_at, resolved_at, resolved_by)
             SELECT id, target_table, source_table, source_db, record_id, operation,
                    raw_json, error_message, error_type, kafka_topic, kafka_partition,
                    kafka_offset, retry_count, max_retries, status,
                    COALESCE(created_at, NOW())::TIMESTAMPTZ,
                    last_retry_at::TIMESTAMPTZ, resolved_at::TIMESTAMPTZ, resolved_by
             FROM failed_sync_logs';
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '[010_partitioning] copied % rows into failed_sync_logs_new', row_count;

    -- Rename swap
    ALTER TABLE failed_sync_logs        RENAME TO failed_sync_logs_legacy;
    ALTER TABLE failed_sync_logs_new    RENAME TO failed_sync_logs;

    -- Sync sequence so new inserts don't collide
    PERFORM setval(
        pg_get_serial_sequence('failed_sync_logs', 'id'),
        COALESCE((SELECT MAX(id) FROM failed_sync_logs), 1),
        true
    );

    RAISE NOTICE '[010_partitioning] swapped: legacy kept as failed_sync_logs_legacy';
END $fsl_swap$;

-- ============================================================
-- cdc_activity_log → partitioned by RANGE(created_at), daily
-- 7 daily partitions pre-created
-- ============================================================

DO $act$
DECLARE
    legacy_kind CHAR;
    legacy_exists BOOLEAN;
BEGIN
    SELECT c.relkind, TRUE INTO legacy_kind, legacy_exists
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'cdc_activity_log' AND n.nspname = 'public';

    IF legacy_exists IS NULL THEN
        legacy_exists := FALSE;
    END IF;

    IF legacy_exists AND legacy_kind = 'p' THEN
        RAISE NOTICE '[010_partitioning] cdc_activity_log already partitioned — skip';
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS cdc_activity_log_new (
        id            BIGSERIAL,
        operation     VARCHAR(50) NOT NULL,
        target_table  VARCHAR(200),
        status        VARCHAR(20) NOT NULL DEFAULT 'running',
        rows_affected BIGINT DEFAULT 0,
        duration_ms   INT,
        details       JSONB,
        error_message TEXT,
        triggered_by  VARCHAR(50) DEFAULT 'scheduler',
        started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at  TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (id, created_at)
    ) PARTITION BY RANGE (created_at);

    CREATE INDEX IF NOT EXISTS idx_act_new_op      ON cdc_activity_log_new (operation);
    CREATE INDEX IF NOT EXISTS idx_act_new_target  ON cdc_activity_log_new (target_table);
    CREATE INDEX IF NOT EXISTS idx_act_new_status  ON cdc_activity_log_new (status);
    CREATE INDEX IF NOT EXISTS idx_act_new_started ON cdc_activity_log_new (started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_act_new_created ON cdc_activity_log_new (created_at DESC, id);
END $act$;

DO $act_parts$
DECLARE
    i INT;
    start_d DATE;
    end_d   DATE;
    part_name TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'cdc_activity_log_new' AND n.nspname = 'public'
    ) THEN
        RETURN;
    END IF;

    -- Current day + next 6 = 7 daily partitions
    FOR i IN 0..6 LOOP
        start_d := (NOW()::DATE + make_interval(days => i));
        end_d   := (start_d + INTERVAL '1 day')::DATE;
        part_name := format('cdc_activity_log_%s', to_char(start_d, 'YYYYMMDD'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF cdc_activity_log_new
             FOR VALUES FROM (%L) TO (%L)',
            part_name, start_d, end_d
        );
    END LOOP;

    EXECUTE 'CREATE TABLE IF NOT EXISTS cdc_activity_log_default PARTITION OF cdc_activity_log_new DEFAULT';
END $act_parts$;

DO $act_swap$
DECLARE
    legacy_kind CHAR;
    row_count BIGINT;
BEGIN
    SELECT c.relkind INTO legacy_kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'cdc_activity_log' AND n.nspname = 'public';

    IF legacy_kind IS NULL THEN
        ALTER TABLE cdc_activity_log_new RENAME TO cdc_activity_log;
        RAISE NOTICE '[010_partitioning] cdc_activity_log created as partitioned (no legacy data)';
        RETURN;
    END IF;

    IF legacy_kind = 'p' THEN
        DROP TABLE IF EXISTS cdc_activity_log_new CASCADE;
        RETURN;
    END IF;

    EXECUTE 'INSERT INTO cdc_activity_log_new
             (id, operation, target_table, status, rows_affected, duration_ms,
              details, error_message, triggered_by, started_at, completed_at, created_at)
             SELECT id, operation, target_table, status, rows_affected, duration_ms,
                    details, error_message, triggered_by,
                    started_at::TIMESTAMPTZ,
                    completed_at::TIMESTAMPTZ,
                    COALESCE(created_at, NOW())::TIMESTAMPTZ
             FROM cdc_activity_log';
    GET DIAGNOSTICS row_count = ROW_COUNT;
    RAISE NOTICE '[010_partitioning] copied % rows into cdc_activity_log_new', row_count;

    ALTER TABLE cdc_activity_log     RENAME TO cdc_activity_log_legacy;
    ALTER TABLE cdc_activity_log_new RENAME TO cdc_activity_log;

    PERFORM setval(
        pg_get_serial_sequence('cdc_activity_log', 'id'),
        COALESCE((SELECT MAX(id) FROM cdc_activity_log), 1),
        true
    );

    RAISE NOTICE '[010_partitioning] swapped: legacy kept as cdc_activity_log_legacy';
END $act_swap$;

-- ------------------------------------------------------------
-- TODO: retention jobs
--   failed_sync_logs → drop monthly partitions older than 90 days
--   cdc_activity_log → drop daily partitions older than 14 days
-- Handled by worker goroutine (see Phase 1 task T1-retention).
-- ------------------------------------------------------------
COMMIT;
