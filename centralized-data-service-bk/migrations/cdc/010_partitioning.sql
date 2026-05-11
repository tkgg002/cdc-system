-- ============================================================
-- 010_partitioning — Phase 01 Split E2E REWRITE (architect T-B3)
--
-- Original 010 (V1) created partitioned tables in `public` then
-- 037 moved parents to `cdc_system`, leaving child partitions
-- orphaned in public (root cause Phase 39 partition partition
-- residue). After fresh wipe + multi-PG split, this migration
-- creates partitioned tables DIRECTLY in `cdc_system` so 037 has
-- nothing to move and 044 has no orphans to clean.
--
-- Tables:
--   cdc_system.failed_sync_logs  → monthly partitions (current + 3 months) + DEFAULT
--   cdc_system.cdc_activity_log  → daily partitions (current + 6 days)     + DEFAULT
-- ============================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS cdc_system;

-- ------------------------------------------------------------
-- failed_sync_logs (monthly RANGE)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cdc_system.failed_sync_logs (
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

CREATE INDEX IF NOT EXISTS idx_fsl_target  ON cdc_system.failed_sync_logs (target_table);
CREATE INDEX IF NOT EXISTS idx_fsl_status  ON cdc_system.failed_sync_logs (status);
CREATE INDEX IF NOT EXISTS idx_fsl_created ON cdc_system.failed_sync_logs (created_at DESC, id);

DO $fsl_parts$
DECLARE
    i INT;
    start_d DATE;
    end_d   DATE;
    part_name TEXT;
BEGIN
    FOR i IN 0..3 LOOP
        start_d := date_trunc('month', NOW()::DATE + make_interval(months => i))::DATE;
        end_d   := (start_d + INTERVAL '1 month')::DATE;
        part_name := format('failed_sync_logs_y%sm%s',
            to_char(start_d, 'YYYY'),
            to_char(start_d, 'MM'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS cdc_system.%I PARTITION OF cdc_system.failed_sync_logs
             FOR VALUES FROM (%L) TO (%L)',
            part_name, start_d, end_d
        );
    END LOOP;

    EXECUTE 'CREATE TABLE IF NOT EXISTS cdc_system.failed_sync_logs_default
             PARTITION OF cdc_system.failed_sync_logs DEFAULT';
END $fsl_parts$;

-- ------------------------------------------------------------
-- cdc_activity_log (daily RANGE, 7 prepared partitions)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cdc_system.cdc_activity_log (
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

CREATE INDEX IF NOT EXISTS idx_act_op      ON cdc_system.cdc_activity_log (operation);
CREATE INDEX IF NOT EXISTS idx_act_target  ON cdc_system.cdc_activity_log (target_table);
CREATE INDEX IF NOT EXISTS idx_act_status  ON cdc_system.cdc_activity_log (status);
CREATE INDEX IF NOT EXISTS idx_act_started ON cdc_system.cdc_activity_log (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_act_created ON cdc_system.cdc_activity_log (created_at DESC, id);

DO $act_parts$
DECLARE
    i INT;
    start_d DATE;
    end_d   DATE;
    part_name TEXT;
BEGIN
    FOR i IN 0..6 LOOP
        start_d := (NOW()::DATE + make_interval(days => i))::DATE;
        end_d   := (start_d + INTERVAL '1 day')::DATE;
        part_name := format('cdc_activity_log_%s', to_char(start_d, 'YYYYMMDD'));

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS cdc_system.%I PARTITION OF cdc_system.cdc_activity_log
             FOR VALUES FROM (%L) TO (%L)',
            part_name, start_d, end_d
        );
    END LOOP;

    EXECUTE 'CREATE TABLE IF NOT EXISTS cdc_system.cdc_activity_log_default
             PARTITION OF cdc_system.cdc_activity_log DEFAULT';
END $act_parts$;

COMMIT;
