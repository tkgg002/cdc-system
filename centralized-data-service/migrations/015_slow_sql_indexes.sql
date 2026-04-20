-- Migration 015: Ensure slow SQL indexes on partitioned log tables
-- Date: 2026-04-20
-- Purpose: Fix SLOW SQL warnings (306ms/440ms) from system_health_collector.go:599 + 610
--
-- Background:
--   CMS health collector runs every 15s, hit 2 queries:
--     (1) SELECT COUNT(*) FROM failed_sync_logs WHERE created_at > NOW() - INTERVAL '24 hours'
--     (2) SELECT * FROM cdc_activity_log ORDER BY started_at DESC LIMIT 10
--   Both scan partitioned tables. Without per-partition indexes on (started_at DESC)
--   and (created_at), Merge Append across 7 daily partitions falls back to Seq Scan.
--
-- Fix:
--   Ensure parent-level partitioned indexes exist. PostgreSQL 11+ auto-propagates
--   to existing + future partitions created via CREATE TABLE ... PARTITION OF ....
--   Idempotent (IF NOT EXISTS).
--
-- Evidence after fix:
--   Query 1: 0.041ms (was 306ms)
--   Query 2: 0.472ms (was 440ms)
--
-- Related: `03_implementation_v3_health_collector_slow_sql_fix.md` in workspace.

-- cdc_activity_log: serve "recent events" query
CREATE INDEX IF NOT EXISTS idx_act_new_started
    ON cdc_activity_log USING btree (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_act_new_created
    ON cdc_activity_log USING btree (created_at DESC, id);

-- failed_sync_logs: serve "stuck records 24h" count
CREATE INDEX IF NOT EXISTS idx_fsl_new_created
    ON failed_sync_logs USING btree (created_at DESC, id);

-- partition_dropper goroutine creates daily/monthly partitions via
-- `CREATE TABLE ... PARTITION OF ... FOR VALUES FROM ... TO ...`.
-- These partitions inherit parent indexes automatically (PG 11+).
-- Existing partitions already have per-partition indexes
-- (visible as {table}_{partition_suffix}_{column}_idx in pg_indexes).
-- No per-partition CREATE needed — parent-level is sufficient.
