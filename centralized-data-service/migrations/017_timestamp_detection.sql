-- Migration 017: Systematic timestamp-field detection + structured error propagation.
-- Date: 2026-04-20
-- ADR: agent/memory/workspaces/feature-cdc-integration/04_decisions_recon_systematic_v4.md §2.1 §2.3 §2.4 §2.7
-- Author: Muscle (claude-opus-4-7)
--
-- Purpose:
--   (1) Auto-detect Mongo timestamp field per collection (O(N) human error → O(1)).
--   (2) Structured error codes propagate source query failures to FE instead of
--       silent source_count=0 masking as "no data".
--   (3) Daily full-count aggregate per table for Total Source / Total Dest UX.
--
-- Anti-pattern rejected: per-table `UPDATE timestamp_field='X' WHERE table='Y'`
-- manual seeding scales as O(N-tables × human-effort). Auto-detect cuts that
-- to a single background sampling pass (see service/timestamp_detector.go).
--
-- Safety:
--   - All additions are `ADD COLUMN IF NOT EXISTS` — idempotent re-runs.
--   - `source_count` NOT NULL → NULLABLE: lets recon reports distinguish
--     "query failed" (NULL) from "0 records" (0). Legacy rows with 0 remain
--     valid; new rows may be NULL when query fails.
--   - `timestamp_field_candidates` JSONB defaults to the four common names
--     observed in audit 2026-04-20 — safe to override per-table later.
--
-- Rollback:
--   ALTER TABLE cdc_table_registry DROP COLUMN timestamp_field_candidates;
--   ALTER TABLE cdc_table_registry DROP COLUMN timestamp_field_detected_at;
--   ALTER TABLE cdc_table_registry DROP COLUMN timestamp_field_source;
--   ALTER TABLE cdc_table_registry DROP COLUMN timestamp_field_confidence;
--   ALTER TABLE cdc_table_registry DROP COLUMN full_source_count;
--   ALTER TABLE cdc_table_registry DROP COLUMN full_dest_count;
--   ALTER TABLE cdc_table_registry DROP COLUMN full_count_at;
--   ALTER TABLE cdc_reconciliation_report DROP COLUMN error_code;
--   ALTER TABLE cdc_reconciliation_report ALTER COLUMN source_count SET NOT NULL,
--                                          ALTER COLUMN source_count SET DEFAULT 0;

-- ============================================================
-- 1. cdc_table_registry — timestamp auto-detect metadata
-- ============================================================

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS timestamp_field_candidates JSONB
        DEFAULT '["updated_at","updatedAt","created_at","createdAt"]'::jsonb;

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS timestamp_field_detected_at TIMESTAMPTZ;

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS timestamp_field_source TEXT DEFAULT 'auto';

-- Enforce enum via CHECK so admin UI/API cannot accidentally store garbage.
-- Using DO block for idempotency — CHECK adds cannot use IF NOT EXISTS.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'cdc_table_registry_timestamp_field_source_chk'
    ) THEN
        ALTER TABLE cdc_table_registry
            ADD CONSTRAINT cdc_table_registry_timestamp_field_source_chk
            CHECK (timestamp_field_source IN ('auto','admin_override'));
    END IF;
END$$;

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS timestamp_field_confidence TEXT;

COMMENT ON COLUMN cdc_table_registry.timestamp_field_candidates IS
    'Ordered list of candidate Mongo field names to probe during auto-detect. Default covers snake_case + camelCase of updated_at / created_at.';
COMMENT ON COLUMN cdc_table_registry.timestamp_field_detected_at IS
    'Last successful auto-detect run. NULL = never detected (new row or pre-migration).';
COMMENT ON COLUMN cdc_table_registry.timestamp_field_source IS
    'auto = field value managed by TimestampDetector; admin_override = pinned by operator, detector must NOT overwrite.';
COMMENT ON COLUMN cdc_table_registry.timestamp_field_confidence IS
    'high (>=80% coverage) | medium (30-80%) | low (<30% → ObjectID fallback).';

-- ============================================================
-- 2. cdc_table_registry — daily full-count aggregate (§2.7)
-- ============================================================

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS full_source_count BIGINT;

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS full_dest_count BIGINT;

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS full_count_at TIMESTAMPTZ;

COMMENT ON COLUMN cdc_table_registry.full_source_count IS
    'Unfiltered Mongo EstimatedDocumentCount (fast, per-collection stats) captured by daily full-count aggregator. NULL = never run.';
COMMENT ON COLUMN cdc_table_registry.full_dest_count IS
    'Unfiltered PG COUNT(*) (or pg_class.reltuples fallback for tables >10M rows). NULL = never run.';
COMMENT ON COLUMN cdc_table_registry.full_count_at IS
    'Timestamp of the most recent full-count aggregator pass. Drift alerts should compare (full_source_count, full_dest_count) rather than window counts for absolute-truth decisions.';

-- ============================================================
-- 3. cdc_reconciliation_report — structured error code + NULL source_count
-- ============================================================

ALTER TABLE cdc_reconciliation_report
    ADD COLUMN IF NOT EXISTS error_code TEXT;

COMMENT ON COLUMN cdc_reconciliation_report.error_code IS
    'Structured error category: SRC_TIMEOUT | SRC_CONNECTION | SRC_FIELD_MISSING | SRC_EMPTY | DST_TIMEOUT | DST_MISSING_COLUMN | CIRCUIT_OPEN | AUTH_ERROR | UNKNOWN. Populated by recon_core when Status=error; NULL for successful runs.';

-- Allow NULL so we can distinguish "query failed → NULL" from "0 records → 0".
-- Keep default 0 for back-compat with legacy insert sites that still pass 0.
ALTER TABLE cdc_reconciliation_report
    ALTER COLUMN source_count DROP NOT NULL;
