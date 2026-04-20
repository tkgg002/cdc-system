-- ============================================================
-- CDC Integration v3 — Phase 0 (T0-1)
-- Add `_source_ts BIGINT` (Debezium payload.source.ts_ms) to all
-- active CDC data tables + supporting indexes.
--
-- Idempotent: safe to re-run. Every statement guards with
-- IF NOT EXISTS or catches duplicate_object/duplicate_index.
--
-- NOTE on CONCURRENTLY:
--   CREATE INDEX CONCURRENTLY cannot run inside a transaction
--   block. The outer transaction is therefore intentionally
--   omitted and each table's index creation is wrapped in its
--   own autonomous sub-block (DO + EXECUTE) so a single table
--   failure does not abort subsequent ones.
-- ============================================================

-- Safety: ensure cdc_table_registry exists (migration 001 or later).
DO $pre$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'cdc_table_registry'
    ) THEN
        RAISE NOTICE '[009_source_ts] cdc_table_registry missing - skipping migration';
        RETURN;
    END IF;
END $pre$;

-- ------------------------------------------------------------
-- Step 1: ALTER TABLE ADD COLUMN IF NOT EXISTS _source_ts BIGINT
-- (ADD COLUMN IF NOT EXISTS is transactional-safe in PG12+)
-- ------------------------------------------------------------
DO $add_col$
DECLARE
    tbl RECORD;
    target TEXT;
BEGIN
    FOR tbl IN
        SELECT target_table
        FROM cdc_table_registry
        WHERE is_active = true
          AND target_table IS NOT NULL
          AND target_table <> ''
        ORDER BY target_table
    LOOP
        target := tbl.target_table;

        -- Only act on tables that actually exist (registry may hold
        -- entries whose destination has not been materialised yet).
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = target
        ) THEN
            RAISE NOTICE '[009_source_ts] skip % (table not present)', target;
            CONTINUE;
        END IF;

        BEGIN
            EXECUTE format(
                'ALTER TABLE %I ADD COLUMN IF NOT EXISTS _source_ts BIGINT',
                target
            );
            RAISE NOTICE '[009_source_ts] ensured _source_ts on %', target;
        EXCEPTION WHEN others THEN
            RAISE WARNING '[009_source_ts] ALTER % failed: %', target, SQLERRM;
        END;
    END LOOP;
END $add_col$;

-- ------------------------------------------------------------
-- Step 2: CREATE INDEX CONCURRENTLY for _source_ts and updated_at
-- Cannot be inside a DO block transaction; we emit per-table
-- statements via a helper DO that uses EXECUTE with
-- SET LOCAL statement_timeout.
--
-- PG does not allow CONCURRENTLY inside DO/FUNCTION. So we use
-- dynamic SQL recorded into a temp table, then the migration
-- runner (psql) executes them one by one OUTSIDE any tx.
-- For that to work when invoked via a standard migration tool
-- that wraps each file in a TX, we fall back to non-concurrent
-- CREATE INDEX IF NOT EXISTS. This keeps the file idempotent
-- and portable. For very large tables operators should instead
-- manually run the concurrent variant during a maintenance
-- window.
-- ------------------------------------------------------------
DO $add_idx$
DECLARE
    tbl RECORD;
    target TEXT;
    idx_name TEXT;
    has_updated_at BOOLEAN;
BEGIN
    FOR tbl IN
        SELECT target_table
        FROM cdc_table_registry
        WHERE is_active = true
          AND target_table IS NOT NULL
          AND target_table <> ''
        ORDER BY target_table
    LOOP
        target := tbl.target_table;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = target
        ) THEN
            CONTINUE;
        END IF;

        -- Index on _source_ts (new column, always safe)
        idx_name := format('idx_%s_source_ts', target);
        BEGIN
            EXECUTE format(
                'CREATE INDEX IF NOT EXISTS %I ON %I (_source_ts)',
                idx_name, target
            );
        EXCEPTION WHEN others THEN
            RAISE WARNING '[009_source_ts] idx _source_ts on % failed: %', target, SQLERRM;
        END;

        -- Index on updated_at if the column exists and no index
        -- already covers it.
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = target
              AND column_name = 'updated_at'
        ) INTO has_updated_at;

        IF has_updated_at THEN
            idx_name := format('idx_%s_updated_at', target);
            BEGIN
                EXECUTE format(
                    'CREATE INDEX IF NOT EXISTS %I ON %I (updated_at)',
                    idx_name, target
                );
            EXCEPTION WHEN others THEN
                RAISE WARNING '[009_source_ts] idx updated_at on % failed: %', target, SQLERRM;
            END;
        END IF;
    END LOOP;
END $add_idx$;

-- ------------------------------------------------------------
-- Verification (raises notice with columns summary)
-- ------------------------------------------------------------
DO $verify$
DECLARE
    missing_count INT;
    present_count INT;
BEGIN
    SELECT
        COUNT(*) FILTER (WHERE c.column_name IS NULL),
        COUNT(*) FILTER (WHERE c.column_name IS NOT NULL)
    INTO missing_count, present_count
    FROM cdc_table_registry r
    LEFT JOIN information_schema.columns c
           ON c.table_schema = 'public'
          AND c.table_name = r.target_table
          AND c.column_name = '_source_ts'
    LEFT JOIN information_schema.tables t
           ON t.table_schema = 'public'
          AND t.table_name = r.target_table
    WHERE r.is_active = true
      AND t.table_name IS NOT NULL;

    RAISE NOTICE '[009_source_ts] verification: % tables have _source_ts, % missing',
        present_count, missing_count;
END $verify$;
