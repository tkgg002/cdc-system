-- ============================================================
-- Track D Hardening (post-P4) — Model-only schema drift patches.
--
-- Sweep follow-up after migration 045 (DLQ columns). Comparing
-- gorm:"column:X" tags in internal/model/*.go against actual DB
-- columns surfaced 6 fields the Go code references but no migration
-- ever creates:
--
--   cdc_system.cdc_mapping_rules
--     - rule_type    : INSERT/SELECT in command_handler.go (728,967)
--                      and scan_service.go (90); model default 'mapping'
--
--   cdc_system.cdc_table_registry
--     - source_url    : SELECT in recon_core.go (397,471,570,588,695)
--     - sync_status   : UPDATE in recon_core.go (841,844,847)
--     - recon_drift   : UPDATE in recon_core.go (842,845)
--     - last_recon_at : UPDATE in recon_core.go (838)
--     - last_bridge_at: model-only, kept for forward compat
--
-- These weren't visible at boot because every callsite uses an
-- explicit column list (Select / UPDATE SET ...) — the failure mode
-- is a 42703 the moment any new caller does .Find()/.First() on the
-- full struct, or a Scan that touches the missing column. Patching
-- preemptively per architect "phải đi tiếp các caí còn lại" ruling.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS only. Defaults match struct
-- tag defaults so existing rows hydrate the same value the model
-- would have written.
-- ============================================================

BEGIN;

-- 1. cdc_mapping_rules.rule_type ------------------------------------------------
DO $rule_type$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'cdc_system' AND c.relname = 'cdc_mapping_rules'
    ) THEN
        EXECUTE 'ALTER TABLE cdc_system.cdc_mapping_rules
                 ADD COLUMN IF NOT EXISTS rule_type VARCHAR(50) DEFAULT ''mapping''';
        RAISE NOTICE '[046] cdc_mapping_rules.rule_type ensured';
    END IF;
END $rule_type$;

-- 2. cdc_table_registry — 5 columns --------------------------------------------
DO $registry_cols$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'cdc_system' AND c.relname = 'cdc_table_registry'
    ) THEN
        RAISE NOTICE '[046] cdc_table_registry not present - skipping';
        RETURN;
    END IF;

    EXECUTE 'ALTER TABLE cdc_system.cdc_table_registry
             ADD COLUMN IF NOT EXISTS source_url TEXT';

    EXECUTE 'ALTER TABLE cdc_system.cdc_table_registry
             ADD COLUMN IF NOT EXISTS sync_status VARCHAR(50) DEFAULT ''unknown''';

    EXECUTE 'ALTER TABLE cdc_system.cdc_table_registry
             ADD COLUMN IF NOT EXISTS last_recon_at TIMESTAMPTZ';

    EXECUTE 'ALTER TABLE cdc_system.cdc_table_registry
             ADD COLUMN IF NOT EXISTS recon_drift BIGINT DEFAULT 0';

    EXECUTE 'ALTER TABLE cdc_system.cdc_table_registry
             ADD COLUMN IF NOT EXISTS last_bridge_at TIMESTAMPTZ';

    RAISE NOTICE '[046] cdc_table_registry columns ensured';
END $registry_cols$;

COMMIT;
