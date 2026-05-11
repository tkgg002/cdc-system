-- ============================================================
-- CDC Integration v3 — Phase 2/3 (T5 Schema Validator)
-- Add `expected_fields JSONB` to `cdc_table_registry` so Phase A
-- (JSON converter) payload validation has an authoritative source
-- of truth for required + known fields.
--
-- Schema (JSON):
-- {
--   "required": ["_id","updated_at"],
--   "known":    ["_id","updated_at","user_id","amount","status",...]
-- }
--
-- When `expected_fields` is NULL the validator degrades gracefully
-- by introspecting information_schema on the target PG table (see
-- schema_validator.go).
-- ============================================================

BEGIN;

DO $fields$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'cdc_table_registry'
    ) THEN
        RAISE NOTICE '[013_registry_expected_fields] cdc_table_registry missing - skip';
        RETURN;
    END IF;

    EXECUTE 'ALTER TABLE cdc_table_registry
             ADD COLUMN IF NOT EXISTS expected_fields JSONB';

    RAISE NOTICE '[013_registry_expected_fields] expected_fields column ensured';
END $fields$;

COMMIT;
