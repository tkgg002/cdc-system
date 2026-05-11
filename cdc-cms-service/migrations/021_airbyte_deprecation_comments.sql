-- Migration 021 — R5 soft deprecation: comment Airbyte legacy columns
-- Plan ref: §R5 "DB soft deprecation markers"
-- Idempotent: COMMENT ON is safe to re-run.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name='cdc_table_registry' AND column_name='airbyte_source_id')
  THEN
    EXECUTE $cmt$
      COMMENT ON COLUMN cdc_table_registry.airbyte_source_id IS
        'DEPRECATED 2026-04-21: Airbyte pipeline removed per v7.2 parallel-system. Retained for back-compat of legacy public.* schema queries.'
    $cmt$;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name='cdc_table_registry' AND column_name='airbyte_connection_id')
  THEN
    EXECUTE 'COMMENT ON COLUMN cdc_table_registry.airbyte_connection_id IS ''DEPRECATED 2026-04-21''';
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name='cdc_table_registry' AND column_name='airbyte_destination_id')
  THEN
    EXECUTE 'COMMENT ON COLUMN cdc_table_registry.airbyte_destination_id IS ''DEPRECATED 2026-04-21''';
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name='cdc_table_registry' AND column_name='airbyte_destination_name')
  THEN
    EXECUTE 'COMMENT ON COLUMN cdc_table_registry.airbyte_destination_name IS ''DEPRECATED 2026-04-21''';
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name='cdc_table_registry' AND column_name='airbyte_raw_table')
  THEN
    EXECUTE 'COMMENT ON COLUMN cdc_table_registry.airbyte_raw_table IS ''DEPRECATED 2026-04-21''';
  END IF;
END $$;
