-- Migration 043 (Phase 39): Normalize shadow_binding.shadow_schema từ
-- legacy 'cdc_internal' (Phase 38 và trước) sang chuẩn V2
-- 'shadow_<source_db>'. Defense-in-depth — sau wipe v2 bảng đã empty,
-- migration này chỉ active khi upgrade từ DB còn data cũ.

BEGIN;

UPDATE cdc_system.shadow_binding sb
SET shadow_schema = 'shadow_' || lower(regexp_replace(
        sor.source_database, '[^a-zA-Z0-9_]', '_', 'g'))
FROM cdc_system.source_object_registry sor
WHERE sb.source_object_id = sor.id
  AND (sb.shadow_schema IS NULL
       OR sb.shadow_schema = 'cdc_internal'
       OR sb.shadow_schema = '');

-- Verify: 0 rows trỏ về cdc_internal sau update.
DO $$
DECLARE n INT;
BEGIN
  SELECT count(*) INTO n FROM cdc_system.shadow_binding
   WHERE shadow_schema = 'cdc_internal'
      OR shadow_schema IS NULL
      OR shadow_schema = '';
  IF n > 0 THEN
    RAISE EXCEPTION 'shadow_binding still has % unnormalized rows', n;
  END IF;
END $$;

COMMIT;
