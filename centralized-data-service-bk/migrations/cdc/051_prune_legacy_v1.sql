-- ----------------------------------------------------------------------
-- 051_prune_legacy_v1.sql — Phase B3 (System Refactor 2026-05).
-- Target DB: gpay-postgres-cdc (cdc_dw)
--
-- Idempotent prune of V1 legacy seed rows in cdc_system.source_object_registry
-- + shadow_binding + master_binding (migration 035 inserts 10 rows
-- legacy_1..legacy_10). Eliminates first-write-wins routeCache collisions
-- when V2 source_object_name == V1 source_object_name (e.g. "orders").
--
-- Re-runnable: only touches rows still active. Soft deactivate (is_active=false)
-- — no DELETE — so historical lineage stays queryable for audit.
-- ----------------------------------------------------------------------
BEGIN;

-- 1. Deactivate shadow_binding rows tied to legacy sources.
WITH legacy_src AS (
    SELECT id FROM cdc_system.source_object_registry
     WHERE object_code LIKE 'legacy\_%' ESCAPE '\'
)
UPDATE cdc_system.shadow_binding sb
   SET is_active = false,
       updated_at = NOW()
  FROM legacy_src ls
 WHERE sb.source_object_id = ls.id
   AND sb.is_active = true;

-- 2. Deactivate master_binding rows tied to legacy sources (defensive —
-- migration 035 currently inserts 0 master rows but a future re-seed
-- might).
WITH legacy_src AS (
    SELECT id FROM cdc_system.source_object_registry
     WHERE object_code LIKE 'legacy\_%' ESCAPE '\'
)
UPDATE cdc_system.master_binding mb
   SET is_active = false,
       updated_at = NOW()
  FROM legacy_src ls
 WHERE mb.source_object_id = ls.id
   AND mb.is_active = true;

-- 3. Deactivate the source rows themselves. Stamp `notes` so reviewers
-- see why (no deactivated_at column to use).
UPDATE cdc_system.source_object_registry
   SET is_active = false,
       notes = COALESCE(notes || E'\n', '') ||
               '[pruned by 051_prune_legacy_v1.sql at ' ||
               NOW()::text || ']',
       updated_at = NOW()
 WHERE object_code LIKE 'legacy\_%' ESCAPE '\'
   AND is_active = true;

-- 4. Report (pure SELECT for psql output; no rows mutated).
SELECT
    (SELECT count(*) FROM cdc_system.source_object_registry
      WHERE object_code LIKE 'legacy\_%' ESCAPE '\' AND is_active = false) AS pruned_sources,
    (SELECT count(*) FROM cdc_system.shadow_binding sb
      JOIN cdc_system.source_object_registry sor ON sor.id = sb.source_object_id
      WHERE sor.object_code LIKE 'legacy\_%' ESCAPE '\' AND sb.is_active = false) AS pruned_shadow_bindings,
    (SELECT count(*) FROM cdc_system.master_binding mb
      JOIN cdc_system.source_object_registry sor ON sor.id = mb.source_object_id
      WHERE sor.object_code LIKE 'legacy\_%' ESCAPE '\' AND mb.is_active = false) AS pruned_master_bindings;

COMMIT;
