-- ============================================================
-- Source Provisioning Mode (Auto/Manual) — state machine columns.
--
-- Phase: feature-cdc-integration / provisioning_mode
-- Architect rulings: 04_decisions_provisioning_mode.md
--   D4 — Legacy source backfill state = 'provisioned' (NOT 'running'),
--        single stamp entry, no fake history.
--   D6 — All state UPDATE statements must use CAS WHERE guard.
--
-- Adds 4 columns to cdc_system.source_object_registry + 1 partial
-- index for orchestrator's pending-state polling.
--
-- Idempotent:
--   - ADD COLUMN IF NOT EXISTS
--   - CREATE INDEX IF NOT EXISTS
--   - Backfill UPDATE has WHERE provisioning_state='draft' (CAS-equivalent)
--     so re-running migration on already-stamped rows is a no-op.
-- ============================================================

BEGIN;

DO $cols$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'cdc_system'
           AND c.relname = 'source_object_registry'
    ) THEN
        RAISE NOTICE '[047] cdc_system.source_object_registry not present - skipping';
        RETURN;
    END IF;

    EXECUTE 'ALTER TABLE cdc_system.source_object_registry
             ADD COLUMN IF NOT EXISTS provisioning_mode VARCHAR(20)
               DEFAULT ''manual''
               CHECK (provisioning_mode IN (''auto'',''manual''))';

    EXECUTE 'ALTER TABLE cdc_system.source_object_registry
             ADD COLUMN IF NOT EXISTS provisioning_state VARCHAR(40)
               DEFAULT ''draft''';

    EXECUTE 'ALTER TABLE cdc_system.source_object_registry
             ADD COLUMN IF NOT EXISTS provisioning_step_log JSONB
               DEFAULT ''[]''::jsonb';

    EXECUTE 'ALTER TABLE cdc_system.source_object_registry
             ADD COLUMN IF NOT EXISTS last_step_error TEXT';

    RAISE NOTICE '[047] source_object_registry provisioning columns ensured';
END $cols$;

-- D4 backfill — stamp legacy active sources into 'provisioned' (terminal,
-- NOT 'running' to distinguish from sources that completed the new flow).
-- WHERE provisioning_state='draft' makes this idempotent on re-run.
UPDATE cdc_system.source_object_registry
   SET provisioning_state = 'provisioned',
       provisioning_step_log = jsonb_build_array(
           jsonb_build_object(
               'seq', 1,
               'step', 'backfill',
               'from_state', 'draft',
               'to_state', 'provisioned',
               'actor', 'migration-047',
               'correlation_id', NULL,
               'started_at', NOW(),
               'completed_at', NOW(),
               'success', true,
               'error', NULL,
               'message', '[Migration-047]: Backfilled legacy source to state ''provisioned'''
           ))
 WHERE is_active = true
   AND provisioning_state = 'draft';

-- Partial index for RecoveryLoop polling (D3 — TTL 10 minutes scan)
-- and orchestrator's general pending-state lookups.
CREATE INDEX IF NOT EXISTS idx_sor_provisioning_state
  ON cdc_system.source_object_registry (provisioning_state, updated_at)
  WHERE provisioning_state IN
    ('shadow_pending','master_pending','mapping_pending','schedule_pending','failed');

-- Report.
DO $report$
DECLARE
    n_provisioned INT;
    n_draft       INT;
BEGIN
    SELECT count(*) INTO n_provisioned
      FROM cdc_system.source_object_registry
     WHERE provisioning_state = 'provisioned';
    SELECT count(*) INTO n_draft
      FROM cdc_system.source_object_registry
     WHERE provisioning_state = 'draft';
    RAISE NOTICE '[047] backfill summary: provisioned=%, draft=%', n_provisioned, n_draft;
END $report$;

COMMIT;
