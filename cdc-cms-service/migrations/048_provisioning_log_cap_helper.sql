-- ============================================================
-- Source Provisioning Mode — step log cap helper (D7)
--
-- Phase: feature-cdc-integration / provisioning_mode (Phase B)
-- Architect ruling: 04_decisions_provisioning_mode.md §D7
--   `provisioning_step_log` JSONB array MUST be capped at 50
--   entries (FIFO trim — keep newest 50, drop oldest). Helper
--   function avoids inlining a multi-line CTE in every UPDATE
--   issued by the orchestrator.
--
-- Idempotent:
--   - CREATE OR REPLACE FUNCTION (re-running migration safe).
-- ============================================================

BEGIN;

CREATE OR REPLACE FUNCTION cdc_system.append_step_log_capped(
    current_log JSONB,
    new_entry   JSONB,
    max_entries INT DEFAULT 50
) RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
AS $fn$
DECLARE
    appended JSONB;
    n        INT;
BEGIN
    -- Defensive: treat NULL log as empty array.
    IF current_log IS NULL OR jsonb_typeof(current_log) <> 'array' THEN
        current_log := '[]'::jsonb;
    END IF;
    IF max_entries IS NULL OR max_entries < 1 THEN
        max_entries := 50;
    END IF;

    appended := current_log || new_entry;
    n := jsonb_array_length(appended);

    IF n <= max_entries THEN
        RETURN appended;
    END IF;

    -- FIFO trim: drop oldest (n - max_entries) entries, keep tail.
    RETURN (
        SELECT COALESCE(jsonb_agg(elem ORDER BY ord), '[]'::jsonb)
          FROM (
            SELECT elem, ord
              FROM jsonb_array_elements(appended) WITH ORDINALITY AS t(elem, ord)
             WHERE ord > (n - max_entries)
          ) sub
    );
END;
$fn$;

COMMENT ON FUNCTION cdc_system.append_step_log_capped(JSONB, JSONB, INT) IS
'D7: append entry to provisioning_step_log JSONB array, FIFO-trim to max_entries (default 50).';

DO $smoke$
DECLARE
    cap INT := 5;
    log JSONB := '[]'::jsonb;
    i   INT;
    n   INT;
    first_seq INT;
BEGIN
    -- Smoke: append 8 entries cap=5 → expect length 5, first.seq=4.
    FOR i IN 1..8 LOOP
        log := cdc_system.append_step_log_capped(
            log,
            jsonb_build_object('seq', i, 'step', 'smoke'),
            cap
        );
    END LOOP;
    n := jsonb_array_length(log);
    first_seq := (log->0->>'seq')::int;
    IF n <> cap THEN
        RAISE EXCEPTION '[048] log cap smoke FAILED: expected length %, got %', cap, n;
    END IF;
    IF first_seq <> (8 - cap + 1) THEN
        RAISE EXCEPTION '[048] log cap smoke FAILED: expected first seq=%, got %',
            8 - cap + 1, first_seq;
    END IF;
    RAISE NOTICE '[048] append_step_log_capped smoke OK (length=%, first.seq=%)', n, first_seq;
END $smoke$;

COMMIT;
