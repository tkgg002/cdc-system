-- Migration 028: Sonyflake Fallback Trigger function (Phase 39 REWRITE)
-- Phase 39 (Option A): function move from cdc_internal → cdc_system.
-- Helper signature đổi từ (p_table) → (p_schema, p_table) để target
-- schema động (caller truyền 'shadow_<src>'). Sequence cũng move.
-- Authoritative path vẫn là Go Worker pkgs/idgen/sonyflake.go.

BEGIN;

-- 1) Sequence dùng cho seq slot (16 bits) — bản mới ở cdc_system.
--    Note: 018 cũ tạo cdc_internal.fencing_token_seq; sau wipe schema
--    cdc_internal bị drop nên reference cũ không còn.
CREATE SEQUENCE IF NOT EXISTS cdc_system.fencing_token_seq;

-- 2) Custom epoch: 2026-01-01 UTC (ms since)
CREATE OR REPLACE FUNCTION cdc_system.gen_sonyflake_id()
RETURNS BIGINT AS $$
DECLARE
  v_ts_ms   BIGINT;
  v_machine INTEGER;
  v_seq     BIGINT;
BEGIN
  v_ts_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - 1767225600000;
  -- Machine ID: session-set via SET LOCAL cdc.machine_id = '...' by the Go worker.
  -- Fallback to 0 for psql inserts / manual use.
  BEGIN
    v_machine := COALESCE(NULLIF(current_setting('cdc.machine_id', true), '')::INTEGER, 0) & 65535;
  EXCEPTION WHEN OTHERS THEN
    v_machine := 0;
  END;
  v_seq := nextval('cdc_system.fencing_token_seq') & 65535;
  RETURN ((v_ts_ms & 4398046511103) << 22) | ((v_machine::BIGINT & 65535) << 6) | (v_seq & 63);
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION cdc_system.gen_sonyflake_id() IS
  'Fallback Sonyflake-like ID. Shape: [42 bits ts ms since 2026-01-01][16 bits machine_id][6 bits seq]. Go worker overrides with authoritative IDs.';

-- 3) Trigger body — attached per shadow table by Go automator.
CREATE OR REPLACE FUNCTION cdc_system.tg_sonyflake_fallback()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.id IS NULL OR NEW.id = 0 THEN
    NEW.id := cdc_system.gen_sonyflake_id();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cdc_system.tg_sonyflake_fallback() IS
  'BEFORE INSERT trigger body — fallback id gen. Attached by cdc_system.ensure_shadow_sonyflake_trigger().';

-- 4) Helper: idempotent attach trigger cho 1 table tại schema bất kỳ.
--    Caller truyền p_schema = 'shadow_<source_db>', p_table = target_table.
CREATE OR REPLACE FUNCTION cdc_system.ensure_shadow_sonyflake_trigger(
    p_schema TEXT, p_table TEXT
) RETURNS VOID AS $$
DECLARE
  v_trigger_name TEXT;
BEGIN
  v_trigger_name := 'trg_' || p_table || '_sonyflake_fallback';
  -- Drop then recreate (idempotent; guards against old signature)
  EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I',
                 v_trigger_name, p_schema, p_table);
  EXECUTE format(
    'CREATE TRIGGER %I BEFORE INSERT ON %I.%I '
    || 'FOR EACH ROW EXECUTE FUNCTION cdc_system.tg_sonyflake_fallback()',
    v_trigger_name, p_schema, p_table
  );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cdc_system.ensure_shadow_sonyflake_trigger(TEXT, TEXT) IS
  'Idempotent: drop+recreate fallback trigger on <p_schema>.<p_table>. Schema-aware (Phase 39 Option A). Called by Go ShadowAutomator after DDL.';

-- 5) Drop legacy cdc_internal.* objects nếu còn (defense-in-depth cho
--    môi trường upgrade từ pre-Phase 39). Sau wipe v2 cdc_internal đã
--    bị DROP CASCADE nên block này thường no-op.
DROP FUNCTION IF EXISTS cdc_internal.ensure_shadow_sonyflake_trigger(TEXT);
DROP FUNCTION IF EXISTS cdc_internal.tg_sonyflake_fallback();
DROP FUNCTION IF EXISTS cdc_internal.gen_sonyflake_id();

COMMIT;
