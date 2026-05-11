-- Migration 038: Finalize control-plane namespace in cdc_system
-- Goal:
--   1. move remaining system sequences/functions from cdc_internal -> cdc_system
--   2. keep runtime-compatible helpers under cdc_system only
--   3. drop cdc_internal schema once emptied

BEGIN;

CREATE SCHEMA IF NOT EXISTS cdc_system;

ALTER TABLE IF EXISTS cdc_internal.worker_registry SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.schema_proposal SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.enum_types SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.sources SET SCHEMA cdc_system;
ALTER TABLE IF EXISTS cdc_internal.cdc_wizard_sessions SET SCHEMA cdc_system;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'table_registry'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_system' AND table_name = 'table_registry_legacy'
  ) THEN
    ALTER TABLE cdc_internal.table_registry SET SCHEMA cdc_system;
    ALTER TABLE cdc_system.table_registry RENAME TO table_registry_legacy;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'master_table_registry'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_system' AND table_name = 'master_table_registry_legacy'
  ) THEN
    ALTER TABLE cdc_internal.master_table_registry SET SCHEMA cdc_system;
    ALTER TABLE cdc_system.master_table_registry RENAME TO master_table_registry_legacy;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_internal' AND table_name = 'transmute_schedule'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'cdc_system' AND table_name = 'transmute_schedule_legacy'
  ) THEN
    ALTER TABLE cdc_internal.transmute_schedule SET SCHEMA cdc_system;
    ALTER TABLE cdc_system.transmute_schedule RENAME TO transmute_schedule_legacy;
  END IF;
END $$;

-- Phase 39: 028 (rewrite) đã tạo cdc_system.fencing_token_seq trực tiếp.
-- Nếu 018 đã tạo cdc_internal.fencing_token_seq trước đó, drop để tránh
-- xung đột tên khi ALTER SET SCHEMA. Tương tự cho machine_id_seq.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname='cdc_system' AND c.relname='machine_id_seq' AND c.relkind='S') THEN
    DROP SEQUENCE IF EXISTS cdc_internal.machine_id_seq;
  ELSE
    EXECUTE 'ALTER SEQUENCE IF EXISTS cdc_internal.machine_id_seq SET SCHEMA cdc_system';
  END IF;

  IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname='cdc_system' AND c.relname='fencing_token_seq' AND c.relkind='S') THEN
    DROP SEQUENCE IF EXISTS cdc_internal.fencing_token_seq;
  ELSE
    EXECUTE 'ALTER SEQUENCE IF EXISTS cdc_internal.fencing_token_seq SET SCHEMA cdc_system';
  END IF;
END $$;

CREATE SEQUENCE IF NOT EXISTS cdc_system.machine_id_seq AS INTEGER MINVALUE 1 MAXVALUE 65535 CYCLE;
CREATE SEQUENCE IF NOT EXISTS cdc_system.fencing_token_seq AS BIGINT MINVALUE 1;

DROP FUNCTION IF EXISTS cdc_system.claim_machine_id(TEXT, INTEGER, INTERVAL);
CREATE OR REPLACE FUNCTION cdc_system.claim_machine_id(
  p_hostname         TEXT,
  p_pid              INTEGER,
  p_stale_threshold  INTERVAL DEFAULT INTERVAL '90 seconds'
) RETURNS TABLE(out_machine_id INTEGER, out_fencing_token BIGINT) AS $$
DECLARE
  v_token BIGINT;
  v_mid INTEGER;
BEGIN
  v_token := nextval('cdc_system.fencing_token_seq');

  UPDATE cdc_system.worker_registry AS wr
    SET hostname      = p_hostname,
        pid           = p_pid,
        claimed_at    = NOW(),
        heartbeat_at  = NOW(),
        fencing_token = v_token
    WHERE wr.machine_id = (
      SELECT w.machine_id FROM cdc_system.worker_registry w
      WHERE w.heartbeat_at < NOW() - p_stale_threshold
      ORDER BY w.heartbeat_at ASC
      LIMIT 1 FOR UPDATE SKIP LOCKED
    )
    RETURNING wr.machine_id INTO v_mid;

  IF v_mid IS NOT NULL THEN
    RETURN QUERY SELECT v_mid, v_token;
    RETURN;
  END IF;

  v_mid := nextval('cdc_system.machine_id_seq');
  INSERT INTO cdc_system.worker_registry (machine_id, fencing_token, hostname, pid)
    VALUES (v_mid, v_token, p_hostname, p_pid);
  RETURN QUERY SELECT v_mid, v_token;
EXCEPTION
  WHEN sqlstate '2200H' THEN
    RAISE EXCEPTION 'machine_id_seq exhausted (>65535 pods)';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cdc_system.heartbeat_machine_id(
  p_machine_id    INTEGER,
  p_fencing_token BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
  v_current_token BIGINT;
BEGIN
  SELECT fencing_token INTO v_current_token
    FROM cdc_system.worker_registry
   WHERE machine_id = p_machine_id;

  IF v_current_token IS NULL OR v_current_token != p_fencing_token THEN
    RETURN FALSE;
  END IF;

  UPDATE cdc_system.worker_registry
     SET heartbeat_at = NOW()
   WHERE machine_id = p_machine_id
     AND fencing_token = p_fencing_token;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cdc_system.tg_fencing_guard()
RETURNS TRIGGER AS $$
DECLARE
  v_session_machine INTEGER;
  v_session_token   BIGINT;
  v_current_token   BIGINT;
BEGIN
  BEGIN
    v_session_machine := current_setting('app.fencing_machine_id', false)::INTEGER;
    v_session_token   := current_setting('app.fencing_token', false)::BIGINT;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'FENCING: session variables app.fencing_machine_id + app.fencing_token required';
  END;

  SELECT fencing_token INTO v_current_token
    FROM cdc_system.worker_registry
   WHERE machine_id = v_session_machine;

  IF v_current_token IS NULL THEN
    RAISE EXCEPTION 'FENCING: machine_id % not registered', v_session_machine;
  END IF;

  IF v_current_token != v_session_token THEN
    RAISE EXCEPTION 'FENCING: token mismatch (pod reclaimed). machine_id=%, pod_token=%, current_token=%',
      v_session_machine, v_session_token, v_current_token;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cdc_system.enable_master_rls(p_table_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  policy_exists BOOLEAN;
BEGIN
  IF p_table_name !~ '^[a-z_][a-z0-9_]{0,62}$' THEN
    RAISE EXCEPTION 'invalid table name: %', p_table_name;
  END IF;

  EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', p_table_name);

  SELECT EXISTS (
    SELECT 1 FROM pg_policies
     WHERE schemaname = 'public' AND tablename = p_table_name
       AND policyname = 'rls_master_default_permissive'
  ) INTO policy_exists;

  IF NOT policy_exists THEN
    EXECUTE format(
      'CREATE POLICY rls_master_default_permissive ON public.%I
        FOR ALL USING (true) WITH CHECK (true)', p_table_name);
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION cdc_system.gen_sonyflake_id()
RETURNS BIGINT AS $$
DECLARE
  v_ts_ms   BIGINT;
  v_machine INTEGER;
  v_seq     BIGINT;
BEGIN
  v_ts_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - 1767225600000;
  BEGIN
    v_machine := COALESCE(NULLIF(current_setting('cdc.machine_id', true), '')::INTEGER, 0) & 65535;
  EXCEPTION WHEN OTHERS THEN
    v_machine := 0;
  END;
  v_seq := nextval('cdc_system.fencing_token_seq') & 65535;
  RETURN ((v_ts_ms & 4398046511103) << 22) | ((v_machine::BIGINT & 65535) << 6) | (v_seq & 63);
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION cdc_system.tg_sonyflake_fallback()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.id IS NULL OR NEW.id = 0 THEN
    NEW.id := cdc_system.gen_sonyflake_id();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS cdc_internal.claim_machine_id(TEXT, INTEGER, INTERVAL);
DROP FUNCTION IF EXISTS cdc_internal.heartbeat_machine_id(INTEGER, BIGINT);
DROP FUNCTION IF EXISTS cdc_internal.tg_fencing_guard();
DROP FUNCTION IF EXISTS cdc_internal.enable_master_rls(TEXT);
DROP FUNCTION IF EXISTS cdc_internal.ensure_shadow_sonyflake_trigger(TEXT);
DROP FUNCTION IF EXISTS cdc_internal.tg_sonyflake_fallback();
DROP FUNCTION IF EXISTS cdc_internal.gen_sonyflake_id();

DROP SCHEMA IF EXISTS cdc_internal CASCADE;

COMMIT;
