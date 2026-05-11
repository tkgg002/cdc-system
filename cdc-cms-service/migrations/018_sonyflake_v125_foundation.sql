-- Migration 018: Sonyflake v1.25 Parallel System Foundation
-- Creates cdc_internal schema + identity provider + fencing mechanism
-- User approved decisions: full envelope, _source_ts=10th field, cdc_internal registry, manual financial review, new v1.25 connector

BEGIN;

-- ============================================================================
-- Schema
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS cdc_internal;
COMMENT ON SCHEMA cdc_internal IS
  'CDC v1.25 parallel system — Sonyflake identity, fencing, clean shadow tables via Debezium';

-- ============================================================================
-- Extensions
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- For digest() in hash fallback (future use)

-- ============================================================================
-- Sequences
-- ============================================================================
CREATE SEQUENCE IF NOT EXISTS cdc_internal.machine_id_seq
  MINVALUE 1 MAXVALUE 65535 START 1 NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS cdc_internal.fencing_token_seq
  START 1 NO CYCLE;  -- Monotonic global, never reuses

-- ============================================================================
-- Worker Registry (heartbeat-based, no 'status' column per lesson #73)
-- ============================================================================
CREATE TABLE IF NOT EXISTS cdc_internal.worker_registry (
  machine_id      INTEGER PRIMARY KEY,
  fencing_token   BIGINT NOT NULL,
  hostname        TEXT NOT NULL,
  pid             INTEGER,
  claimed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_registry_heartbeat
  ON cdc_internal.worker_registry (heartbeat_at);

COMMENT ON TABLE cdc_internal.worker_registry IS
  'v1.25 Worker identity registry. machine_id reclaimable when heartbeat_at stale; fencing_token is monotonic per claim to invalidate zombie pods.';

-- ============================================================================
-- Claim function with heartbeat-based reclaim + fencing token
-- Drop first because OUT parameter names cannot be changed via CREATE OR REPLACE.
-- ============================================================================
DROP FUNCTION IF EXISTS cdc_internal.claim_machine_id(TEXT, INTEGER, INTERVAL);
CREATE OR REPLACE FUNCTION cdc_internal.claim_machine_id(
  p_hostname         TEXT,
  p_pid              INTEGER,
  p_stale_threshold  INTERVAL DEFAULT INTERVAL '90 seconds'
) RETURNS TABLE(out_machine_id INTEGER, out_fencing_token BIGINT) AS $$
DECLARE
  v_token BIGINT;
  v_mid INTEGER;
BEGIN
  v_token := nextval('cdc_internal.fencing_token_seq');

  -- Step 1: Try reclaim stale ID (aliased as wr to avoid ambiguity with OUT params)
  UPDATE cdc_internal.worker_registry AS wr
    SET hostname      = p_hostname,
        pid           = p_pid,
        claimed_at    = NOW(),
        heartbeat_at  = NOW(),
        fencing_token = v_token
    WHERE wr.machine_id = (
      SELECT w.machine_id FROM cdc_internal.worker_registry w
      WHERE w.heartbeat_at < NOW() - p_stale_threshold
      ORDER BY w.heartbeat_at ASC
      LIMIT 1 FOR UPDATE SKIP LOCKED
    )
    RETURNING wr.machine_id INTO v_mid;

  IF v_mid IS NOT NULL THEN
    RETURN QUERY SELECT v_mid, v_token;
    RETURN;
  END IF;

  -- Step 2: Fresh allocation via SEQUENCE
  v_mid := nextval('cdc_internal.machine_id_seq');
  INSERT INTO cdc_internal.worker_registry (machine_id, fencing_token, hostname, pid)
    VALUES (v_mid, v_token, p_hostname, p_pid);
  RETURN QUERY SELECT v_mid, v_token;

EXCEPTION
  WHEN sqlstate '2200H' THEN
    RAISE EXCEPTION 'machine_id_seq exhausted (>65535 pods)';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cdc_internal.claim_machine_id(TEXT, INTEGER, INTERVAL) IS
  'Atomically claims a machine_id (1..65535). Returns (machine_id, fencing_token). Reclaims stale IDs where heartbeat > p_stale_threshold.';

-- ============================================================================
-- Heartbeat function with fencing token check
-- ============================================================================
CREATE OR REPLACE FUNCTION cdc_internal.heartbeat_machine_id(
  p_machine_id    INTEGER,
  p_fencing_token BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
  v_current_token BIGINT;
BEGIN
  SELECT fencing_token INTO v_current_token
    FROM cdc_internal.worker_registry
    WHERE machine_id = p_machine_id;

  IF v_current_token IS NULL OR v_current_token != p_fencing_token THEN
    RETURN FALSE;  -- Pod MUST self-terminate
  END IF;

  UPDATE cdc_internal.worker_registry
    SET heartbeat_at = NOW()
    WHERE machine_id = p_machine_id AND fencing_token = p_fencing_token;
  RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cdc_internal.heartbeat_machine_id(INTEGER, BIGINT) IS
  'Refreshes heartbeat_at for the given machine_id. Returns FALSE when fencing_token no longer matches (pod was reclaimed) — caller MUST self-terminate.';

-- ============================================================================
-- Fencing Guard Trigger Function (T0.2)
-- NOT attached to any table yet — attach per-table in T1.3 when shadow created
-- Uses session variables app.fencing_machine_id + app.fencing_token (set by Worker per tx)
-- ============================================================================
CREATE OR REPLACE FUNCTION cdc_internal.tg_fencing_guard()
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
    FROM cdc_internal.worker_registry
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

COMMENT ON FUNCTION cdc_internal.tg_fencing_guard() IS
  'BEFORE INSERT/UPDATE trigger function. Requires session vars app.fencing_machine_id + app.fencing_token. Raises exception on missing vars or token mismatch (zombie pod). Attach to shadow tables in T1.3.';

COMMIT;
