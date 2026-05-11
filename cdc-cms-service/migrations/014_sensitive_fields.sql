-- migration 014 — v3 §4 + plan WORKER task #7
-- Adds per-table sensitive_fields JSONB to cdc_table_registry so
-- DLQ / recon heal / retry paths can mask field values on a
-- per-table basis instead of using a single hardcoded global list.
--
-- Shape: ["email", "phone", "national_id", ...] — a JSON array of
-- field names. Case-insensitive lookup is done in the Go adapter.
-- Default is an empty array so existing tables keep the same
-- (no-mask) behaviour.

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS sensitive_fields JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN cdc_table_registry.sensitive_fields IS
    'Per-table list of field names whose values are masked when stored into failed_sync_logs.raw_json or cdc_activity_log.details. JSON array of strings; empty means no masking.';
