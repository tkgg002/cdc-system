-- Migration 016: Per-table Mongo timestamp field for reconciliation.
-- Date: 2026-04-20
-- Purpose: Bug B — Source count=0 khi collection dùng field tên khác (createdAt/lastUpdatedAt)
--          thay vì snake_case `updated_at` mà recon source agent hard-code.
--
-- Background (verified via mongosh 2026-04-20):
--   centralized-export-service.export-jobs: keys = [_id, jobId, ..., createdAt, lastUpdatedAt, __v]
--     → NO `updated_at` → filter `{updated_at: {$gte, $lt}}` returns 0 rows.
--   payment-bill-service.refund-requests: mixed schema, some docs `updated_at`, some chỉ `createdAt`.
--     → Tier 1 count sai lệch; dest_count=3422 nhưng source_count=0.
--
-- Fix:
--   Add `timestamp_field` column to cdc_table_registry. Source agent reads this
--   value (fallback default `updated_at`) when building window filters.
--
-- Safety:
--   - Default 'updated_at' preserves existing behaviour for tables đã hoạt động.
--   - Whitelist-validated in Go code (only [a-zA-Z0-9_]) to prevent injection.
--
-- Rollback: DROP COLUMN (no data loss, column is metadata only).

ALTER TABLE cdc_table_registry
    ADD COLUMN IF NOT EXISTS timestamp_field TEXT DEFAULT 'updated_at';

COMMENT ON COLUMN cdc_table_registry.timestamp_field IS
    'Mongo field name used for recon window filter ($gte/$lt). Common: updated_at, updatedAt, createdAt, lastUpdatedAt. Fallback: _id (ObjectID timestamp extraction).';

-- Seed the known exceptions discovered in audit 2026-04-20.
UPDATE cdc_table_registry SET timestamp_field = 'lastUpdatedAt'
 WHERE source_table = 'export-jobs' AND source_db = 'centralized-export-service'
   AND (timestamp_field IS NULL OR timestamp_field = 'updated_at');
