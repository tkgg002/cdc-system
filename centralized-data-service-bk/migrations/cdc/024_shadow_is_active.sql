-- Migration 024 — Active gate L1: shadow `is_active` flag
-- Plan ref: §17.2

ALTER TABLE cdc_internal.table_registry
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT false;

COMMENT ON COLUMN cdc_internal.table_registry.is_active IS
  'L1 active gate. SinkWorker skip messages for shadow with is_active=false OR profile_status!=''active''. Default false — admin must flip after schema review.';

-- Back-fill 2 tables already approved in Phase 2 (user ran explicit UPDATE at S3.7)
UPDATE cdc_internal.table_registry
   SET is_active = true
 WHERE target_table IN ('export_jobs','refund_requests')
   AND profile_status = 'active';
