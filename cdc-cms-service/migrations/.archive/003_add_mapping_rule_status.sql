-- Phase 1.10: Add status and rule_type to cdc_mapping_rules
-- status: pending | approved | rejected
-- rule_type: system | discovered | mapping

ALTER TABLE cdc_mapping_rules
    ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'approved',
    ADD COLUMN IF NOT EXISTS rule_type VARCHAR(20) NOT NULL DEFAULT 'mapping';

-- Backfill: system-generated columns
UPDATE cdc_mapping_rules
SET rule_type = 'system'
WHERE source_field IN ('_raw_data', '_source', '_created_at', '_updated_at', '_deleted', '_hash', '_airbyte_raw_id', '_airbyte_extracted_at');

-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_mapping_rules_status ON cdc_mapping_rules(status);
CREATE INDEX IF NOT EXISTS idx_mapping_rules_rule_type ON cdc_mapping_rules(rule_type);
