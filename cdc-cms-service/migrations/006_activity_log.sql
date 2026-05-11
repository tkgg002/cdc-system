-- ============================================================
-- CDC Integration - v1.13: Activity Log for Worker Monitoring
-- Tracks all auto operations with detail
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_activity_log (
    id BIGSERIAL PRIMARY KEY,
    operation VARCHAR(50) NOT NULL,         -- bridge, transform, scan-fields, reload-cache, partition-check, bridge-batch, drop-gin-index
    target_table VARCHAR(200),              -- which CDC table was affected
    status VARCHAR(20) NOT NULL DEFAULT 'running', -- running, success, error, skipped
    rows_affected BIGINT DEFAULT 0,
    duration_ms INT,                        -- execution time in milliseconds
    details JSONB,                          -- structured details (fields_found, columns_added, hash_matches, etc.)
    error_message TEXT,
    triggered_by VARCHAR(50) DEFAULT 'scheduler', -- scheduler, manual, nats-command
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_activity_log_operation ON cdc_activity_log(operation);
CREATE INDEX idx_activity_log_target ON cdc_activity_log(target_table);
CREATE INDEX idx_activity_log_status ON cdc_activity_log(status);
CREATE INDEX idx_activity_log_started ON cdc_activity_log(started_at DESC);

COMMIT;
