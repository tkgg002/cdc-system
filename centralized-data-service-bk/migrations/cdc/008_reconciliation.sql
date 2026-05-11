-- ============================================================
-- CDC Integration: Reconciliation + Failed Sync Logs
-- Data Integrity System
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_reconciliation_report (
    id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(200) NOT NULL,
    source_db VARCHAR(200),
    source_count BIGINT DEFAULT 0,
    dest_count BIGINT DEFAULT 0,
    diff BIGINT DEFAULT 0,
    missing_count INT DEFAULT 0,
    missing_ids JSONB,
    stale_count INT DEFAULT 0,
    stale_ids JSONB,
    check_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    tier INT DEFAULT 1,
    duration_ms INT,
    error_message TEXT,
    checked_at TIMESTAMP DEFAULT NOW(),
    healed_at TIMESTAMP,
    healed_count INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_recon_table ON cdc_reconciliation_report(target_table);
CREATE INDEX IF NOT EXISTS idx_recon_status ON cdc_reconciliation_report(status);
CREATE INDEX IF NOT EXISTS idx_recon_checked ON cdc_reconciliation_report(checked_at DESC);

CREATE TABLE IF NOT EXISTS failed_sync_logs (
    id BIGSERIAL PRIMARY KEY,
    target_table VARCHAR(200) NOT NULL,
    source_table VARCHAR(200),
    source_db VARCHAR(200),
    record_id VARCHAR(200),
    operation VARCHAR(10),
    raw_json JSONB,
    error_message TEXT NOT NULL,
    error_type VARCHAR(50),
    kafka_topic VARCHAR(200),
    kafka_partition INT,
    kafka_offset BIGINT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    status VARCHAR(20) DEFAULT 'failed',
    created_at TIMESTAMP DEFAULT NOW(),
    last_retry_at TIMESTAMP,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_failed_table ON failed_sync_logs(target_table);
CREATE INDEX IF NOT EXISTS idx_failed_status ON failed_sync_logs(status);
CREATE INDEX IF NOT EXISTS idx_failed_created ON failed_sync_logs(created_at DESC);

COMMIT;
