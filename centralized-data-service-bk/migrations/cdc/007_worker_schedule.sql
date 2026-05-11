-- ============================================================
-- CDC Integration - v1.13: Worker Schedule Management
-- Control which operations run, on which tables, at what interval
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS cdc_worker_schedule (
    id SERIAL PRIMARY KEY,
    operation VARCHAR(50) NOT NULL,          -- bridge, transform, field-scan, partition-check
    target_table VARCHAR(200),               -- NULL = all tables, specific = only this table
    interval_minutes INT NOT NULL DEFAULT 5, -- how often to run
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    last_run_at TIMESTAMP,
    next_run_at TIMESTAMP,
    run_count BIGINT DEFAULT 0,
    last_error TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(operation, target_table)
);

-- Default schedules
INSERT INTO cdc_worker_schedule (operation, target_table, interval_minutes, is_enabled, notes) VALUES
    ('bridge', NULL, 5, true, 'Đồng bộ dữ liệu Airbyte → CDC cho tất cả bảng active'),
    ('transform', NULL, 5, true, 'Chuyển đổi _raw_data → typed columns cho tất cả bảng active'),
    ('field-scan', NULL, 60, true, 'Quét tìm field mới trong _raw_data'),
    ('partition-check', NULL, 1440, true, 'Kiểm tra + tạo partition cho tháng tiếp theo'),
    ('airbyte-sync', NULL, 5, true, 'Quét stream từ Airbyte + đồng bộ registry')
ON CONFLICT DO NOTHING;

CREATE INDEX idx_worker_schedule_operation ON cdc_worker_schedule(operation);
CREATE INDEX idx_worker_schedule_enabled ON cdc_worker_schedule(is_enabled);

COMMIT;
