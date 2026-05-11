-- 039_set_search_path.sql
-- Sau migration 037/038 move CDC system tables sang schema cdc_system,
-- search_path của DB role chưa được cập nhật → raw SQL không qualify schema
-- (ví dụ system_health_collector, reconciliation_handler, activity_log_handler)
-- query "cdc_table_registry" không tìm thấy trong public → ERROR 42P01.
-- Migration này hoàn tất phần còn thiếu: đặt search_path mặc định bao gồm cdc_system.

ALTER ROLE gpay_admin SET search_path = cdc_system, public;
