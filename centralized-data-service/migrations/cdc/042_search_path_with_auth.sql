-- Migration 042 (Phase 39 → revised by Phase 01 split E2E):
-- Sau khi tách multi-PG, schema cdc_auth_service nằm trên DB `gpay_auth`
-- (không phải cdc_dw). Trên cdc_dw, search_path chỉ cần cdc_system → public.
-- Supersedes 039_set_search_path.sql (giữ history).
-- Yêu cầu services restart connection pool sau khi apply để session pickup.

BEGIN;

ALTER ROLE gpay_admin SET search_path = cdc_system, public;

COMMIT;
