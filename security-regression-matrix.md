# Security Regression Matrix - CDC System

- Date: 2026-04-24
- Scope: `cdc-system`, trọng tâm `centralized-data-service`
- Purpose: Ma trận hồi quy bảo mật hợp nhất để kiểm tra nhanh trust boundary, sanitizer/masking strategy, và bằng chứng test.

## 1. Matrix

| # | Trust Boundary | Module / Path | Risk Locked | Protection Strategy | Regression Evidence | Status |
|---|----------------|---------------|-------------|---------------------|---------------------|--------|
| 1 | Raw ingest -> `_raw_data` | `internal/service/dynamic_mapper.go` | `_raw_data` chứa raw Mongo payload | Shared `MaskingService` trước `json.Marshal` | `dynamic_mapper_test.go` | Locked |
| 2 | Heal retry -> `_raw_data` | `internal/service/recon_heal.go` | Healing rebuild `_raw_data` chưa mask | Shared `MaskingService` qua `buildMaskedRawJSON(...)` | `recon_heal_test.go` | Locked |
| 3 | Schema drift sample -> DB/NATS | `internal/service/schema_inspector.go` | `SampleValue` lộ PII | `MaskFieldSample` / heuristic masking | `schema_inspector_test.go` | Locked |
| 4 | DLQ direct handler -> DB/NATS | `internal/handler/dlq_handler.go` | `failed_sync_logs.RawJSON`, `ErrorMessage`, `cdc.dlq` chứa PII | JSON masking + free-form text sanitizer | `dlq_handler_test.go`, `dlq_handler_integration_test.go` | Locked |
| 5 | Kafka ingest failure -> `failed_sync_logs` | `internal/handler/kafka_consumer.go` | raw Kafka message + processing error rơi thẳng xuống DB | `sanitizeDLQRawJSON(...)` + `SanitizeFreeformText(...)` | `kafka_consumer_dlq_test.go`, `kafka_consumer_integration_test.go` | Locked |
| 6 | Batch upsert failure -> `failed_sync_logs` | `internal/handler/batch_buffer.go` | batch failure log giữ raw payload | shared masking trước build log row | `batch_buffer_test.go` | Locked |
| 7 | Admin retry -> SQL args / `_raw_data` | `internal/handler/recon_handler.go` | retry path upsert lại raw JSON chưa mask | `sanitizeRetryRawJSON(...)` trước parse + SQL build | `recon_handler_test.go`, `recon_handler_integration_test.go` | Locked |
| 8 | Legacy DLQ retry rebuild | `internal/service/dlq_worker.go` | Mongo fallback rebuild payload chưa mask | `buildRetryRawJSON(...)` + masking service | `dlq_worker_test.go` | Locked |
| 9 | Admin result -> ActivityLog / NATS / console | `internal/handler/command_handler.go` | admin result/error text lộ PII hoặc SQL/payload thừa | allowlist result map + free-form text sanitizer | `command_handler_test.go`, `command_handler_activity_integration_test.go` | Locked |
| 10 | Event bridge -> NATS downstream | `internal/handler/event_bridge.go` | downstream nhận full-row payload | Data minimization: metadata-only payload | `event_bridge_test.go`, `event_bridge_integration_test.go` | Locked |

## 2. Shared Controls

| Control | File | Purpose |
|---------|------|---------|
| Structured field masking | `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/masking_service.go` | Mask PII theo `sensitive_fields` + heuristic keyword |
| Free-form text sanitizer | `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/text_sanitizer.go` | Redact email / phone / secret / token / password / api_key trong error text, logs, audit text |
| DI wiring | `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/server/worker_server.go` | Đảm bảo runtime path dùng shared masking instance thống nhất |

## 3. Ancillary Flow Audit

### 3.1 `activity_logger.go`
- Result: cần chuẩn hóa
- Fix applied:
  - `Complete(...)` sanitize nested detail strings trước khi ghi `cdc_activity_log`
  - `Fail(...)` sanitize `error_message`
  - `Skip(...)` sanitize reason
  - `Quick(...)` sanitize cả details lẫn error text
- File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/activity_logger.go`

### 3.2 `backfill_source_ts.go`
- Result: cần chuẩn hóa
- Fix applied:
  - `finishRun(...)` sanitize `recon_runs.error_message`
  - `WriteActivity(...)` sanitize nested `results` payload trước khi ghi `cdc_activity_log`
  - mid-run `finalErr` cũng được normalize qua shared free-form sanitizer
- File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/backfill_source_ts.go`

### 3.3 `transmuter.go`
- Result: audit-pass, chưa thấy trust boundary mới cần patch ngay
- Reasoning:
  - module này chủ yếu đọc shadow rows và upsert typed master rows
  - không có đường ghi `failed_sync_logs`, `cdc_activity_log`, hay outbound event/log payload chi tiết theo dạng free-form text giống các path admin/DLQ
  - current error handling chủ yếu là internal logger / return error, không persistence raw document mới
- File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/transmuter.go`

### 3.4 `transmute_scheduler.go`
- Result: cần chuẩn hóa
- Fix applied:
  - sanitize `cdc_internal.transmute_schedule.last_error` khi publish NATS fail
  - sanitize `last_error` khi parse cron fail
- File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/transmute_scheduler.go`

### 3.5 `recon_core.go`
- Result: cần chuẩn hóa
- Fix applied:
  - sanitize `recon_runs.error_message` trong `finishRun(...)`
  - sanitize `cdc_reconciliation_reports.error_message` trong `errorReport(...)`
- File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/recon_core.go`

## 4. Verification Commands

```bash
cd /Users/trainguyen/Documents/work/cdc-system/centralized-data-service
mkdir -p /tmp/cdc-go-cache
GOCACHE=/tmp/cdc-go-cache go test ./internal/handler ./internal/service ./internal/server
GOCACHE=/tmp/cdc-go-cache go test -tags integration ./internal/handler
```

## 5. Maintenance Rule

Khi có trust boundary mới, chỉ được coi là “Locked” khi đủ cả 3 điều kiện:
1. Runtime path đã dùng shared control phù hợp (`MaskingService`, `SanitizeFreeformText`, hoặc `Data Minimization`)
2. Có test regression tương ứng (unit hoặc integration, ưu tiên integration nếu boundary chạm DB/NATS)
3. Matrix này được cập nhật để reviewer nhìn một chỗ là thấy đủ bằng chứng
