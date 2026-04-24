# Security Audit Report - CDC System

- Date: 2026-04-24
- Scope: `cdc-system`, trọng tâm là `centralized-data-service`
- Auditor: Codex (Muscle / Chief Engineer)
- Status: Completed for audited runtime trust boundaries in current worker path

## 1. Executive Summary

Đợt audit này tập trung vào bài toán rò rỉ dữ liệu nhạy cảm tại các trust boundary nơi payload CDC, `_raw_data`, `RawJSON`, `SampleValue`, kết quả command admin, hoặc event bridge có thể đi xuống database, NATS, log, hoặc response mà chưa qua sanitize/minimization.

Kết quả:
- Toàn bộ các trust boundary runtime chính đã được bịt theo một chuẩn thống nhất dựa trên shared `MaskingService` hoặc nguyên tắc `Data Minimization`.
- Các module persistence/retry/reconciliation/inspection/admin command giờ không còn ghi hoặc phát tán raw payload nguyên trạng tại các đường đi đã audit.
- Bộ test hồi quy đã được mở rộng để khóa hành vi masking/minimization, giúp CI chặn regression trong tương lai.

## 2. Methodology

Audit được thực hiện theo 4 bước:
1. Dò tìm trust boundary bằng grep/source review tại các module ingestion, retry, reconciliation, admin command, event publish.
2. Xác định nơi raw payload có thể đi xuống DB/NATS/log/response mà chưa sanitize.
3. Vá runtime path bằng shared `MaskingService` hoặc giảm payload về metadata tối thiểu.
4. Bổ sung unit test bất biến và chạy `gofmt`, `go test` để xác nhận compile + behavior.

## 3. Trust Boundaries Sealed

### 3.1 Shared Masking Backbone
- Shared service: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/masking_service.go`
- Shared free-form text sanitizer: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/text_sanitizer.go`
- Shared DI wiring: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/server/worker_server.go`
- Modules dùng cùng instance trong runtime chính:
  - `DynamicMapper`
  - `DLQHandler`
  - `SchemaInspector`
  - `ReconHealer`
  - `KafkaConsumer`
  - `BatchBuffer`
  - `ReconHandler`

### 3.2 Persistence / Retry / Recovery Paths
1. `DynamicMapper`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/dynamic_mapper.go`
   - Risk đã vá: `_raw_data` từng có nguy cơ chứa raw Mongo document chưa mask.
   - Fix: mask `rawData` trước khi `json.Marshal` vào `MappedData.RawJSON`.

2. `ReconHealer`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/recon_heal.go`
   - Risk đã vá: luồng heal có thể rebuild `_raw_data` bằng helper masking cục bộ không đồng nhất.
   - Fix: bỏ helper cũ, dùng chung `MaskingService`, chuẩn hóa `buildMaskedRawJSON(...)`.

3. `DLQHandler`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/dlq_handler.go`
   - Risk đã vá: `failed_sync_logs.RawJSON`, `failed_sync_logs.ErrorMessage`, và payload publish DLQ có thể nhận raw payload hoặc free-form error text chưa sanitize.
   - Fix: sanitize trước khi persist và trước khi publish; error text cũng đi qua shared free-form text sanitizer.

4. `KafkaConsumer.writeDLQ`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/kafka_consumer.go`
   - Risk đã vá: raw JSON trích từ Kafka message và free-form processing error từng có thể được ghi vào `failed_sync_logs` mà chưa sanitize.
   - Fix: thêm `SetMaskingService(...)` + `sanitizeDLQRawJSON(...)` trước persistence; `ErrorMessage` cũng đi qua shared free-form text sanitizer.

5. `BatchBuffer.batchUpsert` fail path
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/batch_buffer.go`
   - Risk đã vá: khi batch upsert fail, `RawData` từng có thể đi thẳng vào `failed_sync_logs`.
   - Fix: thêm `sanitizeRawData(...)` và `buildFailedSyncLog(...)` trước DB write.

6. `ReconHandler.HandleRetryFailed`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/recon_handler.go`
   - Risk đã vá: retry payload `raw_json` từ command/NATS từng được parse và upsert lại mà không re-mask.
   - Fix: `sanitizeRetryRawJSON(...)` trước parse và trước `BuildUpsertSQL(...)`; integration test xác nhận row đích chỉ nhận args đã sanitize.

7. `DLQWorker` legacy retry path
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/dlq_worker.go`
   - Risk đã vá: fallback fetch từ Mongo có thể rebuild `_raw_data` chưa mask khi retry.
   - Fix: `buildRetryRawJSON(...)` dùng `MaskingService` trước rebuilt UPSERT.

### 3.3 Schema / Alert / Sample Exposure
8. `SchemaInspector`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/schema_inspector.go`
   - Risk đã vá: `SampleValue` của field drift có thể đi vào DB/NATS alert dưới dạng raw value.
   - Fix: `maskSampleValue(...)` trước khi lưu/publish.

### 3.4 Admin / Operator Surfaces
9. `CommandHandler`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/command_handler.go`
   - Risks đã vá:
     - command result có thể log payload/result map quá chi tiết
     - error text nhiều dòng hoặc chứa ngữ cảnh nhạy cảm có thể leak sang console/logs
     - activity log có thể giữ `ErrorMessage` và `Details` chưa sanitize
     - một số admin result map từng có nguy cơ mang field như `sql`, `payload`, hoặc blob không cần thiết
   - Fix:
     - thêm `sanitizeAdminError(...)`
     - thêm `sanitizeAdminResultMap(...)`
     - thêm `sanitizeAdminFields(...)`
     - thêm `logCommandResult(...)`
     - free-form admin error text giờ redaction cả email / phone / secret-like token trước khi log/store
     - `publishResult(...)`, `publishResultWithSubject(...)`, `writeActivity(...)`, `nats_publish(...)` đều đi qua sanitize trước khi log/emit/store
     - loại bỏ chi tiết SQL khỏi result map admin của `alter-column`

10. `EventBridge`
   - File: `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/event_bridge.go`
   - Risk đã vá: bridge từng forward toàn bộ payload thay đổi sang NATS/Moleculer event (`after: data`) gây lộ raw document ngoài phạm vi cần thiết.
   - Fix: áp dụng `Data Minimization` bằng `minimizeBridgePayload(...)`, chỉ giữ metadata tối thiểu:
     - `table`
     - `count` (nếu có)
     - `op` (nếu có)
     - `source` (nếu có)
     - `record_id` (nếu suy ra được)
     - `changed_fields`
   - Raw `after` / `before` document không còn được publish.

## 4. Test Coverage Added / Updated

### 4.1 Recon / Mapping / Sample
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/recon_heal_test.go`
  - top-level masking
  - nested masking
  - array masking
  - heuristic masking
  - OCC vs masking parity
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/dynamic_mapper_test.go`
  - `_raw_data` top-level masking
  - nested + array masking
  - heuristic masking
  - upsert args nhận masked raw JSON
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/schema_inspector_test.go`
  - top-level `SampleValue`
  - nested `SampleValue`
  - array `SampleValue`
  - heuristic masking

### 4.2 DLQ / Retry / Failure Logging
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/dlq_handler_test.go`
  - `failed_sync_logs.RawJSON` top-level masking
  - nested + array masking
  - heuristic masking
  - sanitize-before-persist contract
  - free-form DLQ error redaction
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/dlq_handler_integration_test.go`
  - `sendToDLQ -> failed_sync_logs` không còn PII thô trong `RawJSON`
  - `sendToDLQ -> failed_sync_logs` không còn PII/secret thô trong `ErrorMessage`
  - `sendToDLQ -> cdc.dlq` publish payload đã sanitize trên NATS
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/kafka_consumer_dlq_test.go`
  - top-level masking
  - nested + array masking
  - heuristic masking
  - free-form processing error redaction
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/kafka_consumer_integration_test.go`
  - `writeDLQ -> failed_sync_logs` không còn PII thô trong `RawJSON`
  - `writeDLQ -> failed_sync_logs` không còn PII/secret thô trong `ErrorMessage`
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/batch_buffer_test.go`
  - top-level masking
  - nested + array masking
  - heuristic masking
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/recon_handler_test.go`
  - top-level masking
  - nested + array masking
  - heuristic masking
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/recon_handler_integration_test.go`
  - `HandleRetryFailed -> upsert` chỉ ghi phone/email đã mask vào typed columns
  - `HandleRetryFailed -> _raw_data` chỉ ghi JSON đã mask
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/service/dlq_worker_test.go`
  - top-level masking
  - nested + array masking
  - heuristic masking

### 4.3 Admin / Event Minimization
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/command_handler_test.go`
  - `sanitizeAdminError` strip newline + truncate
  - `sanitizeAdminResultMap` allowlist enforcement
  - `sanitizeAdminResultMap` sanitize error fields + sort `new_fields`
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/command_handler_activity_integration_test.go`
  - `publishResultWithSubject -> cdc_activity_log` không còn email / phone / secret thô
  - `writeActivity -> cdc_activity_log` chỉ giữ allowlisted details và redacted error text
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/event_bridge_test.go`
  - drop raw row image but keep metadata
  - channel/table fallback
  - top-level ID + `changed_fields` minimization
- `/Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/event_bridge_integration_test.go`
  - publish thực lên NATS chỉ còn metadata tối thiểu
  - downstream không nhận phone/email/balance value từ event payload

## 5. Verification Evidence

Commands run:

```bash
cd /Users/trainguyen/Documents/work/cdc-system/centralized-data-service
mkdir -p /tmp/cdc-go-cache
GOCACHE=/tmp/cdc-go-cache go test ./internal/handler ./internal/service ./internal/server
GOCACHE=/tmp/cdc-go-cache go test -tags integration ./internal/handler
```

Result:
- `ok   centralized-data-service/internal/handler`
- `ok   centralized-data-service/internal/service`
- `?    centralized-data-service/internal/server [no test files]`
- `ok   centralized-data-service/internal/handler` (integration tag)

Formatting:

```bash
gofmt -w /Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/command_handler.go \
         /Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/event_bridge.go \
         /Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/command_handler_test.go \
         /Users/trainguyen/Documents/work/cdc-system/centralized-data-service/internal/handler/event_bridge_test.go
```

## 6. Residual Risk / Follow-up Recommendations

1. `command_handler.go` hiện đã sanitize admin-facing results/logs trong các flow đã audit; nếu sau này thêm result map mới, bắt buộc phải đi qua `sanitizeAdminResultMap(...)` trước log/publish/store.
2. `event_bridge.go` hiện đi theo hướng metadata-only; nếu downstream muốn dữ liệu chi tiết hơn, nên dùng một read API có authorization thay vì re-mở raw event payload.
3. `internal/service/dlq_worker.go` là legacy path đã được vá, nhưng về lâu dài nên hợp nhất hoàn toàn về `DLQStateMachine` để giảm dual-path complexity.
4. Ancillary audit đã chuẩn hóa thêm:
   - `activity_logger.go`: sanitize nested details + error text trước khi ghi `cdc_activity_log`
   - `backfill_source_ts.go`: sanitize `recon_runs.error_message` và ActivityLog details
   - `transmute_scheduler.go`: sanitize `cdc_internal.transmute_schedule.last_error`
   - `recon_core.go`: sanitize `recon_runs.error_message` và `cdc_reconciliation_reports.error_message`
   - `transmuter.go`: audit-pass, chưa thấy trust boundary persistence/message bus mới cần patch ngay
5. Ma trận hồi quy hợp nhất hiện được duy trì tại `/Users/trainguyen/Documents/work/cdc-system/security-regression-matrix.md`.

## 7. Conclusion

Ở thời điểm hoàn tất audit này, các trust boundary runtime quan trọng nhất của CDC system đã được harden theo 2 nguyên tắc nhất quán:
- `Mask before persist / publish / retry`
- `Minimize before emit to non-owning consumers`

Điều này giảm mạnh nguy cơ Data Leakage qua `_raw_data`, `RawJSON`, `SampleValue`, admin command output, và bridge event payload, đồng thời cung cấp một regression test suite đủ mạnh để giữ behavior an toàn trong các lần refactor tiếp theo.
