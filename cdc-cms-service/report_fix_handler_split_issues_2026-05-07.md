# Report: Fix 3 Issues Post Handler-Split Refactor

**Date:** 2026-05-07 ICT
**Operator:** Muscle (CC CLI) — model `claude-opus-4-7`
**Component:** `cdc-cms-service` / `internal/api/*.go`
**Trigger:** Boss yêu cầu "fix hết những gì mày nói" sau khi review report `report_master_registry_handler_split_2026-05-07.md` từ 1 model AI khác chuyên nghiệp.

---

## 1. Bối cảnh

Một model AI khác đã hoàn tất Phase 2 Decoupling — split 4 handler monolithic thành các thin adapters ≤100 dòng:

| Handler gốc | Số file split | Max dòng | Status |
|-------------|---------------|----------|--------|
| `master_registry_handler.go` (~600) | 7 | 97 | OK |
| `mapping_rule_handler.go` (~659) | 5 | 67 | OK |
| `reconciliation_handler.go` (~638) | 7 | 98 | OK |
| `registry_handler.go` (~684) | 9 | 89 | OK |

Tổng: 1 939 dòng / 28 file (gồm 1 file test 90 dòng).

Tôi review thì flag 3 vấn đề:
1. `TestUpdateStatus_MissingStatus` panic do unsafe type-assertion + thiếu validation.
2. `reconciliation_drift_test.go` không còn ở `internal/api/` → nghi ngờ regression.
3. `error_messages_vi.go` không còn ở `internal/api/` → nghi ngờ regression.

---

## 2. Verification trước fix — Kiểm chéo issue #2 và #3 (FALSE ALARM)

Tôi re-Read source và xác nhận:

| Issue | Tên file flag | Vị trí mới | Kết luận |
|-------|--------------|-----------|----------|
| #2 | `reconciliation_drift_test.go` | `internal/app/queries/recon_enrichment_test.go` (`TestComputeDriftStatus` line 12 + `TestErrorMessagesVICoverage` line 62) | MOVED — đúng pattern CQRS, không phải xóa |
| #3 | `error_messages_vi.go` | `internal/app/queries/recon_enrichment.go` (map `ErrorMessagesVI` line 14) | MOVED — đúng pattern CQRS, không phải xóa |

→ Bài học: trước khi declare regression, phải `grep -r <symbol_name>` toàn repo, không chỉ check thư mục gốc. **Verify content > verify location.**

→ **Chỉ issue #1 là regression thật.**

---

## 3. Issue #1 — Root cause phân tích

### Hiện tượng

```
=== FAIL: TestUpdateStatus_MissingStatus
panic: interface conversion: interface {} is nil, not string
    at internal/api/mapping_rule_handler_commands.go:56
```

### Nguyên nhân

Hai bug đan vào nhau:

**(a) Thiếu validation theo test contract.**

Test mong muốn:
```go
// mapping_rule_handler_test.go (đã có sẵn)
body := `{"status":""}`
// expect: HTTP 400 + body {"error":"status is required"}
```

Nhưng `UpdateStatus` chỉ check `BodyParser` lỗi, không check `body.Status == ""`. Khi `status` rỗng, code vẫn đi tiếp tới `c.Locals("username")` → panic.

**(b) Unsafe type-assertion trong 5 vị trí.**

Pattern unsafe:
```go
username := c.Locals("username").(string)  // panic khi Locals trả nil (test không set)
```

Pattern safe (comma-ok):
```go
username, _ := c.Locals("username").(string)  // nil → username = ""
```

5 vị trí dính:
- `mapping_rule_handler_commands.go:38` (Reload)
- `mapping_rule_handler_commands.go:56` (UpdateStatus — đây là điểm panic nguyên gốc)
- `mapping_rule_handler_create.go:34` (Create)
- `mapping_rule_handler_batch.go:19` (Backfill)
- `mapping_rule_handler_batch.go:32` (BatchUpdate)

---

## 4. Files Modified (4 file, 6 thay đổi)

| File | Vị trí | Loại fix |
|------|--------|----------|
| `internal/api/mapping_rule_handler_commands.go` | line 38 (Reload) | Safe-cast comma-ok |
| `internal/api/mapping_rule_handler_commands.go` | line 56-58 (UpdateStatus) | THÊM `if body.Status == "" { return 400 "status is required" }` |
| `internal/api/mapping_rule_handler_commands.go` | line 59 (UpdateStatus) | Safe-cast comma-ok |
| `internal/api/mapping_rule_handler_create.go` | line 34 (Create) | Safe-cast comma-ok |
| `internal/api/mapping_rule_handler_batch.go` | line 19 (Backfill) | Safe-cast comma-ok |
| `internal/api/mapping_rule_handler_batch.go` | line 32 (BatchUpdate) | Safe-cast comma-ok |

### Diff minh hoạ (UpdateStatus)

```go
// BEFORE
func (h *MappingRuleHandler) UpdateStatus(c *fiber.Ctx) error {
    id, _ := c.ParamsInt("id")
    var body struct{ Status string `json:"status"` }
    if err := c.BodyParser(&body); err != nil {
        return c.Status(400).JSON(fiber.Map{"error": err.Error()})
    }
    username := c.Locals("username").(string)  // ← PANIC khi nil
    cmd := commands.UpdateMappingRuleCommand{ID: int64(id), Status: body.Status, ...}
    ...
}

// AFTER
func (h *MappingRuleHandler) UpdateStatus(c *fiber.Ctx) error {
    id, _ := c.ParamsInt("id")
    var body struct{ Status string `json:"status"` }
    if err := c.BodyParser(&body); err != nil {
        return c.Status(400).JSON(fiber.Map{"error": err.Error()})
    }
    if body.Status == "" {                                                    // ← THÊM
        return c.Status(400).JSON(fiber.Map{"error": "status is required"})  // ← THÊM
    }                                                                         // ← THÊM
    username, _ := c.Locals("username").(string)  // ← SAFE
    cmd := commands.UpdateMappingRuleCommand{ID: int64(id), Status: body.Status, ...}
    ...
}
```

Minimal-impact patch (CLAUDE.md §6 Simplicity First): chỉ chạm 2 line/handler, không refactor mở rộng.

---

## 5. Verification sau fix (REAL EVIDENCE)

### 5.1 Grep counts

```bash
$ grep -rn 'username := c.Locals("username").(string)' internal/api/
# → 0 hit (zero unsafe)

$ grep -rn 'username, _ := c.Locals("username").(string)' internal/api/
internal/api/mapping_rule_handler_commands.go:38
internal/api/mapping_rule_handler_commands.go:59
internal/api/mapping_rule_handler_batch.go:19
internal/api/mapping_rule_handler_batch.go:32
internal/api/mapping_rule_handler_create.go:34
# → 5 hit (đủ và đúng)
```

### 5.2 Build / vet / test

| Command | Exit | Thời gian | Kết quả |
|---------|------|-----------|---------|
| `go build ./...` | 0 | — | PASS |
| `go vet ./...` | 0 | — | PASS |
| `go test ./internal/api/... -count=1` | 0 | 0.407s | `ok cdc-cms-service/internal/api` |

### 5.3 Service health (live)

```bash
$ curl -fsS http://127.0.0.1:8083/health
{"service":"cdc-cms","status":"ok"}    # exit=0

$ curl -fsS http://127.0.0.1:8082/health
{"service":"cdc-worker","status":"ok"} # exit=0
```

cms 8083 + worker 8082 alive.

### 5.4 Line counts (DoD ≤100)

```
internal/api/mapping_rule_handler.go            19
internal/api/mapping_rule_handler_batch.go      59
internal/api/mapping_rule_handler_commands.go   67
internal/api/mapping_rule_handler_create.go     63
internal/api/mapping_rule_handler_list.go       58
internal/api/master_registry_handler.go         55
internal/api/master_registry_handler_approve.go 97
internal/api/master_registry_handler_create.go  93
internal/api/master_registry_handler_read.go    31
internal/api/master_registry_handler_resolve.go 59
internal/api/master_registry_handler_swap.go    73
internal/api/master_registry_handler_toggle.go  52
internal/api/reconciliation_handler.go          70
internal/api/reconciliation_handler_backfill.go 98
internal/api/reconciliation_handler_commands.go 82
internal/api/reconciliation_handler_heal.go     47
internal/api/reconciliation_handler_reports.go  71
internal/api/reconciliation_handler_retry.go    60
internal/api/reconciliation_handler_tools.go    44
internal/api/registry_handler.go                50
internal/api/registry_handler_bulk.go           78
internal/api/registry_handler_dispatch.go       64
internal/api/registry_handler_read.go           58
internal/api/registry_handler_register.go       78
internal/api/registry_handler_tools_columns.go  87
internal/api/registry_handler_tools_scan.go     89
internal/api/registry_handler_transform.go      68
internal/api/registry_handler_update.go         79
```

Max 98 dòng (`reconciliation_handler_backfill.go`) → đáp ứng DoD `≤100 lines`.

---

## 6. Risk & Blast Radius

| Mục | Đánh giá |
|-----|----------|
| Reversibility | HIGH — chỉ 6 thay đổi nhỏ, dễ revert qua git diff |
| Behavioral change | (1) UpdateStatus với `status=""` → 400 thay vì panic 500 (đúng test contract); (2) các handler khác không đổi behavior, chỉ tránh panic khi `Locals("username")` nil — production luôn có middleware set sẵn nên không có behavior khác trong runtime thực |
| Sandbox impact | ZERO — production :8083 KHÔNG restart; chỉ rebuild + test |
| Lane | cms-lane only (CLAUDE.md §10) |
| Memory | APPEND-only `05_progress.md` (CLAUDE.md §11) |

---

## 7. Skills sử dụng (CLAUDE.md §0)

- **§3 Plan & Verify (Deep Execution)** — verify trước fix (re-Read 4 handler + grep symbols), verify sau fix (build/vet/test/curl với real-evidence)
- **§4 Deep Execution (Agent-within-Agent)** — không dùng sub-agent (task minimal-impact); thay bằng grep + Read trực tiếp
- **§6 Simplicity First & Minimal Impact** — chỉ chạm 6 dòng, không refactor mở rộng
- **§9 Workspace-First / Double-Verification** — kiểm chéo issue #2 và #3 trước khi escalate, tránh false alarm
- **§11 Memory APPEND-only** — `05_progress.md` chỉ APPEND, không overwrite
- **§13 Lesson Writing** — `L-PLAN-VS-IMPL-MISREAD-DRIFT` đã APPEND vào `lessons.md` trước đó (lesson tổng quát hoá: distinguish plan-tier vs impl-tier DoD)
- **§14 Governance Pre-flight** — quét rule trước khi đóng task (Memory APPEND ✓, Report file ✓, Skills list ✓)

---

## 8. Files Changed Summary (cho audit)

```
internal/api/mapping_rule_handler_commands.go  | +3 -1 (validation + 2 safe-cast)
internal/api/mapping_rule_handler_create.go    | +1 -1 (safe-cast)
internal/api/mapping_rule_handler_batch.go     | +2 -2 (2 safe-cast)
agent/memory/workspaces/feature-cdc-system-refactor/05_progress.md | APPEND 1 row
cdc-cms-service/report_fix_handler_split_issues_2026-05-07.md      | NEW (this file)
```

**Total: 4 source file modified, 6 line touched, 0 file created in source tree, 1 progress entry APPEND, 1 report file NEW.**

---

## 9. Trạng thái

- [x] Issue #1 fix (UpdateStatus validation + 5 safe-cast)
- [x] Issue #2 verify FALSE alarm (file moved → app/queries)
- [x] Issue #3 verify FALSE alarm (file moved → app/queries)
- [x] `go build ./...` PASS
- [x] `go vet ./...` PASS
- [x] `go test ./internal/api/... -count=1` PASS
- [x] cms :8083 `/health` 200
- [x] worker :8082 `/health` 200
- [x] Memory APPEND `05_progress.md`
- [x] Report file written

**Status: DONE — Verified.**
