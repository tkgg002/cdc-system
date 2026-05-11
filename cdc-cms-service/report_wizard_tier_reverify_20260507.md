# Report — Wizard Tier-Classification Re-Verification (2026-05-07)

## Mục đích
Re-verify plan `eager-orbiting-wave.md` sau khi session bị compact. Plan đã hoàn tất trong phiên trước (2026-05-06); phiên này chỉ chạy lại test matrix để chứng minh fix vẫn còn hiệu lực với BE binary hiện tại.

## Tóm tắt fix (đã làm trong phiên trước, không thay đổi trong phiên này)
- **Bug**: FE bootstrap `POST /v1/wizard/sessions` rớt `400 missing Idempotency-Key` vì route bị mount nhầm vào destructive chain.
- **Root cause**: tier-classification error — Create/Patch (draft mutations) bị xếp chung với Execute/Swap (destructive).
- **Fix**:
  - `internal/router/router.go` → Create + Patch chuyển sang `admin.Post / admin.Patch` (chỉ JWT + role admin); Execute + Swap giữ `registerDestructive`.
  - `cdc-cms-web/src/pages/SourceToMasterWizard.tsx` → bỏ `Idempotency-Key` cho Create/Patch; Execute thêm `{reason}` body + `Idempotency-Key` + `X-Action-Reason` headers.
  - `agent/memory/global/lessons.md` → append lesson "Route classification".

## Test Matrix — chạy lại 2026-05-07 08:50 (port 8083)
| # | Endpoint | Headers/body | Expected | Got |
|---|----------|--------------|----------|-----|
| 1 | `POST /api/v1/wizard/sessions` | JWT, body `{}`, **không** Idempotency-Key | 201 | **201** `{id:85f385b1-…, current_step:0, status:"draft"}` |
| 2 | `PATCH /api/v1/wizard/sessions/85f385b1-…` | JWT, body `{master_name:"public_user_e2e"}` | 200 | **200** body `master_name=public_user_e2e` |
| 3 | `POST /api/v1/wizard/sessions/85f385b1-…/execute` | JWT + Idempotency-Key, body `{}` | 400 | **400** `{error:"missing or too-short reason", min_length:10}` |
| 4 | `POST /api/v1/wizard/sessions/85f385b1-…/execute` | JWT + Idempotency-Key + X-Action-Reason + body `{reason:"automate source=test_src master=public_user_e2e via wizard"}` | 200/202 | **202** `{session_id, status:"running"}` |

## DB Verification
```sql
-- cdc_dw schema cdc_system (cdc_internal đã được drop, dùng cdc_system)
SELECT id, current_step, status, master_name
  FROM cdc_system.cdc_wizard_sessions
 WHERE id='85f385b1-678b-4529-8887-2c169a013866';
-- → current_step=1, status=running, master_name=public_user_e2e ✅

SELECT action, created_at
  FROM cdc_system.admin_actions
 WHERE created_at >= NOW() - INTERVAL '15 minutes'
 ORDER BY created_at DESC;
-- → 1 row: post__api_wizard_sessions_id_execute (success)
-- → 0 row cho create/patch ✅ (tier split effective)
```

## Các file vật lý đã sửa/thêm trong phiên này
- `agent/memory/workspaces/feature-cdc-integration/05_progress.md` — append entry "2026-05-07 08:50 — Wizard tier-classification re-verification (post-compact)" (APPEND-only theo §11).
- `cdc-cms-service/report_wizard_tier_reverify_20260507.md` (file này).

KHÔNG sửa file source code trong phiên này.

## Phát hiện phụ
- Schema `cdc_internal` đã bị drop khỏi `cdc_dw`. Schema còn lại: `cdc_system`, `public`, `shadow_*`. `project_context.md` cũ vẫn ghi `cdc_internal` — cần update khi đụng task khác.
- BE đang chạy port 8083, container DB là `gpay-postgres-cdc` user `gpay_admin` db `cdc_dw`.

## Skills/công cụ đã dùng
- Bash (curl × 4, docker exec psql × 4, node JWT mint)
- Edit / Write (APPEND vào progress, tạo report file mới)
- Read (offset-bounded — `05_progress.md` lớn)
- Plan-mode resume (theo system-reminder `eager-orbiting-wave.md`)
- Governance: §0 tiếng việt, §3 verify-before-done, §11 append-only memory, §14 pre-flight scan.
