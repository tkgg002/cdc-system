# Report — Flow 1 LOOP iter#6 — x2 (Muscle) — 2026-05-07 ICT

> **Author**: x2 (Muscle, cms-lane)
> **Trigger**: max-Brain ship `04_decisions_flow1_path_a_vs_b_REV2_2026-05-07.md` (11:04 ICT) recommend A3 hybrid + §5.3 yêu cầu x2 confirm constructor.
> **Iteration**: #6 (Flow 1 fork)
> **Lane lock**: cms-only

## §1 Executive summary

iter#6 = **info-tier read-only investigation** confirm max REV2 §5 plan implementable. KHÔNG code change. KHÔNG plan revision (per L-MUSCLE-PLAN-PROHIBITION).

| Decision | Status |
|---|---|
| max REV2 doc | ✅ shipped 11:04 (10405 bytes) |
| x2 confirm §5.3 (ShadowAutomator constructor) | ✅ DONE |
| x2 ready execute A3 | ⏳ đợi Boss approve gate |

## §2 max REV2 doc consumption

x2 đọc full `04_decisions_flow1_path_a_vs_b_REV2_2026-05-07.md`:
- §0 Recommendation đảo từ A4+A1 → **A3 hybrid** ✅.
- §1 Iter#3 errata: max-Brain admit decision iter#3 §1.5 + §1.3 + §1.4 SAI fact ✅.
- §2 Consolidated evidence: confirm 5 evidence x2 §8 collected iter#4 ✅.
- §3 Decision matrix: A3 RECOMMENDED, A1 REVOKED, A4 REVOKED, A2 partial reject ✅.
- §5 Implementation plan: 5 steps (config + boot + ShadowAutomator inject + migration + smoke) ✅.
- §6 Boss decision: G-7 P0 unchanged + G-8 A3 NEW + A1 REVOKE ✅.
- §7 Open questions Q-1..Q-4 cho Boss.
- §8 Lesson candidate `L-DECISION-DOC-FACT-CHECK-DRIFT`.

## §3 ShadowAutomator + boot wiring investigation (factual)

### §3.1 Constructor signature — already accepts `*gorm.DB`

`internal/infra/persistence/shadow_automator.go:26`:
```go
func NewShadowAutomator(db *gorm.DB, logger *zap.Logger) *ShadowAutomator { ... }
```

→ ✅ Match max §5.3. KHÔNG cần refactor signature.

### §3.2 Single call site

`internal/server/server.go:198`:
```go
shadowAutomator := persistence.NewShadowAutomator(db, logger)
```

`db` = global control plane gorm session (Path A 5433 cdc_dw, opened earlier từ `cfg.DB`).
→ Đây là **điểm duy nhất** cần đổi inject `shadowDB` riêng.

### §3.3 Config schema gap (drift confirmed)

`config/config.go:16-23`:
```go
type AppConfig struct {
    DB DBConfig `mapstructure:"db"`   // ← Path A only
    // ❌ MISSING: ShadowDB DBConfig `mapstructure:"shadowDb"`
}
```

`config/config-local.yml`:
- ✅ `db:` block (host=localhost port=5433 database=cdc_dw user=gpay_admin) — Path A control plane.
- ❌ MISSING `shadowDb:` block. ShadowAutomator vì vậy fallback global `db` = Path A → orphan tables.

→ Confirm max REV2 §2.6 cms config drift evidence.

### §3.4 Effort precise (refine max §5 conservative estimate)

| Step | Effort | Touch |
|---|---|---|
| 1. Add `ShadowDB DBConfig \`mapstructure:"shadowDb"\`` field vào `AppConfig` | 5 min | `config/config.go` |
| 2. Add `shadowDb:` block (port=5436 db=cdc_shadow) vào `config-local.yml` | 5 min | `config/*.yml` (3 file) |
| 3. Open 2nd gorm session `shadowDB := openGorm(cfg.ShadowDB)` cạnh existing `db` | 10 min | `server.go` boot |
| 4. Đổi `server.go:198` → `NewShadowAutomator(shadowDB, logger)` | 1 min | `server.go:198` |
| 5. (Optional) env override `CDC_SHADOW_DB_URL` parse → fallback host/port (giống worker `.env:7` pattern) | 15 min | `config/config.go` |
| 6. `go build && go vet && go test ./...` | 5 min | — |
| 7. Smoke test (rebuild cms + Register source 49 qua Phương án Z + verify Path B 5436 table create) | 30 min | runtime |
| **TOTAL** | **~70 min** | — |

→ Max §5 estimate 4-6h là **conservative**. Refactor narrow, không touch hexagonal app/domain layer.

### §3.5 Risk assessment (refine max §5 "Medium")

| Risk | Verdict |
|---|---|
| Migration data move? | ❌ NO — Path A 0-row tables DROP an toàn, Path B 1720 rows keep nguyên. |
| Existing tests fail? | LOW — ShadowAutomator unit test pass `*gorm.DB` mock, sẽ pass cả 2 connection. |
| Phương án Y (Phase 2) depends? | ❌ NO — orthogonal refactor admin endpoint. |
| G-7 worker enable depends? | ❌ NO — G-7 worker-lane env var, parallel cms A3. |
| Server boot order? | LOW — 2 gorm sessions independent, init parallel. |
| **Overall risk** | **Low** (vs max §5 "Medium") |

## §4 Boss output integrity (iter#6)

```
$ docker exec gpay-postgres-shadow psql -U gpay_admin -d cdc_shadow -tAc \
    "SELECT count(*) FROM shadow_payment_bill_service.refund_requests"
1720
$ curl http://localhost:8083/health
{"service":"cdc-cms","status":"ok"}
```

→ ✅ Boss output 1720 rows persist iter#0 → iter#6.

## §5 Files modified iter#6

| Repo | File | Change |
|---|---|---|
| cdc-system | (none committed — info tier only) | — |
| cdc-system | `cdc-cms-service/report_flow1_loop_iter6_x2_2026-05-07.md` | new (this file, untracked) |
| agent | `agent/memory/workspaces/feature-cdc-system-refactor/05_progress.md` | append iter#6 |
| agent | `agent/memory/workspaces/feature-cdc-system-refactor/09_tasks_solution_flow1_x2_2026-05-07.md` | append §10 |
| agent | `agent/memory/workspaces/feature-cdc-system-refactor/coordination_max_x2_2026-05-07.md` | append iter#6 ACK + investigation result |

## §6 Boss escalation iter#6 (3 outstanding decisions)

| # | Pri | Decision | x2 commitment post-approve |
|---|---|---|---|
| 1 | **P0** | G-7 worker enable | x2 verify state machine advance `shadow_active` (5 min) |
| 2 | **P1** | Swap cms binary `/tmp/cdc-cms-service-flow1.new` → `/tmp/cdc-cms-service-flow1` | x2 verify health post-restart + smoke G-10 (`pk_type='string'` Register) (10 min) |
| 3 | **P1 NEW** | A3 hybrid (max REV2 §3 + §5) | x2 thi công 6 steps refactor (~70 min) + report iter#7 |
| 4 | HOLD | A1 destructive cleanup | ⛔ REVOKED iter#4 |

## §7 Workflow gate audit iter#6

| Gate | x2 status |
|---|---|
| L-MUSCLE-PLAN-PROHIBITION | ✅ KHÔNG draft `02_plan_*` / `03_implementation_*` / `08_tasks_*`. Chỉ append `09_tasks_solution §10` (info tier). |
| Lane lock cms-only | ✅ Read-only investigation cms code. |
| Auto Mode safety | ✅ HOLD config schema change đợi Boss approve (shared system effect). |
| APPEND-only memory | ✅ 4 file append. |
| Verify before done | ✅ Health probe + DB count + grep evidence. |
| Boss directive "bằng mọi giá lên Flow 1" | ⏳ 1720 rows persist; G-7 + binary swap + A3 đều outstanding. |

## §8 Skills used iter#6

- Bash (grep, find, ls, docker exec psql, curl, date, cat append, file size compare)
- Read (`04_decisions_flow1_path_a_vs_b_REV2_2026-05-07.md` full, `config/config-local.yml`, `internal/server/server.go:170-228`, `config/config.go:1-100`, `shadow_automator.go:26`)
- Write (this report)
- §0 tiếng việt + skills tail
- §3 Plan & Verify (factual investigation 4 evidence + effort precise + risk refine)
- §6 Simplicity & Demand Elegance (constructor đã đúng — không over-engineer signature)
- §7 APPEND-only memory + Knowledge Retention
- §8 Security/Escalation (escalate Boss approve gate cho A3, không tự thi công)
- §10 Lane lock cms-only
- §11 APPEND-only memory (3 file workspace + 1 report cms repo)
- §12 Brain Code Prohibition không áp dụng (x2 = Muscle, nhưng cũng không thi công source code iter#6 vì đợi Boss approve)
- §14 Pre-flight Check (health + DB count + git head + binary timestamp)
- L-MUSCLE-PLAN-PROHIBITION (KHÔNG draft `02_plan_*` revision dù evidence đủ — defer max REV2)
- Auto-mode safety (HOLD destructive `kill PID` iter#5 unchanged + HOLD config schema change iter#6)

— x2 (loop iter #6)
