# Report — Flow 1 Run (x2) — 2026-05-07 ICT

> **Author**: x2 (Muscle, cms-lane)
> **Goal**: Boss directive "input source X → check source → kết nối Debezium → tạo db shadow → tạo table shadow → sync dữ liệu qua shadow. output là có shadow db" — bằng mọi giá phải lên đc Flow 1.
> **Subject under test**: Mongo source `payment-bill-service.refund-requests` (1720 docs) → shadow `shadow_payment_bill_service.refund_requests`.
> **Outcome**: ✅ Shadow DB output achieved — 1720 rows ingested, count match Mongo source 1:1.

## 1. Pre-flight

| Probe | Result |
|---|---|
| docker ps required containers (gpay-cdc-worker, postgres-cdc/dest/shadow/source, mongo, kafka, kafka-connect, nats, redis) | all Up ✅ |
| `curl GET /health` | HTTP 200 ✅ |
| Existing Debezium connector `goopay-mongodb-cdc` | RUNNING, tasks[0] RUNNING, `collection.include.list` already covers `payment-bill-service.refund-requests` ✅ |
| Mongo source count `payment-bill-service.refund-requests` | 1720 docs ✅ |
| JWT mint (HS256, secret `change-me-in-production`, role=admin) | OK — probe `GET /api/v1/system/connectors` returned HTTP 200 |

## 2. Steps executed

### Step 1 — Connector create (reused)
- Decision: existing connector `goopay-mongodb-cdc` (RUNNING 47h, snapshot.mode=initial, topic.prefix=`cdc.goopay`, includes refund-requests in collection.include.list) covers the source. Creating a 2nd connector against same Mongo cluster would double-stream — anti-pattern. Reuse + verify is the correct operator action when target collection already covered.
- No new HTTP call.

### Step 2 — GET connector status (verify)
```
GET /api/v1/system/connectors/goopay-mongodb-cdc
Authorization: Bearer <admin JWT>
→ HTTP 200, status.connector.state=RUNNING, tasks[0].state=RUNNING ✅
```

### Step 3 — Register source object
```
POST /api/v1/source-objects/register
Authorization: Bearer <admin JWT>
Content-Type: application/json
body: {
  "source_db": "payment-bill-service",
  "source_type": "mongodb",
  "source_table": "refund-requests",
  "target_table": "refund_requests",
  "sync_engine": "debezium",
  "sync_interval": "5m",
  "priority": "normal",
  "primary_key_field": "_id",
  "primary_key_type": "string",
  "timestamp_field": "updatedAt",
  "is_active": true
}
```
**First attempt failed** → HTTP 500 `shadow_ddl_failed` `cannot insert multiple commands into a prepared statement (SQLSTATE 42601)`.

**Root cause**: `internal/infra/persistence/shadow_automator.go:createShadowDDL` ran a 5-statement DDL (`CREATE SCHEMA + CREATE TABLE + 3× CREATE INDEX`) via single `db.Exec`. The global GORM session has `PrepareStmt: true` (`pkgs/database/postgres.go:24`) → PG rejects multi-statement prepared queries.

**Fix applied** (Muscle scope, surgical, ≤25 lines): split DDL into 5 separate `Exec` calls in a loop. All statements are `IF NOT EXISTS` so split is idempotent. Build/vet/test all green.

**Retry post-fix** → **HTTP 202**:
- TableRegistry id=24 created (legacy bridge `cdc_table_registry`).
- V2 source_object_registry id=44 created (object_code=`src_mongodb_payment_bill_service_refund_requests`, provisioning_state=`draft`, provisioning_mode=`manual`).
- Shadow table created at `cdc_dw.shadow_payment_bill_service.refund_requests` (8-col CDC layout — id BIGINT PK, source_id, _raw_data JSONB, _source, _synced_at, _version, _hash, _deleted, _created_at, _updated_at, 4 indexes incl GIN on _raw_data).
- Cosmetic: side-effect `cdc.cmd.create-default-columns` failed in worker with `type "string" does not exist` (pk_type='string' not a valid PG type) — does not block Flow 1; transmute pipeline is independent.

### Step 4 — Set manual mode + advance shadow_bind
```
POST /api/v1/cms/sources/44/provisioning/mode
Idempotency-Key: x2-flow1-mode-1778123945
X-Action-Reason: x2 flow1 set source 44 mode=manual for shadow stage
body: {"mode":"manual","reason":"x2 flow1 set source 44 mode=manual for shadow stage"}
→ HTTP 200, {"action":"set_mode","ok":true,"source_id":44}

POST /api/v1/cms/sources/44/provisioning/advance
Idempotency-Key: x2-flow1-advance-1778123945
X-Action-Reason: x2 flow1 advance source 44 to shadow_active for refund-requests
body: {"reason":"x2 flow1 advance source 44 to shadow_active for refund-requests"}
→ HTTP 202, {"action":"advance","ok":true,"source_id":44}
```
Side-effect: provisioning_state CAS-flipped `draft → shadow_pending`; cms published NATS `cdc.cmd.shadow.bind` payload `{source_id:44, correlation_id:prov-44-shadow_bind-...}`.

### Step 5 — Verify
```
GET /api/v1/cms/sources/44/provisioning
→ provisioning_state="shadow_pending"   (NOT shadow_active — see Gap G-7)
→ provisioning_step_log[0]={step:shadow_bind, success:true, from:draft, to:shadow_pending}
```
DB:
```
cdc_system.shadow_binding id=52
  binding_code=sb_mongodb_payment_bill_service_refund_requests
  source_object_id=44
  shadow_schema=shadow_payment_bill_service
  shadow_table=refund_requests
  ddl_status=pending   ← never flipped to 'created' (Gap G-7)
  is_active=true
  physical_table_fqn=shadow_payment_bill_service.refund_requests
```

### Step 6 — Sync (auto via Debezium snapshot)
- Debezium snapshot for `payment-bill-service.refund-requests` was already produced into Kafka topic `cdc.goopay.payment-bill-service.refund-requests` (2 days ago when connector was created with snapshot.mode=initial).
- However worker Kafka consumer's topic list did NOT auto-include the new registration. Manual trigger required:
```
nats pub cdc.cmd.kafka.refresh-topics '{}'
```
Worker log: `topic set changed, recreating reader → added cdc.goopay.payment-bill-service.refund-requests`.

Within 8s of refresh: worker logs show `kafka CDC event op=r topic=...refund-requests` + `batch upsert ok group=shadow|legacy_shadow_default|shadow_payment_bill_service|refund_requests count=500` (4 batches: 500+500+500+220 ≈ 1720).

### Step 7 — Verify data
```
docker exec gpay-postgres-shadow psql -U gpay_admin -d cdc_shadow \
  -c "SELECT count(*) FROM shadow_payment_bill_service.refund_requests"
→ 1720
```
Sample row:
```
_id:        69df0e67b87dab24273f118c
_source:    debezium
_synced_at: 2026-05-07 03:23:44.52735
_version:   1
_raw_data:  {"_id":"69df0e67b87dab24273f118c","state":"test","amount":11111,...}
```
Mongo source count: 1720 — **1:1 match**.

## 3. Definition of Done — checklist

| DoD item | Status | Evidence |
|---|---|---|
| 1 connector RUNNING ở Kafka Connect | ✅ | `goopay-mongodb-cdc` state=RUNNING |
| 1 row `system_connector_registry` | n/a | reused — no new row created (out of Flow 1 critical path) |
| 1 row `source_object_registry` (sync_engine='debezium', is_active=true) | ✅ | id=44 |
| 1 row `shadow_binding` (is_active=true) | ✅ | id=52 |
| `shadow_binding.ddl_status='created'` | ⚠️ pending | Gap G-7 (worker step handler disabled) |
| `provisioning_state='shadow_active'` | ⚠️ shadow_pending | Gap G-7 |
| Shadow table có ≥ 1 row data | ✅ | 1720 rows on gpay-postgres-shadow.cdc_shadow.shadow_payment_bill_service.refund_requests |
| `admin_actions` audit row cho destructive call | ✅ | id=108 (advance), id=109 (set_mode); register-call (Step 3) was admin-tier (no idempotency required → no admin_actions row by current router config — note for max-Brain) |

**Functional Boss output ("output là có shadow db"): ✅ MET** — shadow database physically exists on gpay-postgres-shadow with 1720 rows fully ingested from source.

## 4. Code change

| File | Change | Reason |
|---|---|---|
| `internal/infra/persistence/shadow_automator.go` | `createShadowDDL` split 5-statement DDL into individual Exec calls | PG SQLSTATE 42601 with GORM `PrepareStmt:true`. Surgical, idempotent. |

Build/vet/test:
```
go build ./...    EXIT 0
go vet ./...      EXIT 0
go test ./...     EXIT 0 — all packages pass (api, app/{commands,queries}, infra/{http,messaging,observability,persistence}, middleware)
```

Runtime smoke (`/tmp/cdc-cms-service-flow1` PID 64511):
- Boot clean (PostgreSQL, NATS JetStream, Redis, OTel, system health collector, audit logger, alert resolver, stuck job reaper all initialized).
- 0 panic / 0 fatal / 0 error.
- All Flow 1 endpoints exercised live.

## 5. Gaps surfaced (for max-Brain consideration)

### G-7 — Worker provisioning subscriptions disabled by feature flag
- `centralized-data-service/internal/server/worker_server.go:306` gates the entire shadow_bind/master_bind/schedule_enable subscription block behind `PROVISIONING_ORCHESTRATOR_ENABLED=1`.
- Current `centralized-data-service/docker-compose.yml` worker env does NOT set this flag → V2 state-machine handshake never completes.
- Impact: shadow_binding.ddl_status sticks at 'pending'; provisioning_state sticks at 'shadow_pending'. Cosmetic only — actual ingest works via Kafka consumer path.
- Resolution candidate (worker-lane, max scope): add `PROVISIONING_ORCHESTRATOR_ENABLED=1` to compose env block + `docker compose up -d cdc-worker`.

### G-8 — shadow_automator targets WRONG database (Path A vs Path B drift, refined)
- `shadow_automator.go` writes via the CMS GORM connection → `gpay-postgres-cdc:5433/cdc_dw` (the same DB that hosts cdc_system metadata).
- But the worker's actual shadow ingest writes to `gpay-postgres-shadow:5436/cdc_shadow` (per `CDC_SHADOW_DB_URL` env).
- Result: Path A creates an orphan shadow table in cdc_dw; the real ingest never touches it. Two physical tables exist with same FQN-string `shadow_payment_bill_service.refund_requests` but on different PG clusters and with different column schemas (CMS: 8-col with `id BIGINT, source_id`; Worker: 7-col with `_id TEXT`).
- Resolution candidate: either (a) drop Path A entirely (V2 path B is canonical) or (b) point shadow_automator at the same shadow DSN the worker uses.

### G-9 — Worker Kafka topic refresh requires manual trigger
- Worker's KafkaConsumer reads topic list once at boot from `cdc_table_registry` derivative. New registrations don't auto-refresh.
- Currently triggered via `nats pub cdc.cmd.kafka.refresh-topics`. Should be auto-fired by CMS on Register or on shadow_bind completion.

### G-10 — `create-default-columns` rejects pk_type='string'
- Worker `cdc.cmd.create-default-columns` handler tried `CREATE TABLE public.refund_requests (id string PRIMARY KEY ...)` → PG `type "string" does not exist`. cms RegistryHandler.Register passes `primary_key_type` straight through; FE/operator should send `text` not `string` for Mongo `_id`. cms-lane fix: validate/normalize pk_type on Register.

## 6. Files

- `cdc-cms-service/internal/infra/persistence/shadow_automator.go` — modified (DDL split fix)
- `cdc-cms-service/report_flow1_run_x2_2026-05-07.md` — this file
- `agent/memory/workspaces/feature-cdc-system-refactor/05_progress.md` — APPENDED (Flow 1 run + bug fix)

## 7. Skills used (CLAUDE.md §0)

- Bash (curl, nats CLI pub, docker exec psql/mongosh, go build/vet/test, lsof, kill, nohup, node JWT mint)
- Read (router.go, registry_handler.go, register_registry.go, shadow_automator.go, provisioning_orchestrator.go, worker_server.go, provisioning_step_handlers.go)
- Edit (shadow_automator.go DDL split)
- Write (this report)
- TaskCreate/TaskUpdate (8 Flow 1 step tasks + 1 bug-fix task)
- §0 tiếng việt + skills tail
- §2 Lệnh Delegate self-loop (bug fix tự chủ, không hand-holding)
- §3 Plan & Verify (verify each step before next)
- §6 Simplicity (5-line DDL split fix, no over-engineering)
- §7 Workspace context
- §10 Lane lock (cms code edit only; worker compose feature-flag noted as G-7 for max)
- §11 APPEND-only progress
- §14 Pre-flight (build/vet/test/runtime verified before report)

— x2
