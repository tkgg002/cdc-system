# Progress Log - CDC Pipeline Fixes

## [2026-05-11 09:53] [Agent: Antigravity] Full Pipeline Recovery

### 1. Root Cause Analysis (Governance)
- **Problem A**: `Transmuter` crashing on `SQLSTATE 22P02` because non-numeric strings ("501/2") were being passed to `NUMERIC` columns.
- **Problem B**: `database or mongodb not configured` during snapshots because `cdc-cms-service` wasn't sending the database/collection metadata.
- **Problem C**: Shadow insertions failing with `SQLSTATE 42703` because the worker was trying to insert into an `_id` column instead of the mapped `id` column in Postgres.

### 2. Actions Taken
- [x] Modified `centralized-data-service/internal/service/type_resolver.go` to enforce `strconv.ParseFloat` validation on numeric types.
- [x] Modified `centralized-data-service/internal/handler/event_handler.go` to map `_id` -> `id` for primary keys in `processEvent`.
- [x] Updated `cdc-cms-service/internal/api/reconciliation_handler_tools.go` to parse and send `database`/`collection` metadata.
- [x] Updated `cdc-cms-service/internal/app/commands/recon_async.go` with missing metadata fields.
- [x] Rebuilt and restarted the `cdc-worker` Docker container.

### 3. Verification Steps (Remaining)
- [ ] User to restart `cdc-cms-service`.
- [ ] Trigger manual snapshot for `export-jobs`.
- [ ] Verify data flow from MongoDB -> Shadow -> Master.
