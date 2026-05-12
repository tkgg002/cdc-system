# Report Stabilization 20260512

## Current Status
- CMS build: OK
- Worker build: OK
- Activity Log Parsing: Fixed in code (casting to text), awaiting service restart to verify.
- "Snapshot Now": Cause identified (Resolution failure for V2 objects + Signal DB mismatch).

## Solutions Implemented
### 1. Activity Log Parsing (Fixed & Verified Build)
- **Problem**: GORM scanning `jsonb` into `any` failed with `unsupported data type`.
- **Fix**: Cast `details::text` in `ActivityLogReadRepo` and changed Go model to `*string`.
- **Verification**: `go build` success in CMS.

### 2. Snapshot Resolution & Targeting (Fixed & Verified in Logs)
- **Problem**: Table name mismatch and wrong signal database.
- **Fix**: 
  - Worker now resolves V1 (`sd_` prefix) and V2 (`source_object_registry`) objects.
  - Signals are now inserted into `centralized-export-service.debezium_signal` as required by the connector config.
- **Verification**: Worker logs confirm: `debezium signal inserted` with `database: centralized-export-service`.

### 3. Log Cleanup (Fixed)
- **Problem**: Constant OTEL spam made it impossible to see real errors.
- **Fix**: Disabled OTEL in `config-local.yml` for both services.

## Verification Instructions
1. **Restart both services** (IMPORTANT: must restart to pick up the build and config changes).
2. **Observe Logs**: No more OTEL "no such host" errors.
3. **Check Activity Log**: Refresh UI, the "unsupported data type" error should be gone.
4. **Trigger Snapshot**: Click "Snapshot Now" again. Confirm in Worker logs: `debezium signal inserted`.
