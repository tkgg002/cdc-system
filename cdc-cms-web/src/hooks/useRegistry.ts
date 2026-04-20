/**
 * Specialized async-dispatch hooks for registry & tooling operations.
 *
 * All hooks wrap `useAsyncDispatch` (202-accepted + poll pattern) and expose
 * the same interface: `{ state, dispatch, dispatchAsync, isPending, reset }`.
 *
 * Endpoint ↔ hook table:
 *   useScanFields(id)          POST /api/registry/:id/scan-fields
 *   useSyncAirbyte(id)         POST /api/registry/:id/sync
 *   useRefreshCatalog(id)      POST /api/registry/:id/refresh-catalog
 *   useScanSource()            POST /api/registry/scan-source
 *   useRestartDebezium()       POST /api/tools/restart-debezium
 *   useBulkSyncFromAirbyte()   POST /api/registry/sync-from-airbyte
 */
import { useAsyncDispatch } from './useAsyncDispatch';

export function useScanFields(registryId: number, targetTable?: string) {
  return useAsyncDispatch({
    endpoint: `/api/registry/${registryId}/scan-fields`,
    operation: 'scan-fields',
    targetTable,
  });
}

export function useSyncAirbyte(registryId: number, targetTable?: string) {
  return useAsyncDispatch({
    endpoint: `/api/registry/${registryId}/sync`,
    operation: 'airbyte-sync',
    targetTable,
  });
}

export function useRefreshCatalog(registryId: number, targetTable?: string) {
  return useAsyncDispatch({
    endpoint: `/api/registry/${registryId}/refresh-catalog`,
    operation: 'refresh-catalog',
    targetTable,
  });
}

export function useScanSource() {
  return useAsyncDispatch({
    endpoint: '/api/registry/scan-source',
    operation: 'scan-source',
  });
}

export function useRestartDebezium() {
  return useAsyncDispatch({
    endpoint: '/api/tools/restart-debezium',
    operation: 'restart-debezium',
  });
}

export function useBulkSyncFromAirbyte() {
  return useAsyncDispatch({
    endpoint: '/api/registry/sync-from-airbyte',
    operation: 'bulk-sync-from-airbyte',
  });
}
