/**
 * Specialized async-dispatch hooks for registry & tooling operations.
 *
 * Post-Sprint 4 only Debezium-native operations remain.
 */
import { useAsyncDispatch } from './useAsyncDispatch';

export function useScanFields(registryId: number, targetTable?: string) {
  return useAsyncDispatch({
    endpoint: `/api/registry/${registryId}/scan-fields`,
    operation: 'scan-fields',
    targetTable,
  });
}

export function useRestartDebezium() {
  return useAsyncDispatch({
    endpoint: '/api/tools/restart-debezium',
    operation: 'restart-debezium',
  });
}
