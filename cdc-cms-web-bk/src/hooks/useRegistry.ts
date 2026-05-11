import { useAsyncDispatch } from './useAsyncDispatch';

export function useScanFields(sourceObjectId?: number | null, registryId?: number | null, targetTable?: string) {
  const canUseV2 = sourceObjectId != null && sourceObjectId > 0;
  const endpoint = canUseV2
    ? `/api/v1/source-objects/${sourceObjectId}/scan-fields`
    : `/api/v1/source-objects/registry/${registryId ?? 0}/scan-fields`;
  const statusEndpoint = canUseV2
    ? `/api/v1/source-objects/${sourceObjectId}/dispatch-status`
    : `/api/v1/source-objects/registry/${registryId ?? 0}/dispatch-status`;
  return useAsyncDispatch({
    endpoint,
    statusEndpoint,
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
