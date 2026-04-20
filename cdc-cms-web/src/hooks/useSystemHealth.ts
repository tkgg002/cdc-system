/**
 * React Query hooks for System Health endpoints.
 *
 * Endpoints:
 *   GET  /api/system/health           → snapshot (cached 25s, auto-refetch 30s)
 *   POST /api/tools/restart-debezium  → restart connector (with Idempotency-Key + reason)
 *
 * Usage example:
 *   const { data, isLoading, error, refetch } = useSystemHealth();
 *   const restart = useRestartConnector();
 *   await restart.mutateAsync({ reason: 'Kafka Connect stuck for 10m' });
 *
 * Note: Backend v3 refactor is in-flight. `SectionResult.data` is kept as `unknown`
 * to stay forward-compatible until backend section contracts freeze.
 */
import { useMutation, useQuery } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

// ---------- Types ----------

export type SectionStatus = 'ok' | 'degraded' | 'down' | 'unknown';

export interface SectionResult {
  status: SectionStatus;
  error?: string;
  data?: unknown;
}

export interface SystemHealthSnapshot {
  timestamp: string;
  cache_age_seconds: number;
  sections: {
    infrastructure?: SectionResult;
    pipeline?: SectionResult;
    reconciliation?: SectionResult;
    latency?: SectionResult;
    alerts?: SectionResult;
    recent_events?: SectionResult;
  };
  // Forward-compat: backend may return extra top-level fields during refactor
  [key: string]: unknown;
}

export interface RestartConnectorInput {
  reason: string;
  connectorName?: string; // default handled server-side
}

export interface RestartConnectorResult {
  success: boolean;
  message?: string;
  connector?: string;
  [key: string]: unknown;
}

// ---------- Hooks ----------

/**
 * Fetch system health snapshot.
 * - staleTime 25s aligns with backend cache TTL (avoid thundering herd).
 * - refetchInterval 30s gives fresh data shortly after cache expiry.
 */
export function useSystemHealth() {
  return useQuery<SystemHealthSnapshot>({
    queryKey: ['system-health'],
    queryFn: async () => {
      const { data } = await cmsApi.get<SystemHealthSnapshot>('/api/system/health');
      return data;
    },
    refetchInterval: 30_000,
    staleTime: 25_000,
    retry: 2,
  });
}

/**
 * Restart Debezium / Kafka-Connect connector with idempotency + reason audit.
 * Caller MUST provide a non-empty `reason` (UI enforces min 10 chars).
 */
export function useRestartConnector() {
  return useMutation<RestartConnectorResult, Error, RestartConnectorInput>({
    mutationFn: async ({ reason, connectorName }) => {
      const idempotencyKey =
        typeof crypto !== 'undefined' && 'randomUUID' in crypto
          ? crypto.randomUUID()
          : `${Date.now()}-${Math.random().toString(36).slice(2)}`;

      const { data } = await cmsApi.post<RestartConnectorResult>(
        '/api/tools/restart-debezium',
        { reason, connector: connectorName },
        {
          headers: {
            'Idempotency-Key': idempotencyKey,
            'X-Action-Reason': reason,
          },
        },
      );
      return data;
    },
    retry: 0, // destructive action — don't auto-retry
  });
}
