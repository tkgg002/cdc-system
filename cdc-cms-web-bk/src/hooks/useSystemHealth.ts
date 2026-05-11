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
  [key: string]: unknown;
}

export interface RestartConnectorInput {
  reason: string;
  connectorName?: string;
}

export interface RestartConnectorResult {
  success: boolean;
  message?: string;
  connector?: string;
  [key: string]: unknown;
}

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
    retry: 0,
  });
}
