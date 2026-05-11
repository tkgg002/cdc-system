import { useMutation, useQuery } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

export type ReconStatus =
  | 'ok'
  | 'ok_empty'
  | 'warning'
  | 'drift'
  | 'dest_missing'
  | 'source_missing_or_stale'
  | 'error';

export interface ReconRow {
  target_table: string;
  source_object_id?: number | null;
  source_table?: string | null;
  shadow_schema?: string | null;
  shadow_table?: string | null;
  scope_ambiguous?: boolean;
  sync_engine?: string | null;
  source_type?: string | null;
  source_count: number | null;
  dest_count: number;
  drift_pct: number | null;
  status: ReconStatus;
  error_code?: string | null;
  full_source_count?: number | null;
  full_dest_count?: number | null;
  full_count_at?: string | null;
  timestamp_field?: string | null;
  timestamp_field_source?: 'auto' | 'admin_override' | null;
  timestamp_field_confidence?: 'high' | 'medium' | 'low' | null;
  checked_at: string;
  registry_id?: number | null;
}

export interface ReconReport extends ReconRow {
  id: number;
  source_db: string;
  diff: number;
  missing_count: number;
  stale_count: number;
  check_type: string;
  tier: number;
  duration_ms: number;
  healed_count: number;
  source_query_method?: string;
}

export interface FailedLog {
  id: number;
  target_table: string;
  source_db?: string | null;
  record_id: string;
  operation: string;
  error_message: string;
  error_type: string;
  retry_count: number;
  status: string;
  created_at: string;
  resolved_source_table?: string | null;
  shadow_schema?: string | null;
  shadow_table?: string | null;
  scope_ambiguous?: boolean;
}

export interface FailedLogsResponse {
  data: FailedLog[];
  total: number;
}

// ---------- Helpers ----------

function newIdempotencyKey(): string {
  return typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function auditHeaders(reason: string) {
  return {
    'Idempotency-Key': newIdempotencyKey(),
    'X-Action-Reason': reason,
  };
}

// ---------- Queries ----------
export function useReconReport() {
  return useQuery<ReconReport[]>({
    queryKey: ['recon-report'],
    queryFn: async () => {
      const { data } = await cmsApi.get<{ data: ReconReport[] }>('/api/reconciliation/report');
      return data.data || [];
    },
    refetchInterval: 30_000,
    staleTime: 25_000,
    retry: 2,
  });
}

export function useFailedLogs(pageSize = 50) {
  return useQuery<FailedLogsResponse>({
    queryKey: ['failed-logs', pageSize],
    queryFn: async () => {
      const { data } = await cmsApi.get<FailedLogsResponse>('/api/failed-sync-logs', {
        params: { page_size: pageSize },
      });
      return { data: data.data || [], total: data.total || 0 };
    },
    refetchInterval: 30_000,
    staleTime: 25_000,
    retry: 2,
  });
}

// ---------- Mutations ----------

export function useCheckAllMutation() {
  return useMutation<void, Error, { reason: string }>({
    mutationFn: async ({ reason }) => {
      await cmsApi.post('/api/reconciliation/check', { reason }, { headers: auditHeaders(reason) });
    },
    retry: 0,
  });
}

export function useCheckTableMutation() {
  return useMutation<void, Error, { table: string; tier: string; reason: string; sourceDatabase?: string; sourceTable?: string; shadowSchema?: string; shadowTable?: string }>({
    mutationFn: async ({ table, tier, reason, sourceDatabase, sourceTable, shadowSchema, shadowTable }) => {
      await cmsApi.post(
        `/api/reconciliation/check?tier=${encodeURIComponent(tier)}`,
        {
          reason,
          table,
          source_database: sourceDatabase || undefined,
          source_table: sourceTable || undefined,
          shadow_schema: shadowSchema || undefined,
          shadow_table: shadowTable || undefined,
        },
        { headers: auditHeaders(reason) },
      );
    },
    retry: 0,
  });
}

export function useHealMutation() {
  return useMutation<void, Error, { table: string; reason: string; sourceDatabase?: string; sourceTable?: string; shadowSchema?: string; shadowTable?: string }>({
    mutationFn: async ({ table, reason, sourceDatabase, sourceTable, shadowSchema, shadowTable }) => {
      await cmsApi.post(
        '/api/reconciliation/heal',
        {
          reason,
          table,
          source_database: sourceDatabase || undefined,
          source_table: sourceTable || undefined,
          shadow_schema: shadowSchema || undefined,
          shadow_table: shadowTable || undefined,
        },
        { headers: auditHeaders(reason) },
      );
    },
    retry: 0,
  });
}

export function useRetryFailedMutation() {
  return useMutation<void, Error, { id: number; reason: string }>({
    mutationFn: async ({ id, reason }) => {
      await cmsApi.post(
        `/api/failed-sync-logs/${id}/retry`,
        { reason },
        { headers: auditHeaders(reason) },
      );
    },
    retry: 0,
  });
}

export interface BackfillStatusRow {
  id: string;
  table_name: string;
  tier: number;
  status: 'running' | 'success' | 'failed' | 'cancelled';
  started_at: string;
  finished_at: string | null;
  docs_scanned: number;
  heal_actions: number;
  error_message: string | null;
  instance_id: string | null;
  total_rows: number;
  null_remaining: number;
  percent_done: number;
}

interface BackfillStatusResponse {
  data: BackfillStatusRow[];
  total: number;
}

export interface BackfillTriggerResponse {
  message: string;
  run_id: string;
  table: string;
  status_url: string;
}

export function useBackfillSourceTsMutation() {
  return useMutation<BackfillTriggerResponse, Error, { table?: string; reason: string }>({
    mutationFn: async ({ table, reason }) => {
      const { data } = await cmsApi.post<BackfillTriggerResponse>(
        '/api/recon/backfill-source-ts',
        { table: table || '' },
        { headers: auditHeaders(reason) },
      );
      return data;
    },
    retry: 0,
  });
}

export function useBackfillStatus(enabled = true, tableFilter?: string) {
  return useQuery<BackfillStatusResponse>({
    queryKey: ['backfill-status', tableFilter ?? 'all'],
    queryFn: async () => {
      const params: Record<string, string> = {};
      if (tableFilter) params.table = tableFilter;
      const { data } = await cmsApi.get<BackfillStatusResponse>(
        '/api/recon/backfill-source-ts/status',
        { params },
      );
      return { data: data.data || [], total: data.total || 0 };
    },
    enabled,
    refetchInterval: 5_000,
    staleTime: 4_000,
    retry: 1,
  });
}
