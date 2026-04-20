/**
 * React Query hooks for Data Integrity / Reconciliation endpoints.
 *
 * Endpoints used by DataIntegrity page:
 *   GET  /api/reconciliation/report                      → recon report list
 *   GET  /api/failed-sync-logs?page_size=...             → failed sync logs
 *   POST /api/reconciliation/check                       → trigger check all
 *   POST /api/reconciliation/check/:table?tier=N         → trigger per-table check
 *   POST /api/reconciliation/heal/:table                 → heal drift
 *   POST /api/failed-sync-logs/:id/retry                 → retry failed log
 *
 * Notes:
 *   - `useReconReport` and `useFailedLogs` are stale-while-revalidate with 30s refetch.
 *   - All destructive mutations send `Idempotency-Key` (UUID) and `X-Action-Reason`
 *     so backend audit log captures operator intent (Governance §2 / Plan v3 §12).
 */
import { useMutation, useQuery } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

// ---------- Types ----------

export interface ReconReport {
  id: number;
  target_table: string;
  source_db: string;
  source_count: number;
  dest_count: number;
  diff: number;
  missing_count: number;
  stale_count: number;
  check_type: string;
  status: string;
  tier: number;
  duration_ms: number;
  checked_at: string;
  healed_count: number;
}

export interface FailedLog {
  id: number;
  target_table: string;
  record_id: string;
  operation: string;
  error_message: string;
  error_type: string;
  retry_count: number;
  status: string;
  created_at: string;
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

/**
 * Reconciliation report (per-table drift summary).
 * stale 25s / refetch 30s aligns with backend recon scheduler cadence.
 */
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

/**
 * Failed sync logs (last N with server-side pagination).
 */
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

/**
 * Trigger Tier-1 check across all active tables.
 */
export function useCheckAllMutation() {
  return useMutation<void, Error, { reason: string }>({
    mutationFn: async ({ reason }) => {
      await cmsApi.post('/api/reconciliation/check', { reason }, { headers: auditHeaders(reason) });
    },
    retry: 0,
  });
}

/**
 * Trigger check for a single table at chosen tier.
 */
export function useCheckTableMutation() {
  return useMutation<void, Error, { table: string; tier: string; reason: string }>({
    mutationFn: async ({ table, tier, reason }) => {
      await cmsApi.post(
        `/api/reconciliation/check/${encodeURIComponent(table)}?tier=${encodeURIComponent(tier)}`,
        { reason },
        { headers: auditHeaders(reason) },
      );
    },
    retry: 0,
  });
}

/**
 * Heal drift for a table (destructive — writes to dest DB).
 */
export function useHealMutation() {
  return useMutation<void, Error, { table: string; reason: string }>({
    mutationFn: async ({ table, reason }) => {
      await cmsApi.post(
        `/api/reconciliation/heal/${encodeURIComponent(table)}`,
        { reason },
        { headers: auditHeaders(reason) },
      );
    },
    retry: 0,
  });
}

/**
 * Retry a failed sync log entry (replays the upsert).
 */
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

// ---------- Backfill `_source_ts` (tier-4 job) ----------

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

/**
 * Trigger the `_source_ts` backfill job. `table` empty = all tables.
 */
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

/**
 * Poll tier-4 recon_runs rows. enabled=false pauses the query (used
 * when the user has not yet triggered a run).
 */
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
    refetchInterval: 5_000, // user wants near-real-time progress
    staleTime: 4_000,
    retry: 1,
  });
}
