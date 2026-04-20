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

/**
 * ReconRow — strongly-typed row shape for the Data Integrity table.
 *
 * Source of truth: ADR v4 §2.2 (drift formula), §2.3 (error codes),
 * §2.7 (full counts), §2.8 (FE rendering).
 *
 * Backend populates these via `ReconciliationHandler.LatestReport` (joins
 * `cdc_reconciliation_report` with `cdc_table_registry`). Many fields are
 * optional so the FE renders safely while backend rollout is in flight.
 */
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
  sync_engine?: string | null;
  source_type?: string | null;
  /** NULL when source query failed — distinguishes from a real 0 count. */
  source_count: number | null;
  dest_count: number;
  /** 0–100, unsigned (|src-dst| / max(src,dst) * 100). Null until backend
   *  ships drift_pct in the LatestReport response. */
  drift_pct: number | null;
  status: ReconStatus;
  error_code?: string | null;
  /** Full unfiltered Mongo count, refreshed daily (ADR §2.7). */
  full_source_count?: number | null;
  /** Full unfiltered Postgres count, refreshed daily. */
  full_dest_count?: number | null;
  full_count_at?: string | null;
  timestamp_field?: string | null;
  timestamp_field_source?: 'auto' | 'admin_override' | null;
  timestamp_field_confidence?: 'high' | 'medium' | 'low' | null;
  checked_at: string;
  /** Optional — backend may omit for rows where the registry row was deleted. */
  registry_id?: number | null;
}

/**
 * Legacy ReconReport — kept as a superset of ReconRow so existing mutation
 * callers (heal, check-all, retry, backfill) don't break. Once all call
 * sites move to ReconRow this alias can be slimmed down.
 */
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
