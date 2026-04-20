/**
 * Generic hook for async-dispatch endpoints (202 Accepted + polling).
 *
 * Backend contract (post refactor v3 — see
 * `agent/memory/workspaces/feature-cdc-integration/10_gap_analysis_scan_fields_boundary_violation.md`):
 *   POST  {endpoint}                       → 202 Accepted  { message, ... }
 *   GET   {endpoint}/dispatch-status?subject=&since=&target_table=
 *                                          → { entries: [{ status, error_message, details, ... }] }
 *
 * State machine:
 *   idle → dispatching → accepted → running → success | error
 *                             └────────────────→ success | error (direct)
 *
 * Polling strategy:
 *   - 3s interval (default), tunable via `pollInterval`.
 *   - Stops automatically on terminal status (success/error).
 *   - 5 min max (default), tunable via `maxPollDuration`.
 *   - Uses latest entry since dispatch timestamp to avoid mixing with older
 *     runs of the same operation (critical for repeated scan/sync actions).
 *
 * Audit: every dispatch sends `Idempotency-Key` + `X-Action-Reason` headers
 * matching the governance pattern from `useReconStatus`, `useSystemHealth`.
 */
import { useEffect, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

// ---------- Types ----------

export type DispatchStatus =
  | 'idle'
  | 'dispatching'
  | 'accepted'
  | 'running'
  | 'success'
  | 'error'
  | 'timeout';

export interface DispatchState {
  status: DispatchStatus;
  dispatchedAt?: string;
  message?: string;
  error?: string;
  // Shape varies per operation (scan-fields returns { added, total }, sync returns
  // { job_id }, etc.). Consumers narrow at call site.
  details?: unknown;
}

export interface DispatchStatusEntry {
  status: 'running' | 'success' | 'error';
  error_message?: string;
  details?: unknown;
  timestamp?: string;
}

export interface DispatchStatusResponse {
  entries?: DispatchStatusEntry[];
}

export interface DispatchMutationInput {
  reason: string;
  payload?: Record<string, unknown>;
}

export interface UseAsyncDispatchOptions {
  /** Dispatch endpoint, e.g. '/api/registry/37/scan-fields'. */
  endpoint: string;
  /** Optional override; defaults to `${endpoint}/dispatch-status`. */
  statusEndpoint?: string;
  /** Activity-log subject used to filter status entries. */
  operation: string;
  /** Optional target_table filter for multi-tenant endpoints. */
  targetTable?: string;
  /** Poll interval in ms (default 3_000). */
  pollInterval?: number;
  /** Max poll duration in ms (default 5 min). */
  maxPollDuration?: number;
  /** Query keys to invalidate on success. Default: ['registry', 'mapping-rules']. */
  invalidateKeys?: string[][];
}

// ---------- Helpers ----------

function newIdempotencyKey(): string {
  return typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

// ---------- Hook ----------

export function useAsyncDispatch(opts: UseAsyncDispatchOptions) {
  const {
    endpoint,
    statusEndpoint,
    operation,
    targetTable,
    pollInterval = 3_000,
    maxPollDuration = 5 * 60_000,
    invalidateKeys = [['registry'], ['mapping-rules']],
  } = opts;

  const [state, setState] = useState<DispatchState>({ status: 'idle' });
  const [sinceTs, setSinceTs] = useState<string | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const queryClient = useQueryClient();

  const dispatch = useMutation<unknown, Error, DispatchMutationInput>({
    mutationFn: async ({ reason, payload }) => {
      setState({ status: 'dispatching' });
      const idempotencyKey = newIdempotencyKey();
      const body = payload ?? { reason };
      const { data } = await cmsApi.post<{ message?: string; [k: string]: unknown }>(
        endpoint,
        body,
        {
          headers: {
            'Idempotency-Key': idempotencyKey,
            'X-Action-Reason': reason,
          },
        },
      );
      const now = new Date().toISOString();
      setSinceTs(now);
      setState({
        status: 'accepted',
        dispatchedAt: now,
        message: data?.message,
        details: data,
      });

      // Start timeout guard — if we never reach a terminal state, flip to timeout.
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      timeoutRef.current = setTimeout(() => {
        setState((s) =>
          s.status === 'success' || s.status === 'error'
            ? s
            : {
                ...s,
                status: 'timeout',
                error: `No terminal status after ${Math.round(maxPollDuration / 1000)}s`,
              },
        );
      }, maxPollDuration);

      return data;
    },
    onError: (err) => {
      // Axios attaches `response` — narrow without leaking `any` everywhere.
      const maybeAxios = err as { response?: { data?: { error?: string } }; message?: string };
      setState({
        status: 'error',
        error: maybeAxios.response?.data?.error ?? maybeAxios.message ?? 'Dispatch failed',
      });
    },
    retry: 0,
  });

  const isPolling =
    state.status === 'accepted' || state.status === 'running';

  const statusQuery = useQuery<DispatchStatusResponse | null>({
    queryKey: ['dispatch-status', endpoint, operation, sinceTs, targetTable ?? ''],
    queryFn: async () => {
      if (!sinceTs) return null;
      const url = statusEndpoint ?? `${endpoint}/dispatch-status`;
      const params = new URLSearchParams({ subject: operation, since: sinceTs });
      if (targetTable) params.set('target_table', targetTable);
      const { data } = await cmsApi.get<DispatchStatusResponse>(`${url}?${params.toString()}`);
      return data ?? { entries: [] };
    },
    enabled: isPolling && sinceTs !== null,
    refetchInterval: isPolling ? pollInterval : false,
    staleTime: 0,
    retry: 1,
  });

  // Drive state machine off polled entries.
  useEffect(() => {
    const entries = statusQuery.data?.entries ?? [];
    if (!entries.length) return;
    const latest = entries[0];
    if (latest.status === 'success') {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      setState((s) => ({ ...s, status: 'success', details: latest.details }));
      for (const key of invalidateKeys) {
        queryClient.invalidateQueries({ queryKey: key });
      }
    } else if (latest.status === 'error') {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      setState((s) => ({
        ...s,
        status: 'error',
        error: latest.error_message ?? 'Remote handler reported error',
      }));
    } else if (latest.status === 'running') {
      setState((s) => (s.status === 'running' ? s : { ...s, status: 'running' }));
    }
  }, [statusQuery.data, queryClient, invalidateKeys]);

  // Clean up timer on unmount.
  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  const reset = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setState({ status: 'idle' });
    setSinceTs(null);
  };

  return {
    state,
    dispatch: dispatch.mutate,
    dispatchAsync: dispatch.mutateAsync,
    isPending:
      dispatch.isPending ||
      state.status === 'dispatching' ||
      state.status === 'accepted' ||
      state.status === 'running',
    reset,
  };
}
