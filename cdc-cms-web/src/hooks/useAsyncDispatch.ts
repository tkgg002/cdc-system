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
  endpoint: string;
  statusEndpoint?: string;
  operation: string;
  targetTable?: string;
  statusParams?: Record<string, string | number | boolean | null | undefined>;
  pollInterval?: number;
  maxPollDuration?: number;
  invalidateKeys?: string[][];
}

// ---------- Helpers ----------

// Module-level constant so the default array keeps a stable identity across
// renders. Inline default values would be re-allocated each call and become
// an unstable useEffect dependency below, causing an infinite render loop
// once the dispatch reaches a terminal state.
const DEFAULT_INVALIDATE_KEYS: readonly string[][] = [['registry'], ['mapping-rules']];

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
    statusParams,
    pollInterval = 3_000,
    maxPollDuration = 5 * 60_000,
    invalidateKeys = DEFAULT_INVALIDATE_KEYS,
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
      const { data } = await cmsApi.post<{ message?: string;[k: string]: unknown }>(
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
    queryKey: ['dispatch-status', endpoint, operation, sinceTs, targetTable ?? '', JSON.stringify(statusParams ?? {})],
    queryFn: async () => {
      if (!sinceTs) return null;
      const url = statusEndpoint ?? `${endpoint}/dispatch-status`;
      const params = new URLSearchParams({ subject: operation, since: sinceTs });
      if (targetTable) params.set('target_table', targetTable);
      if (statusParams) {
        for (const [key, value] of Object.entries(statusParams)) {
          if (value !== undefined && value !== null && value !== '') {
            params.set(key, String(value));
          }
        }
      }
      const { data } = await cmsApi.get<DispatchStatusResponse>(`${url}?${params.toString()}`);
      return data ?? { entries: [] };
    },
    enabled: isPolling && sinceTs !== null,
    refetchInterval: isPolling ? pollInterval : false,
    staleTime: 0,
    retry: 1,
  });

  // Drive state machine off polled entries. Bail out idempotently when the
  // dispatch has already reached the same terminal state so a re-render of
  // this effect (eg. an unstable dep upstream) cannot recurse infinitely.
  useEffect(() => {
    const entries = statusQuery.data?.entries ?? [];
    if (!entries.length) return;
    const latest = entries[0];
    if (latest.status === 'success') {
      setState((s) =>
        s.status === 'success' ? s : { ...s, status: 'success', details: latest.details },
      );
    } else if (latest.status === 'error') {
      setState((s) =>
        s.status === 'error'
          ? s
          : { ...s, status: 'error', error: latest.error_message ?? 'Remote handler reported error' },
      );
    } else if (latest.status === 'running') {
      setState((s) => (s.status === 'running' ? s : { ...s, status: 'running' }));
    }
  }, [statusQuery.data]);

  // Run side effects once per terminal-state transition.
  useEffect(() => {
    if (state.status === 'success' || state.status === 'error' || state.status === 'timeout') {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    }
    if (state.status === 'success') {
      for (const key of invalidateKeys) {
        queryClient.invalidateQueries({ queryKey: key });
      }
    }
  }, [state.status, queryClient, invalidateKeys]);

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
