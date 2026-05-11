import { useEffect, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

export type JobStatus =
  | 'idle'
  | 'dispatching'
  | 'pending'
  | 'running'
  | 'success'
  | 'failed'
  | 'timeout';

export interface JobState {
  status: JobStatus;
  jobId?: string;
  message?: string;
  error?: string;
  details?: unknown;
}

export interface JobResponse {
  id: string;
  type: string;
  status: 'pending' | 'running' | 'success' | 'failed';
  payload?: string;
  result?: string;
  error_message?: string;
  created_at: string;
  updated_at: string;
}

export interface UseAsyncJobOptions {
  endpoint: string;
  invalidateKeys?: string[][];
  pollInterval?: number;
  maxPollDuration?: number;
}

export interface DispatchMutationInput {
  endpoint?: string;
  payload?: Record<string, unknown>;
  headers?: Record<string, string>;
}

function newIdempotencyKey(): string {
  return typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

export function useAsyncJob(opts: UseAsyncJobOptions) {
  const {
    endpoint,
    pollInterval = 2_000,
    maxPollDuration = 3 * 60_000,
    invalidateKeys = [['master-registry']],
  } = opts;

  const [state, setState] = useState<JobState>({ status: 'idle' });
  const [jobId, setJobId] = useState<string | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const queryClient = useQueryClient();

  const dispatch = useMutation<unknown, Error, DispatchMutationInput | void>({
    mutationFn: async (input) => {
      setState({ status: 'dispatching' });
      const idempotencyKey = newIdempotencyKey();
      
      const payload = input?.payload ?? {};
      const extraHeaders = input?.headers ?? {};

      const targetEndpoint = input?.endpoint ?? endpoint;
      const { data } = await cmsApi.post<{ status?: string; job_id?: string; [k: string]: unknown }>(
        targetEndpoint,
        payload,
        {
          headers: {
            'Idempotency-Key': idempotencyKey,
            ...extraHeaders,
          },
        },
      );
      
      if (data?.job_id) {
        setJobId(data.job_id);
        setState({
          status: 'pending',
          jobId: data.job_id,
          message: data?.status,
        });
      } else {
        // Fallback if no job_id is returned (synchronous completion or legacy endpoint)
        setState({
          status: 'success',
          details: data,
        });
        return data;
      }

      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      timeoutRef.current = setTimeout(() => {
        setState((s) =>
          s.status === 'success' || s.status === 'failed'
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
        status: 'failed',
        error: maybeAxios.response?.data?.error ?? maybeAxios.message ?? 'Dispatch failed',
      });
    },
    retry: 0,
  });

  const isPolling =
    state.status === 'pending' || state.status === 'running';

  const statusQuery = useQuery<JobResponse | null>({
    queryKey: ['job-status', jobId],
    queryFn: async () => {
      if (!jobId) return null;
      const { data } = await cmsApi.get<JobResponse>(`/api/v1/jobs/${jobId}`);
      return data;
    },
    enabled: isPolling && jobId !== null,
    refetchInterval: isPolling ? pollInterval : false,
    staleTime: 0,
    retry: 1,
  });

  useEffect(() => {
    const job = statusQuery.data;
    if (!job) return;

    if (job.status === 'success') {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      
      let details: unknown;
      try {
        if (job.result) details = JSON.parse(job.result);
      } catch {
        details = job.result;
      }

      setState((s) => ({ ...s, status: 'success', details }));
      
      for (const key of invalidateKeys) {
        queryClient.invalidateQueries({ queryKey: key });
      }
    } else if (job.status === 'failed') {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
      setState((s) => ({
        ...s,
        status: 'failed',
        error: job.error_message ?? 'Job failed',
      }));
    } else if (job.status === 'running' || job.status === 'pending') {
      setState((s) => (s.status === job.status ? s : { ...s, status: job.status }));
    }
  }, [statusQuery.data, queryClient, invalidateKeys]);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  const reset = () => {
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setState({ status: 'idle' });
    setJobId(null);
  };

  return {
    state,
    dispatch: dispatch.mutate,
    dispatchAsync: dispatch.mutateAsync,
    isPending:
      dispatch.isPending ||
      state.status === 'dispatching' ||
      state.status === 'pending' ||
      state.status === 'running',
    reset,
  };
}
