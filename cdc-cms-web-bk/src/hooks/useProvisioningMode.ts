import { useMutation, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';
import type { ProvisioningMode } from '../types';

interface SetModeArgs {
  id: number;
  mode: ProvisioningMode;
  reason: string;
}

interface SetModeResponse {
  ok: boolean;
  action: string;
  source_id: number;
  mode: ProvisioningMode;
  actor: string;
}

function newIdempotencyKey(): string {
  return typeof crypto !== 'undefined' && 'randomUUID' in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

export function useProvisioningMode() {
  const qc = useQueryClient();
  return useMutation<SetModeResponse, Error, SetModeArgs>({
    mutationFn: async ({ id, mode, reason }) => {
      const { data } = await cmsApi.post<SetModeResponse>(
        `/api/v1/cms/sources/${id}/provisioning/mode`,
        { mode, reason },
        {
          headers: {
            'Idempotency-Key': newIdempotencyKey(),
            'X-Action-Reason': reason,
          },
        },
      );
      return data;
    },
    retry: 0,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['source-objects'] });
      qc.invalidateQueries({ queryKey: ['sources'] });
    },
  });
}
