/**
 * Phase multi_engine_unified — L3 T3.2.
 *
 * Toggle Auto/Manual provisioning_mode for a single source object row.
 *
 * Endpoint:
 *   POST /api/v1/cms/sources/:id/provisioning/mode
 *   body: { mode: 'auto' | 'manual' }
 *   headers: Idempotency-Key, X-Action-Reason
 *
 * Behaviour:
 *   - 200 → invalidate ['source-objects'] so TableRegistry refetches.
 *   - 409 (CAS conflict) / 422 (invalid transition) → propagate so
 *     the caller renders an Ant message + refresh prompt. Do NOT retry
 *     automatically: a CAS conflict means another operator (or
 *     orchestrator) already advanced the row, so a blind replay would
 *     either no-op or fight the live state.
 */
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
      // Audit middleware reads `reason` from JSON body (>= 10 chars). The
      // X-Action-Reason header is informational and not consulted by the
      // gate — keep both so log scrapers can pick whichever they prefer.
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
