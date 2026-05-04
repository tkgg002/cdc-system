/**
 * Re-detect timestamp field button — kicks off the Worker auto-detect pass
 * for a single registry entry.
 *
 * Backend contract (ADR §2.1):
 *   Preferred:
 *     POST /api/v1/source-objects/:id/detect-timestamp-field         → 202 Accepted
 *     GET  /api/v1/source-objects/:id/dispatch-status?subject=detect-timestamp-field
 *   Fallback bridge:
 *     POST /api/v1/source-objects/registry/:id/detect-timestamp-field
 *     GET  /api/v1/source-objects/registry/:id/dispatch-status?subject=detect-timestamp-field
 *
 * The worker samples the Mongo collection, ranks `timestamp_field_candidates`
 * by coverage, and updates `cdc_table_registry.timestamp_field` +
 * `timestamp_field_confidence` + `timestamp_field_detected_at`.
 *
 * UX rules:
 *   - Disabled + greyed while pending; spinner via AntD `loading`.
 *   - Tooltip reason (§2.8) is keyboard-focusable — AntD Tooltip wraps the
 *     Button (a native <button>) so tab-focus triggers the tooltip text as
 *     `aria-describedby`.
 *   - `reason` string fulfills the governance audit header `X-Action-Reason`
 *     (see useAsyncDispatch) so admin actions stay traceable.
 */
import { Button, Tooltip } from 'antd';
import { ExperimentOutlined } from '@ant-design/icons';
import { useAsyncDispatch } from '../hooks/useAsyncDispatch';

export interface ReDetectButtonProps {
  targetTable: string;
  sourceObjectId?: number | null;
  registryId?: number | null;
}

export function ReDetectButton({ targetTable, sourceObjectId, registryId }: ReDetectButtonProps) {
  const canUseV2 = sourceObjectId != null && sourceObjectId > 0;
  const canUseBridge = registryId != null && registryId > 0;
  if (!canUseV2 && !canUseBridge) return null;

  const endpoint = canUseV2
    ? `/api/v1/source-objects/${sourceObjectId}/detect-timestamp-field`
    : `/api/v1/source-objects/registry/${registryId}/detect-timestamp-field`;
  const statusEndpoint = canUseV2
    ? `/api/v1/source-objects/${sourceObjectId}/dispatch-status`
    : `/api/v1/source-objects/registry/${registryId}/dispatch-status`;

  const dispatch = useAsyncDispatch({
    endpoint,
    statusEndpoint,
    operation: 'detect-timestamp-field',
    targetTable,
    invalidateKeys: [['registry'], ['recon-report']],
  });

  const onClick = () => {
    // fire-and-forget — hook surfaces `state` for any caller that wants to
    // render progress. Catch here to stop the unhandled-rejection noise.
    dispatch
      .dispatchAsync({ reason: `Re-detect timestamp field cho ${targetTable}` })
      .catch(() => {
        /* Error already captured into `state` by the hook. */
      });
  };

  return (
    <Tooltip title="Quét lại Mongo để phát hiện timestamp field đúng">
      <Button
        size="small"
        icon={<ExperimentOutlined />}
        loading={dispatch.isPending}
        onClick={onClick}
      >
        Re-detect
      </Button>
    </Tooltip>
  );
}

export default ReDetectButton;
