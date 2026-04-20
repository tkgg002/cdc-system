/**
 * Re-detect timestamp field button — kicks off the Worker auto-detect pass
 * for a single registry entry.
 *
 * Backend contract (ADR §2.1):
 *   POST /api/registry/:id/detect-timestamp-field  →  202 Accepted
 *   GET  /api/registry/:id/detect-timestamp-field/dispatch-status?subject=detect-timestamp-field
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
  registryId: number;
}

export function ReDetectButton({ targetTable, registryId }: ReDetectButtonProps) {
  const dispatch = useAsyncDispatch({
    endpoint: `/api/registry/${registryId}/detect-timestamp-field`,
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
