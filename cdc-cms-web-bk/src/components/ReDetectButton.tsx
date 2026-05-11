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
