/**
 * Visual indicator for async-dispatch operations.
 *
 * Surfaces the `DispatchState` produced by `useAsyncDispatch` / specialized
 * hooks (`useScanFields`, `useRestartDebezium`, ...) as an Ant Design tag with
 * optional spinner + tooltip showing dispatch time.
 *
 * Accessibility: Tooltip is keyboard-focusable via tabIndex; Spin uses the
 * Ant Design default ARIA-live "polite" region for progress announcements.
 */
import { Spin, Tag, Tooltip } from 'antd';
import type { DispatchState } from '../hooks/useAsyncDispatch';

const COLOR_BY_STATUS: Record<DispatchState['status'], string> = {
  idle: 'default',
  dispatching: 'processing',
  accepted: 'processing',
  running: 'processing',
  success: 'success',
  error: 'error',
  timeout: 'warning',
};

function labelFor(state: DispatchState): string {
  switch (state.status) {
    case 'idle':
      return 'Sẵn sàng';
    case 'dispatching':
      return 'Đang gửi...';
    case 'accepted':
      return 'Đã nhận — đang xử lý';
    case 'running':
      return 'Đang chạy';
    case 'success':
      return 'Hoàn tất';
    case 'timeout':
      return 'Quá thời gian chờ';
    case 'error':
      return `Lỗi: ${state.error ?? 'unknown'}`;
  }
}

export interface DispatchStatusBadgeProps {
  state: DispatchState;
}

export default function DispatchStatusBadge({ state }: DispatchStatusBadgeProps) {
  const showSpinner =
    state.status === 'dispatching' ||
    state.status === 'accepted' ||
    state.status === 'running';

  const tooltip = state.dispatchedAt
    ? `Dispatched at ${new Date(state.dispatchedAt).toLocaleString()}`
    : '';

  return (
    <Tooltip title={tooltip}>
      <span tabIndex={0} aria-live="polite">
        <Tag
          color={COLOR_BY_STATUS[state.status]}
          icon={showSpinner ? <Spin size="small" /> : undefined}
        >
          {labelFor(state)}
        </Tag>
      </span>
    </Tooltip>
  );
}

export { DispatchStatusBadge };
