/**
 * QueryErrorBoundary
 *
 * Render fallback UI when React Query retries have exhausted or when a child
 * component throws during render. Pairs well with the app shell so a single
 * failing page section does not crash the whole layout.
 *
 * Usage:
 *   <QueryErrorBoundary>
 *     <SystemHealth />
 *   </QueryErrorBoundary>
 */
import { Component, type ErrorInfo, type ReactNode } from 'react';
import { Alert, Button, Space } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { QueryErrorResetBoundary } from '@tanstack/react-query';

interface FallbackProps {
  error: Error | null;
  onReset: () => void;
}

function DefaultFallback({ error, onReset }: FallbackProps) {
  return (
    <div style={{ padding: 24 }}>
      <Alert
        type="error"
        showIcon
        message="Không tải được dữ liệu"
        description={
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            <div>
              {error?.message || 'Đã xảy ra lỗi không xác định khi gọi API.'}
            </div>
            <Button type="primary" icon={<ReloadOutlined />} onClick={onReset}>
              Thử lại
            </Button>
          </Space>
        }
      />
    </div>
  );
}

interface BoundaryState {
  error: Error | null;
}

interface BoundaryProps {
  children: ReactNode;
  onReset: () => void;
  fallback?: (props: FallbackProps) => ReactNode;
}

class InnerBoundary extends Component<BoundaryProps, BoundaryState> {
  state: BoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): BoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Surface error to console so developers can diagnose; production
    // observability pipeline can hook via window.onerror.
    // eslint-disable-next-line no-console
    console.error('[QueryErrorBoundary] caught error:', error, info);
  }

  handleReset = () => {
    this.setState({ error: null });
    this.props.onReset();
  };

  render() {
    if (this.state.error) {
      const fallback = this.props.fallback ?? DefaultFallback;
      return fallback({ error: this.state.error, onReset: this.handleReset });
    }
    return this.props.children;
  }
}

export interface QueryErrorBoundaryProps {
  children: ReactNode;
  fallback?: (props: FallbackProps) => ReactNode;
}

export default function QueryErrorBoundary({ children, fallback }: QueryErrorBoundaryProps) {
  return (
    <QueryErrorResetBoundary>
      {({ reset }) => (
        <InnerBoundary onReset={reset} fallback={fallback}>
          {children}
        </InnerBoundary>
      )}
    </QueryErrorResetBoundary>
  );
}
