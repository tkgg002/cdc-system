/**
 * SystemHealth page (Phase 7 — React Query refactor).
 *
 * Responsibilities:
 *   - Consume `useSystemHealth()` (stale-while-revalidate 25s, auto-refetch 30s).
 *   - Render each section independently via `<HealthSection>` so one probe failure
 *     does not take the whole page down.
 *   - Surface `cache_age_seconds > 60` as a stale-data warning banner.
 *   - Destructive "Restart Connector" goes through `<ConfirmDestructiveModal>`
 *     and re-invalidates `['system-health']` on success.
 *
 * Compatibility: backend v3 migration is in-flight. When `data.sections.*` is
 * absent we fall back to legacy top-level fields (`infrastructure`, `cdc_pipeline`,
 * `reconciliation`, `latency`, `alerts`, `recent_events`, `failed_sync`). This
 * lets FE ship ahead of backend Plan v3 §7 rewrite without breaking current prod.
 */
import { useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Row,
  Skeleton,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  DashboardOutlined,
  ExclamationCircleOutlined,
  QuestionCircleOutlined,
  ReloadOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import {
  useRestartConnector,
  useSystemHealth,
  type SectionResult,
  type SectionStatus,
  type SystemHealthSnapshot,
} from '../hooks/useSystemHealth';
import ConfirmDestructiveModal from '../components/ConfirmDestructiveModal';

const { Title, Text } = Typography;

// ---------- Status helpers ----------

type UnifiedStatus = SectionStatus | 'up' | 'RUNNING' | 'running' | 'healthy' | 'critical' | 'FAILED';

function normalizeStatus(status: unknown): SectionStatus {
  const s = typeof status === 'string' ? status.toLowerCase() : '';
  if (s === 'ok' || s === 'up' || s === 'running' || s === 'healthy') return 'ok';
  if (s === 'degraded' || s === 'warning') return 'degraded';
  if (s === 'down' || s === 'failed' || s === 'critical') return 'down';
  return 'unknown';
}

function statusIcon(raw: UnifiedStatus | string | undefined) {
  const s = normalizeStatus(raw);
  if (s === 'ok') return <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 24 }} />;
  if (s === 'down') return <CloseCircleOutlined style={{ color: '#ff4d4f', fontSize: 24 }} />;
  if (s === 'degraded') return <WarningOutlined style={{ color: '#faad14', fontSize: 24 }} />;
  return <QuestionCircleOutlined style={{ color: '#bfbfbf', fontSize: 24 }} />;
}

function statusBg(raw: UnifiedStatus | string | undefined) {
  const s = normalizeStatus(raw);
  if (s === 'ok') return '#f6ffed';
  if (s === 'down') return '#fff2f0';
  if (s === 'degraded') return '#fffbe6';
  return '#fafafa';
}

function statusTag(raw: UnifiedStatus | string | undefined) {
  const s = normalizeStatus(raw);
  if (s === 'ok') return <Tag color="green">Healthy</Tag>;
  if (s === 'down') return <Tag color="red">Down</Tag>;
  if (s === 'degraded') return <Tag color="orange">Degraded</Tag>;
  return <Tag color="default">Unknown</Tag>;
}

// ---------- Types for unified sections ----------

interface UnifiedSections {
  infrastructure: SectionResult;
  pipeline: SectionResult;
  reconciliation: SectionResult;
  latency: SectionResult;
  alerts: SectionResult;
  recent_events: SectionResult;
}

function resolveSections(data: SystemHealthSnapshot): UnifiedSections {
  const s = (data.sections ?? {}) as Partial<UnifiedSections>;
  const legacy = data as Record<string, unknown>;

  const fallback = (key: string, legacyKey = key): SectionResult => {
    const raw = legacy[legacyKey];
    if (raw === undefined || raw === null) {
      return { status: 'unknown', error: `Section "${key}" not reported by backend.` };
    }
    return { status: 'ok', data: raw };
  };

  return {
    infrastructure: s.infrastructure ?? fallback('infrastructure'),
    pipeline: s.pipeline ?? fallback('pipeline', 'cdc_pipeline'),
    reconciliation: s.reconciliation ?? fallback('reconciliation'),
    latency: s.latency ?? fallback('latency'),
    alerts: s.alerts ?? fallback('alerts'),
    recent_events: s.recent_events ?? fallback('recent_events'),
  };
}

// ---------- Shared section wrapper ----------

interface HealthSectionProps {
  title: string;
  section: SectionResult;
  extra?: React.ReactNode;
  children: (data: unknown) => React.ReactNode;
}

function HealthSection({ title, section, extra, children }: HealthSectionProps) {
  const normalized = normalizeStatus(section.status);

  if (normalized === 'unknown') {
    return (
      <Card size="small" title={title} extra={extra} style={{ marginBottom: 16 }}>
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description={
            <Space direction="vertical" size={4}>
              <Text type="secondary">Không thể lấy dữ liệu.</Text>
              {section.error && (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {section.error}
                </Text>
              )}
            </Space>
          }
        />
      </Card>
    );
  }

  if (normalized === 'down') {
    return (
      <Card size="small" title={title} extra={extra} style={{ marginBottom: 16 }}>
        <Alert
          type="error"
          showIcon
          message={`${title} đang gặp sự cố`}
          description={section.error || 'Probe trả về trạng thái down.'}
        />
      </Card>
    );
  }

  return (
    <Card
      size="small"
      title={
        <Space>
          {title}
          {statusTag(section.status)}
        </Space>
      }
      extra={extra}
      style={{ marginBottom: 16 }}
    >
      {normalized === 'degraded' && (
        <Alert
          type="warning"
          showIcon
          message="Section đang ở trạng thái degraded"
          description={section.error || 'Một số probe trả về cảnh báo.'}
          style={{ marginBottom: 12 }}
        />
      )}
      {children(section.data)}
    </Card>
  );
}

// ---------- Section renderers ----------

interface ComponentCell {
  key: string;
  label: string;
  data: Record<string, unknown> | undefined;
}

function InfrastructureBody({ data, pipeline }: { data: unknown; pipeline: unknown }) {
  const infra = (data as Record<string, Record<string, unknown>>) || {};
  const pipelineObj = (pipeline as Record<string, unknown>) || {};
  const debezium = (pipelineObj.debezium as Record<string, unknown>) || {};
  const worker = (pipelineObj.worker as Record<string, unknown>) || {};

  const components: ComponentCell[] = [
    { key: 'kafka', label: 'Kafka', data: infra.kafka },
    { key: 'nats', label: 'NATS', data: infra.nats },
    { key: 'postgres', label: 'PostgreSQL', data: infra.postgres },
    { key: 'redis', label: 'Redis', data: infra.redis },
    { key: 'airbyte', label: 'Airbyte', data: infra.airbyte },
    { key: 'debezium', label: 'Debezium', data: debezium },
    { key: 'worker', label: 'Worker', data: worker },
  ];

  return (
    <Row gutter={[12, 12]}>
      {components.map((c) => {
        const status = (c.data?.status as string) || 'unknown';
        const detail =
          normalizeStatus(status) === 'ok'
            ? (c.data?.topics && `${c.data.topics} topics`) ||
              (c.data?.streams && `${c.data.streams} streams`) ||
              (c.data?.tables_registered && `${c.data.tables_registered} tables`) ||
              (c.data?.connector as string) ||
              'Connected'
            : ((c.data?.error as string) || status || 'Unknown');
        return (
          <Col key={c.key} xs={12} sm={8} md={6} lg={6} xl={4}>
            <Card size="small" style={{ background: statusBg(status), textAlign: 'center' }}>
              {statusIcon(status)}
              <div style={{ fontWeight: 600, marginTop: 4 }}>{c.label}</div>
              <div style={{ fontSize: 11, color: '#666' }}>{String(detail)}</div>
            </Card>
          </Col>
        );
      })}
    </Row>
  );
}

function DebeziumFailurePanel({
  pipeline,
  onRestart,
}: {
  pipeline: unknown;
  onRestart: (name: string) => void;
}) {
  const [traceOpen, setTraceOpen] = useState(false);
  const pipelineObj = (pipeline as Record<string, unknown>) || {};
  const debezium = (pipelineObj.debezium as Record<string, unknown>) || {};
  const tasks = (debezium.tasks as Array<Record<string, unknown>>) || [];
  const failedTasks = tasks.filter((t) => t.state === 'FAILED');
  const debeziumFailed = debezium.status === 'FAILED' || debezium.status === 'down';

  if (failedTasks.length === 0 && !debeziumFailed) return null;

  const connectorName =
    (debezium.connector as string) || (debezium.name as string) || 'debezium-connector';

  return (
    <Card
      size="small"
      style={{ marginBottom: 16, border: '1px solid #ff4d4f' }}
      title={
        <span>
          <ExclamationCircleOutlined style={{ color: '#ff4d4f', marginRight: 8 }} />
          Debezium — {failedTasks.length > 0 ? `${failedTasks.length} task(s) FAILED` : 'connector down'}
        </span>
      }
      extra={
        <Button type="primary" danger size="small" onClick={() => onRestart(connectorName)}>
          Restart Connector
        </Button>
      }
    >
      {failedTasks.map((t) => (
        <div key={String(t.id)} style={{ marginBottom: 8 }}>
          <Tag color="red">
            Task {String(t.id)}: {String(t.state)}
          </Tag>
          {t.trace ? (
            <>
              <Button type="link" size="small" onClick={() => setTraceOpen((prev) => !prev)}>
                {traceOpen ? 'Ẩn trace' : 'Xem trace'}
              </Button>
              {traceOpen && (
                <pre
                  style={{
                    background: '#fff2f0',
                    padding: 8,
                    fontSize: 11,
                    maxHeight: 200,
                    overflow: 'auto',
                    whiteSpace: 'pre-wrap',
                    marginTop: 4,
                  }}
                >
                  {String(t.trace)}
                </pre>
              )}
            </>
          ) : null}
        </div>
      ))}
    </Card>
  );
}

function PipelineStatsBody({
  pipeline,
  failedSync,
}: {
  pipeline: unknown;
  failedSync: unknown;
}) {
  const failed = (failedSync as { count_1h?: number; count_24h?: number }) || {};
  const pipelineObj = (pipeline as Record<string, unknown>) || {};
  const worker = (pipelineObj.worker as Record<string, unknown>) || {};
  const tablesRegistered = (worker?.tables_registered as number) ?? 0;

  return (
    <Row gutter={[16, 16]}>
      <Col xs={12} md={6}>
        <Card size="small">
          <Statistic title="Tables registered" value={tablesRegistered} />
        </Card>
      </Col>
      <Col xs={12} md={6}>
        <Card size="small">
          <Statistic
            title="Failed 1h"
            value={failed.count_1h ?? 0}
            valueStyle={{ color: (failed.count_1h ?? 0) > 0 ? '#cf1322' : '#3f8600' }}
          />
        </Card>
      </Col>
      <Col xs={12} md={6}>
        <Card size="small">
          <Statistic
            title="Failed 24h"
            value={failed.count_24h ?? 0}
            valueStyle={{ color: (failed.count_24h ?? 0) > 0 ? '#cf1322' : '#3f8600' }}
          />
        </Card>
      </Col>
      <Col xs={12} md={6}>
        <Card size="small">
          <Statistic
            title="Worker status"
            value={String(worker?.status ?? 'unknown')}
          />
        </Card>
      </Col>
    </Row>
  );
}

function LatencyBody({ data }: { data: unknown }) {
  const latency =
    (data as {
      p50_ms?: number;
      p95_ms?: number;
      p99_ms?: number;
      source?: string;
    }) || {};
  const source = (latency.source || 'unknown').toLowerCase();

  const sourceLabel: Record<string, { color: string; text: string; hint: string }> = {
    prometheus: {
      color: 'green',
      text: 'Prometheus',
      hint: 'Tính toán từ histogram_quantile trên Prometheus — chính xác.',
    },
    fallback_worker_metrics: {
      color: 'orange',
      text: 'Fallback /metrics',
      hint: 'Prometheus không sẵn sàng — CMS tự scrape /metrics của Worker và tính quantile trong process. Giảm chính xác khi có nhiều worker.',
    },
    unknown: {
      color: 'red',
      text: 'Unknown',
      hint: 'Không xác định được nguồn số liệu latency — cần kiểm tra Worker /metrics hoặc Prometheus.',
    },
  };
  const meta = sourceLabel[source] || sourceLabel.unknown;

  const fmt = (v?: number) => (v != null ? `${v.toFixed(0)} ms` : '—');

  return (
    <>
      <Space style={{ marginBottom: 8 }}>
        <Text>Nguồn số liệu:</Text>
        <Tag color={meta.color}>{meta.text}</Tag>
        <Tooltip title={meta.hint}>
          <QuestionCircleOutlined style={{ color: '#999' }} />
        </Tooltip>
      </Space>
      <Row gutter={[16, 16]}>
        <Col xs={8}>
          <Card size="small">
            <Statistic title="P50" value={fmt(latency.p50_ms)} />
          </Card>
        </Col>
        <Col xs={8}>
          <Card size="small">
            <Statistic title="P95" value={fmt(latency.p95_ms)} />
          </Card>
        </Col>
        <Col xs={8}>
          <Card size="small">
            <Statistic title="P99" value={fmt(latency.p99_ms)} />
          </Card>
        </Col>
      </Row>
    </>
  );
}

interface ReconRow {
  table?: string;
  source_count?: number;
  dest_count?: number;
  drift_pct?: string | number;
  status?: string;
  last_check?: string;
}

function ReconciliationBody({ data }: { data: unknown }) {
  const rows = Array.isArray(data) ? (data as ReconRow[]) : [];
  if (rows.length === 0) {
    return <Empty description="Chưa có báo cáo đối soát" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }
  return (
    <Table
      dataSource={rows}
      rowKey={(r) => r.table || Math.random().toString(36)}
      size="small"
      pagination={false}
      columns={[
        {
          title: 'Bảng',
          dataIndex: 'table',
          render: (v: string) => <strong>{v}</strong>,
        },
        { title: 'Source', dataIndex: 'source_count' },
        { title: 'Dest', dataIndex: 'dest_count' },
        {
          title: 'Drift %',
          dataIndex: 'drift_pct',
          render: (v: string | number) => {
            const val = Math.abs(parseFloat(String(v ?? '0')));
            return val > 0 ? <Tag color="orange">{v}%</Tag> : <Tag color="green">0%</Tag>;
          },
        },
        {
          title: 'Trạng thái',
          dataIndex: 'status',
          render: (v: string) =>
            v === 'ok' ? (
              <Tag color="green">Khớp</Tag>
            ) : v === 'error' ? (
              <Tag color="red">Lỗi nguồn</Tag>
            ) : (
              <Tag color="orange">Lệch</Tag>
            ),
        },
        {
          title: 'Kiểm tra lúc',
          dataIndex: 'last_check',
          render: (v: string) =>
            v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : '-',
        },
      ]}
    />
  );
}

interface AlertItem {
  component?: string;
  level?: string;
  message?: string;
}

function AlertsBanner({ data }: { data: unknown }) {
  const alerts = Array.isArray(data) ? (data as AlertItem[]) : [];
  if (alerts.length === 0) {
    return <Empty description="Không có cảnh báo" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }
  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      {alerts.map((a, i) => (
        <Alert
          key={`${a.component}-${i}`}
          type={
            a.level === 'critical' ? 'error' : a.level === 'warning' ? 'warning' : 'info'
          }
          message={`[${a.component || '-'}] ${a.message || ''}`}
          showIcon
        />
      ))}
    </Space>
  );
}

interface EventRow {
  time?: string;
  operation?: string;
  table?: string;
  status?: string;
}

function RecentEventsBody({ data }: { data: unknown }) {
  // Support both raw array and `{ events: [...] }` shape (Plan v3 §12).
  const events: EventRow[] = Array.isArray(data)
    ? (data as EventRow[])
    : ((data as { events?: EventRow[] })?.events ?? []);
  if (events.length === 0) {
    return <Empty description="Không có sự kiện" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }
  return (
    <Table
      dataSource={events}
      rowKey={(_, i) => `${i}`}
      size="small"
      pagination={false}
      columns={[
        {
          title: 'Thời gian',
          dataIndex: 'time',
          width: 160,
          render: (v: string) =>
            v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : '-',
        },
        {
          title: 'Tác vụ',
          dataIndex: 'operation',
          width: 200,
          render: (v: string) => <Tag>{v}</Tag>,
        },
        { title: 'Bảng', dataIndex: 'table', width: 150 },
        {
          title: 'Trạng thái',
          dataIndex: 'status',
          width: 80,
          render: (v: string) =>
            v === 'success' ? <Tag color="green">OK</Tag> : <Tag color="red">{v}</Tag>,
        },
      ]}
    />
  );
}

// ---------- Page ----------

export default function SystemHealth() {
  const queryClient = useQueryClient();
  const health = useSystemHealth();
  const restart = useRestartConnector();
  const [restartOpen, setRestartOpen] = useState(false);
  const [restartTarget, setRestartTarget] = useState<string>('debezium-connector');

  const snapshot = health.data;
  const sections = useMemo<UnifiedSections | null>(
    () => (snapshot ? resolveSections(snapshot) : null),
    [snapshot],
  );

  // ---- Loading ----
  if (health.isLoading && !snapshot) {
    return (
      <div>
        <Space style={{ marginBottom: 16 }}>
          <Title level={4} style={{ margin: 0 }}>
            <DashboardOutlined /> Sức khỏe hệ thống
          </Title>
        </Space>
        <Skeleton active paragraph={{ rows: 4 }} />
        <div style={{ height: 16 }} />
        <Skeleton active paragraph={{ rows: 6 }} />
      </div>
    );
  }

  // ---- Full-page error ----
  if (health.isError || !snapshot || !sections) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          type="error"
          showIcon
          message="Không tải được System Health"
          description={
            <Space direction="vertical" style={{ width: '100%' }}>
              <Text>
                {health.error instanceof Error
                  ? health.error.message
                  : 'Backend /api/system/health không phản hồi.'}
              </Text>
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                loading={health.isFetching}
                onClick={() => health.refetch()}
              >
                Thử lại
              </Button>
            </Space>
          }
        />
      </div>
    );
  }

  const handleRestartClick = (connectorName: string) => {
    setRestartTarget(connectorName);
    setRestartOpen(true);
  };

  const cacheAge = Number(snapshot.cache_age_seconds ?? 0);
  const overall =
    (snapshot.overall as string) ||
    (normalizeStatus(sections.infrastructure.status) === 'ok' ? 'healthy' : 'degraded');

  return (
    <div>
      <Space
        style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }}
      >
        <Title level={4} style={{ margin: 0 }}>
          <DashboardOutlined /> Sức khỏe hệ thống
          {overall === 'healthy' && <Tag color="green" style={{ marginLeft: 8 }}>Healthy</Tag>}
          {overall === 'degraded' && <Tag color="orange" style={{ marginLeft: 8 }}>Degraded</Tag>}
          {overall === 'critical' && <Tag color="red" style={{ marginLeft: 8 }}>Critical</Tag>}
        </Title>
        <Space>
          <Text type="secondary" style={{ fontSize: 12 }}>
            Cache age: {cacheAge}s
          </Text>
          <Button
            icon={<ReloadOutlined />}
            onClick={() => health.refetch()}
            loading={health.isFetching}
          >
            Làm mới
          </Button>
        </Space>
      </Space>

      {cacheAge > 60 && (
        <Alert
          type="warning"
          showIcon
          message="Dữ liệu có thể đã cũ"
          description={`Snapshot hiện tại cũ hơn ${cacheAge}s (ngưỡng 60s). Background collector có thể đang chậm.`}
          style={{ marginBottom: 12 }}
        />
      )}

      <HealthSection title="Infrastructure" section={sections.infrastructure}>
        {(data) => (
          <InfrastructureBody data={data} pipeline={sections.pipeline.data} />
        )}
      </HealthSection>

      <DebeziumFailurePanel
        pipeline={sections.pipeline.data}
        onRestart={handleRestartClick}
      />

      <HealthSection title="Pipeline" section={sections.pipeline}>
        {(data) => (
          <PipelineStatsBody
            pipeline={data}
            failedSync={(snapshot as { failed_sync?: unknown }).failed_sync}
          />
        )}
      </HealthSection>

      <HealthSection title="Latency" section={sections.latency}>
        {(data) => <LatencyBody data={data} />}
      </HealthSection>

      <HealthSection title="Đối soát dữ liệu" section={sections.reconciliation}>
        {(data) => <ReconciliationBody data={data} />}
      </HealthSection>

      <HealthSection title="Cảnh báo" section={sections.alerts}>
        {(data) => <AlertsBanner data={data} />}
      </HealthSection>

      <HealthSection title="Sự kiện gần đây" section={sections.recent_events}>
        {(data) => <RecentEventsBody data={data} />}
      </HealthSection>

      <ConfirmDestructiveModal
        open={restartOpen}
        title="Khởi động lại Debezium Connector"
        description="Pipeline sẽ gián đoạn 30-60s. Hãy chắc chắn đã thông báo cho team trước khi tiếp tục."
        targetName={restartTarget}
        actionLabel="Restart Connector"
        danger
        loading={restart.isPending}
        onConfirm={async (reason) => {
          await restart.mutateAsync({ reason, connectorName: restartTarget });
          setRestartOpen(false);
          queryClient.invalidateQueries({ queryKey: ['system-health'] });
        }}
        onCancel={() => setRestartOpen(false)}
      />
    </div>
  );
}
