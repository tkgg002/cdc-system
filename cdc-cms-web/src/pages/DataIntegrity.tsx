/**
 * DataIntegrity page (Phase 7 — React Query + ConfirmDestructiveModal).
 *
 * - Reads recon report + failed-sync logs through React Query hooks
 *   (`useReconReport`, `useFailedLogs`) with stale-while-revalidate 25s /
 *   refetch 30s.
 * - Every destructive action (Check, Heal, Retry, Check-all) goes through
 *   `<ConfirmDestructiveModal>` which enforces an audit reason (>= 10 chars).
 *   Mutations attach `Idempotency-Key` and `X-Action-Reason` headers.
 * - Query invalidation is driven per-action so the table refreshes as soon
 *   as the backend returns success.
 */
import { useMemo, useState } from 'react';
import {
  Button,
  Card,
  Col,
  Empty,
  message,
  Progress,
  Row,
  Select,
  Skeleton,
  Space,
  Statistic,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  Alert,
} from 'antd';
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  InfoCircleOutlined,
  MedicineBoxOutlined,
  ReloadOutlined,
  SyncOutlined,
  ThunderboltOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useQueryClient } from '@tanstack/react-query';
import ConfirmDestructiveModal from '../components/ConfirmDestructiveModal';
import {
  useBackfillSourceTsMutation,
  useBackfillStatus,
  useCheckAllMutation,
  useCheckTableMutation,
  useFailedLogs,
  useHealMutation,
  useReconReport,
  useRetryFailedMutation,
  type BackfillStatusRow,
  type FailedLog,
  type ReconReport,
  type ReconStatus,
} from '../hooks/useReconStatus';
import { ReDetectButton } from '../components/ReDetectButton';
import { lookupReconError } from '../constants/reconErrorMessages';

const { Text } = Typography;

const { Title } = Typography;

// Status colour + VI label lookup (ADR §2.8). `error` is handled separately
// so it can surface the structured `error_code`.
const STATUS_COLOR: Record<ReconStatus, string> = {
  ok: 'green',
  ok_empty: 'default',
  warning: 'gold',
  drift: 'red',
  dest_missing: 'red',
  source_missing_or_stale: 'orange',
  error: 'red',
};

const STATUS_LABEL_VI: Record<ReconStatus, string> = {
  ok: 'Khớp',
  ok_empty: 'Khớp (trống)',
  warning: 'Cảnh báo',
  drift: 'Lệch',
  dest_missing: 'Thiếu dest',
  source_missing_or_stale: 'Source stale',
  error: 'Lỗi',
};

const SYNC_ENGINE_COLOR: Record<string, string> = {
  debezium: 'blue',
  airbyte: 'green',
  both: 'purple',
};

// ---- Modal plan types ---------------------------------------------------

type ModalAction =
  | { kind: 'check-all' }
  | { kind: 'check-table'; table: string; tier: string }
  | { kind: 'heal'; table: string }
  | { kind: 'retry'; id: number; table: string }
  | { kind: 'backfill-source-ts'; table: string };

interface ModalPlan {
  action: ModalAction;
  title: string;
  description: string;
  targetName: string;
  actionLabel: string;
  danger: boolean;
}

// ---- Page --------------------------------------------------------------

export default function DataIntegrity() {
  const queryClient = useQueryClient();
  const reports = useReconReport();
  const failed = useFailedLogs(50);

  const checkAll = useCheckAllMutation();
  const checkTable = useCheckTableMutation();
  const heal = useHealMutation();
  const retry = useRetryFailedMutation();
  const backfill = useBackfillSourceTsMutation();
  const backfillStatus = useBackfillStatus(true);

  const [modalPlan, setModalPlan] = useState<ModalPlan | null>(null);
  const [backfillTarget, setBackfillTarget] = useState<string>('');

  const invalidateReports = () =>
    queryClient.invalidateQueries({ queryKey: ['recon-report'] });
  const invalidateFailed = () =>
    queryClient.invalidateQueries({ queryKey: ['failed-logs'] });

  const reportList = reports.data ?? [];
  const failedLogs = failed.data?.data ?? [];
  const failedTotal = failed.data?.total ?? 0;

  const driftCount = reportList.filter((r) => r.status === 'drift').length;
  const okCount = reportList.filter((r) => r.status === 'ok').length;

  // ---- Modal open handlers ---------------------------------------------

  const openCheckAll = () =>
    setModalPlan({
      action: { kind: 'check-all' },
      title: 'Kiểm tra đối soát toàn bộ bảng',
      description:
        'Tác vụ sẽ khởi chạy Tier 1 trên mọi bảng đang active. Có thể gây tải cho source DB — hãy thực hiện ngoài giờ cao điểm.',
      targetName: 'ALL active tables (Tier 1)',
      actionLabel: 'Kiểm tra tất cả',
      danger: false,
    });

  const openCheckTable = (record: ReconReport) =>
    setModalPlan({
      action: { kind: 'check-table', table: record.target_table, tier: '2' },
      title: `Kiểm tra Tier 2 cho ${record.target_table}`,
      description:
        'Tier 2 sẽ đối chiếu window-based XOR-hash trên cả 2 bên — chỉ đọc, không ghi dữ liệu.',
      targetName: `${record.target_table} (Tier 2)`,
      actionLabel: 'Kiểm tra ID',
      danger: false,
    });

  const openHeal = (record: ReconReport) =>
    setModalPlan({
      action: { kind: 'heal', table: record.target_table },
      title: `Chữa lành drift cho ${record.target_table}`,
      description:
        'Hành động này sẽ upsert dữ liệu từ source sang destination để khớp bản ghi lệch. Không thể undo.',
      targetName: record.target_table,
      actionLabel: 'Chữa lành',
      danger: true,
    });

  const openRetry = (record: FailedLog) =>
    setModalPlan({
      action: { kind: 'retry', id: record.id, table: record.target_table },
      title: `Thử lại đồng bộ bản ghi #${record.id}`,
      description: `Replay upsert cho record ID "${record.record_id}" trên bảng ${record.target_table}.`,
      targetName: `#${record.id} (${record.target_table})`,
      actionLabel: 'Thử lại',
      danger: false,
    });

  const openBackfillSourceTs = () =>
    setModalPlan({
      action: { kind: 'backfill-source-ts', table: backfillTarget },
      title: 'Backfill `_source_ts` cho rows cũ',
      description:
        'Quét rows có `_source_ts IS NULL` và populate từ Mongo source. Chạy background — có thể mất vài phút tùy data. Chỉ enrich, không overwrite rows đã có timestamp.',
      targetName: backfillTarget ? backfillTarget : 'ALL active tables',
      actionLabel: 'Bắt đầu Backfill',
      danger: false,
    });

  // ---- Modal confirm dispatcher ----------------------------------------

  const handleConfirm = async (reason: string) => {
    if (!modalPlan) return;
    const action = modalPlan.action;
    try {
      if (action.kind === 'check-all') {
        await checkAll.mutateAsync({ reason });
        message.success('Đã kích hoạt kiểm tra — kết quả sẽ cập nhật trong vài phút.');
        setTimeout(invalidateReports, 5000);
      } else if (action.kind === 'check-table') {
        await checkTable.mutateAsync({ table: action.table, tier: action.tier, reason });
        message.success(`Đang kiểm tra Tier ${action.tier} cho ${action.table}…`);
        setTimeout(invalidateReports, 10000);
      } else if (action.kind === 'heal') {
        await heal.mutateAsync({ table: action.table, reason });
        message.success(`Đang chữa lành ${action.table}…`);
        setTimeout(invalidateReports, 10000);
      } else if (action.kind === 'retry') {
        await retry.mutateAsync({ id: action.id, reason });
        message.success('Đang thử lại…');
        setTimeout(invalidateFailed, 3000);
      } else if (action.kind === 'backfill-source-ts') {
        const res = await backfill.mutateAsync({ table: action.table, reason });
        message.success(
          `Backfill đã được khởi chạy (run_id ${res.run_id.slice(0, 8)}…) — tiến độ cập nhật mỗi 5 giây.`,
        );
        setTimeout(() => {
          queryClient.invalidateQueries({ queryKey: ['backfill-status'] });
          queryClient.invalidateQueries({ queryKey: ['system-health'] });
          queryClient.invalidateQueries({ queryKey: ['recon-status'] });
        }, 2000);
      }
      setModalPlan(null);
    } catch (err) {
      const msg =
        (err as { response?: { data?: { error?: string } } }).response?.data?.error ||
        (err instanceof Error ? err.message : 'Lỗi không xác định');
      message.error(msg);
    }
  };

  const mutationPending =
    checkAll.isPending ||
    checkTable.isPending ||
    heal.isPending ||
    retry.isPending ||
    backfill.isPending;

  // Build the select options from known recon report tables plus a
  // fallback "all" option.
  const tableOptions = useMemo(() => {
    const set = new Set<string>();
    reportList.forEach((r) => set.add(r.target_table));
    return Array.from(set).sort();
  }, [reportList]);

  const backfillRows = backfillStatus.data?.data ?? [];

  // ---- Columns ---------------------------------------------------------

  // v4 columns (ADR §2.2, §2.3, §2.7, §2.8). Full-count columns surface
  // the daily aggregate; window columns surface the 7-day recon sample;
  // drift% uses the unsigned formula; errors translate to VI via
  // `lookupReconError`.
  const reportColumns: ColumnsType<ReconReport> = [
    { title: 'Bảng', dataIndex: 'target_table', key: 'target_table' },
    {
      title: (
        <Space size={4}>
          Sync Engine
          <Tooltip title="Airbyte hoặc Debezium quản lý sync bảng này">
            <InfoCircleOutlined tabIndex={0} aria-label="Giải thích Sync Engine" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'sync_engine',
      key: 'sync_engine',
      render: (v: string | null | undefined) => (
        <Tag color={v ? (SYNC_ENGINE_COLOR[v] ?? 'default') : 'default'}>{v ?? '-'}</Tag>
      ),
    },
    {
      title: (
        <Space size={4}>
          Total Source
          <Tooltip title="Full count của Mongo collection (cập nhật hằng ngày 03:00 UTC)">
            <InfoCircleOutlined tabIndex={0} aria-label="Giải thích Total Source" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'full_source_count',
      key: 'full_source_count',
      render: (v: number | null | undefined) =>
        v == null ? <Text type="secondary">—</Text> : v.toLocaleString(),
    },
    {
      title: (
        <Space size={4}>
          Total Dest
          <Tooltip title="Full count của Postgres table">
            <InfoCircleOutlined tabIndex={0} aria-label="Giải thích Total Dest" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'full_dest_count',
      key: 'full_dest_count',
      render: (v: number | null | undefined) =>
        v == null ? <Text type="secondary">—</Text> : v.toLocaleString(),
    },
    {
      title: (
        <Space size={4}>
          Source (7d window)
          <Tooltip title="Số document trong Mongo filter theo timestamp_field trong 7 ngày gần nhất. Dùng để detect DRIFT RECENT, không phải full table.">
            <InfoCircleOutlined tabIndex={0} aria-label="Giải thích Source 7d window" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'source_count',
      key: 'source_count',
      render: (v: number | null | undefined) =>
        v == null ? <Tag color="error">Query fail</Tag> : v.toLocaleString(),
    },
    {
      title: 'Dest (7d window)',
      dataIndex: 'dest_count',
      key: 'dest_count',
      render: (v: number | null | undefined) => v?.toLocaleString() ?? '-',
    },
    {
      title: (
        <Space size={4}>
          Drift %
          <Tooltip title="|Source - Dest| / max(Source, Dest) × 100. Unsigned, bounded 0-100%.">
            <InfoCircleOutlined tabIndex={0} aria-label="Giải thích công thức Drift" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'drift_pct',
      key: 'drift_pct',
      render: (v: number | null | undefined) => {
        if (v == null) return '-';
        const color = v < 0.5 ? 'default' : v < 5 ? 'gold' : 'red';
        return <Tag color={color}>{v.toFixed(2)}%</Tag>;
      },
    },
    {
      title: 'Trạng thái',
      dataIndex: 'status',
      key: 'status',
      render: (status: ReconStatus, row: ReconReport) => {
        if (status === 'error') {
          const { message: msg, severity } = lookupReconError(row.error_code);
          return (
            <Tooltip title={msg}>
              <Tag color={severity === 'critical' ? 'red' : 'orange'}>
                {row.error_code || 'Lỗi'}
              </Tag>
            </Tooltip>
          );
        }
        return (
          <Tag color={STATUS_COLOR[status] ?? 'default'}>
            {STATUS_LABEL_VI[status] ?? status}
          </Tag>
        );
      },
    },
    {
      title: 'Timestamp field',
      dataIndex: 'timestamp_field',
      key: 'timestamp_field',
      render: (v: string | null | undefined, row: ReconReport) => (
        <Tooltip
          title={`Confidence: ${row.timestamp_field_confidence ?? '-'} | Source: ${row.timestamp_field_source ?? '-'}`}
        >
          <Space size={4}>
            <Text code>{v || '-'}</Text>
            {row.timestamp_field_source === 'admin_override' && (
              <Tag color="purple">Manual</Tag>
            )}
          </Space>
        </Tooltip>
      ),
    },
    {
      title: 'Kiểm tra lúc',
      dataIndex: 'checked_at',
      key: 'checked_at',
      render: (v: string | null | undefined) =>
        v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : '-',
    },
    {
      title: 'Thao tác',
      key: 'actions',
      render: (_: unknown, record: ReconReport) => (
        <Space>
          {record.registry_id != null && (
            <ReDetectButton targetTable={record.target_table} registryId={record.registry_id} />
          )}
          <Button
            size="small"
            icon={<SyncOutlined />}
            onClick={() => openCheckTable(record)}
          >
            Kiểm tra
          </Button>
          {(record.status === 'drift' || record.status === 'dest_missing') && (
            <Button
              size="small"
              type="primary"
              danger
              icon={<MedicineBoxOutlined />}
              onClick={() => openHeal(record)}
            >
              Chữa lành
            </Button>
          )}
        </Space>
      ),
    },
  ];

  const failedColumns: ColumnsType<FailedLog> = [
    { title: 'Bảng', dataIndex: 'target_table', width: 150 },
    { title: 'Record ID', dataIndex: 'record_id', width: 200, ellipsis: true },
    {
      title: 'Loại lỗi',
      dataIndex: 'error_type',
      width: 120,
      render: (v) => <Tag color="red">{v || 'unknown'}</Tag>,
    },
    {
      title: 'Lỗi',
      dataIndex: 'error_message',
      ellipsis: true,
      render: (v) => (
        <Tooltip title={v}>
          <span>{v}</span>
        </Tooltip>
      ),
    },
    { title: 'Thử lại', dataIndex: 'retry_count', width: 70 },
    {
      title: 'Trạng thái',
      dataIndex: 'status',
      width: 100,
      render: (v) =>
        v === 'resolved' ? (
          <Tag color="green">Đã xử lý</Tag>
        ) : v === 'retrying' ? (
          <Tag color="blue">Đang thử</Tag>
        ) : (
          <Tag color="red">Lỗi</Tag>
        ),
    },
    {
      title: 'Thời gian',
      dataIndex: 'created_at',
      width: 150,
      render: (v) => new Date(v).toLocaleString('vi-VN', { hour12: false }),
    },
    {
      title: '',
      width: 90,
      render: (_, record) =>
        record.status === 'failed' ? (
          <Button size="small" onClick={() => openRetry(record)}>
            Thử lại
          </Button>
        ) : null,
    },
  ];

  // ---- Render ----------------------------------------------------------

  const showReportLoading = reports.isLoading && !reports.data;
  const showReportError = reports.isError && !reports.data;

  return (
    <div>
      <Title level={4}>Toàn vẹn dữ liệu</Title>

      {showReportError && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="Không tải được báo cáo đối soát"
          description={
            <Space direction="vertical">
              <span>
                {reports.error instanceof Error
                  ? reports.error.message
                  : 'Backend /api/reconciliation/report không phản hồi.'}
              </span>
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={() => reports.refetch()}
                loading={reports.isFetching}
              >
                Thử lại
              </Button>
            </Space>
          }
        />
      )}

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card size="small">
            <Statistic title="Tổng bảng" value={reportList.length} />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Khớp"
              value={okCount}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Lệch"
              value={driftCount}
              prefix={<WarningOutlined />}
              valueStyle={{ color: driftCount > 0 ? '#cf1322' : '#3f8600' }}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Lỗi đồng bộ"
              value={failedTotal}
              valueStyle={{ color: failedTotal > 0 ? '#faad14' : '#3f8600' }}
            />
          </Card>
        </Col>
      </Row>

      <Space style={{ marginBottom: 12 }} wrap>
        <Button
          type="primary"
          icon={<SyncOutlined />}
          loading={checkAll.isPending}
          onClick={openCheckAll}
        >
          Kiểm tra tất cả (Tier 1)
        </Button>
        <Space.Compact>
          <Select
            placeholder="Tất cả bảng"
            allowClear
            style={{ width: 220 }}
            value={backfillTarget || undefined}
            onChange={(v) => setBackfillTarget(v ?? '')}
            options={tableOptions.map((t) => ({ value: t, label: t }))}
          />
          <Button
            icon={<ThunderboltOutlined />}
            loading={backfill.isPending}
            onClick={openBackfillSourceTs}
          >
            Backfill Source Timestamp
          </Button>
        </Space.Compact>
        <Button
          icon={<ReloadOutlined />}
          loading={reports.isFetching || failed.isFetching}
          onClick={() => {
            reports.refetch();
            failed.refetch();
            backfillStatus.refetch();
          }}
        >
          Làm mới
        </Button>
      </Space>

      <Tabs
        defaultActiveKey="overview"
        items={[
          {
            key: 'overview',
            label: 'Tổng quan',
            children: showReportLoading ? (
              <Skeleton active paragraph={{ rows: 6 }} />
            ) : reportList.length === 0 ? (
              <Empty description="Chưa có báo cáo đối soát" />
            ) : (
              <Table
                columns={reportColumns}
                dataSource={reportList}
                rowKey="id"
                loading={reports.isFetching}
                size="small"
                pagination={false}
              />
            ),
          },
          {
            key: 'failed',
            label: `Lỗi đồng bộ (${failedTotal})`,
            children: failed.isLoading && !failed.data ? (
              <Skeleton active paragraph={{ rows: 6 }} />
            ) : failed.isError && !failed.data ? (
              <Alert
                type="error"
                showIcon
                message="Không tải được danh sách lỗi đồng bộ"
                description={
                  <Button
                    type="primary"
                    icon={<ReloadOutlined />}
                    onClick={() => failed.refetch()}
                  >
                    Thử lại
                  </Button>
                }
              />
            ) : failedLogs.length === 0 ? (
              <Empty description="Không có lỗi đồng bộ" />
            ) : (
              <Table
                columns={failedColumns}
                dataSource={failedLogs}
                rowKey="id"
                loading={failed.isFetching}
                size="small"
                pagination={{ pageSize: 30 }}
              />
            ),
          },
          {
            key: 'backfill',
            label: 'Backfill _source_ts',
            children: backfillStatus.isLoading && !backfillStatus.data ? (
              <Skeleton active paragraph={{ rows: 4 }} />
            ) : backfillRows.length === 0 ? (
              <Empty description="Chưa có run backfill — nhấn Backfill Source Timestamp ở trên để chạy." />
            ) : (
              <Table<BackfillStatusRow>
                rowKey="id"
                size="small"
                loading={backfillStatus.isFetching}
                dataSource={backfillRows}
                pagination={false}
                columns={[
                  { title: 'Bảng', dataIndex: 'table_name', width: 180 },
                  {
                    title: 'Trạng thái',
                    dataIndex: 'status',
                    width: 110,
                    render: (s: string) => {
                      if (s === 'success') return <Tag color="green">Thành công</Tag>;
                      if (s === 'running')
                        return (
                          <Tag color="blue" icon={<ClockCircleOutlined />}>
                            Đang chạy
                          </Tag>
                        );
                      if (s === 'failed') return <Tag color="red">Lỗi</Tag>;
                      return <Tag>{s}</Tag>;
                    },
                  },
                  {
                    title: 'Tiến độ',
                    dataIndex: 'percent_done',
                    render: (_: number, r) => (
                      <Progress
                        percent={Math.round(r.percent_done)}
                        size="small"
                        status={
                          r.status === 'failed'
                            ? 'exception'
                            : r.null_remaining === 0
                              ? 'success'
                              : 'active'
                        }
                      />
                    ),
                  },
                  {
                    title: 'Đã điền / Tổng',
                    width: 150,
                    render: (_: unknown, r) =>
                      `${(r.total_rows - r.null_remaining).toLocaleString()} / ${r.total_rows.toLocaleString()}`,
                  },
                  {
                    title: 'Còn NULL',
                    dataIndex: 'null_remaining',
                    width: 110,
                    render: (v: number) =>
                      v > 0 ? <Tag color="orange">{v.toLocaleString()}</Tag> : <Tag color="green">0</Tag>,
                  },
                  {
                    title: 'Đã cập nhật (run)',
                    dataIndex: 'heal_actions',
                    width: 130,
                    render: (v: number) => (v ?? 0).toLocaleString(),
                  },
                  {
                    title: 'Bắt đầu',
                    dataIndex: 'started_at',
                    width: 160,
                    render: (v: string) => new Date(v).toLocaleString('vi-VN', { hour12: false }),
                  },
                  {
                    title: 'Kết thúc',
                    dataIndex: 'finished_at',
                    width: 160,
                    render: (v: string | null) =>
                      v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : '—',
                  },
                ]}
              />
            ),
          },
        ]}
      />

      {modalPlan && (
        <ConfirmDestructiveModal
          open={!!modalPlan}
          title={modalPlan.title}
          description={modalPlan.description}
          targetName={modalPlan.targetName}
          actionLabel={modalPlan.actionLabel}
          danger={modalPlan.danger}
          loading={mutationPending}
          onConfirm={handleConfirm}
          onCancel={() => setModalPlan(null)}
        />
      )}
    </div>
  );
}
