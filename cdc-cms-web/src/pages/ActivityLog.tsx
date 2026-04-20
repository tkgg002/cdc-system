import { useEffect, useState, useCallback } from 'react';
import { Table, Tag, Select, Space, Card, Row, Col, Statistic, Typography, Button, Tooltip } from 'antd';
import { ReloadOutlined, CheckCircleOutlined, CloseCircleOutlined, ClockCircleOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { cmsApi } from '../services/api';

const { Title } = Typography;

interface ActivityLogEntry {
  id: number;
  operation: string;
  target_table: string;
  status: string;
  rows_affected: number;
  duration_ms: number | null;
  details: any;
  error_message: string | null;
  triggered_by: string;
  started_at: string;
  completed_at: string | null;
}

interface OpStat {
  operation: string;
  total: number;
  success: number;
  error: number;
  skipped: number;
}

const statusColor: Record<string, string> = {
  success: 'green',
  error: 'red',
  running: 'blue',
  skipped: 'orange',
};

const operationColor: Record<string, string> = {
  bridge: 'cyan',
  transform: 'purple',
  'field-scan': 'geekblue',
  'periodic-cycle': 'blue',
  'reload-cache': 'gold',
  'partition-check': 'lime',
  'create-default-columns': 'magenta',
  'drop-gin-index': 'volcano',
};

export default function ActivityLog() {
  const [logs, setLogs] = useState<ActivityLogEntry[]>([]);
  const [stats, setStats] = useState<OpStat[]>([]);
  const [, setRecentErrors] = useState<ActivityLogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [opFilter, setOpFilter] = useState<string>('');
  const [statusFilter, setStatusFilter] = useState<string>('');

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    try {
      const params: any = { page, page_size: 30 };
      if (opFilter) params.operation = opFilter;
      if (statusFilter) params.status = statusFilter;
      const { data: res } = await cmsApi.get('/api/activity-log', { params });
      setLogs(res.data || []);
      setTotal(res.total || 0);
    } catch { /* */ }
    finally { setLoading(false); }
  }, [page, opFilter, statusFilter]);

  const fetchStats = useCallback(async () => {
    try {
      const { data: res } = await cmsApi.get('/api/activity-log/stats');
      setStats(res.stats_24h || []);
      setRecentErrors(res.recent_errors || []);
    } catch { /* */ }
  }, []);

  useEffect(() => { fetchLogs(); }, [fetchLogs]);
  useEffect(() => { fetchStats(); }, [fetchStats]);

  const totalOps = stats.reduce((s, o) => s + o.total, 0);
  const totalErrors = stats.reduce((s, o) => s + o.error, 0);
  const totalSuccess = stats.reduce((s, o) => s + o.success, 0);

  const columns: ColumnsType<ActivityLogEntry> = [
    {
      title: 'Time', dataIndex: 'started_at', width: 160,
      render: (v) => new Date(v).toLocaleString('vi-VN', { hour12: false }),
    },
    {
      title: 'Operation', dataIndex: 'operation', width: 160,
      render: (v) => <Tag color={operationColor[v] || 'default'}>{v}</Tag>,
    },
    { title: 'Table', dataIndex: 'target_table', width: 180, render: (v) => v === '*' ? <Tag>ALL</Tag> : <strong>{v}</strong> },
    {
      title: 'Status', dataIndex: 'status', width: 90,
      render: (v) => <Tag color={statusColor[v] || 'default'}>{v}</Tag>,
    },
    { title: 'Rows', dataIndex: 'rows_affected', width: 80, render: (v) => v > 0 ? v.toLocaleString() : '-' },
    {
      title: 'Duration', dataIndex: 'duration_ms', width: 90,
      render: (v) => v != null ? (v < 1000 ? `${v}ms` : `${(v / 1000).toFixed(1)}s`) : '-',
    },
    { title: 'Triggered', dataIndex: 'triggered_by', width: 100, render: (v) => <Tag>{v}</Tag> },
    {
      title: 'Details', dataIndex: 'details', ellipsis: true,
      render: (v, record) => {
        if (record.error_message) return <Tooltip title={record.error_message}><span style={{ color: 'red' }}>{record.error_message}</span></Tooltip>;
        if (v) return <Tooltip title={JSON.stringify(v, null, 2)}><span>{JSON.stringify(v)}</span></Tooltip>;
        return '-';
      },
    },
  ];

  return (
    <div>
      <Title level={4} style={{ marginBottom: 16 }}>CDC Worker Activity Log</Title>

      {/* Stats Cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card size="small"><Statistic title="Operations (24h)" value={totalOps} prefix={<ClockCircleOutlined />} /></Card>
        </Col>
        <Col span={6}>
          <Card size="small"><Statistic title="Success" value={totalSuccess} prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} /></Card>
        </Col>
        <Col span={6}>
          <Card size="small"><Statistic title="Errors" value={totalErrors} prefix={<CloseCircleOutlined />} valueStyle={{ color: totalErrors > 0 ? '#cf1322' : '#3f8600' }} /></Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            {stats.map(s => (
              <div key={s.operation} style={{ fontSize: 12, marginBottom: 2 }}>
                <Tag color={operationColor[s.operation] || 'default'} style={{ fontSize: 11 }}>{s.operation}</Tag>
                {s.total} ({s.error > 0 ? <span style={{ color: 'red' }}>{s.error} err</span> : <span style={{ color: 'green' }}>ok</span>})
              </div>
            ))}
          </Card>
        </Col>
      </Row>

      {/* Filters */}
      <Space style={{ marginBottom: 12 }}>
        <Select placeholder="Filter operation" allowClear style={{ width: 180 }} value={opFilter || undefined}
          onChange={(v) => { setOpFilter(v || ''); setPage(1); }}>
          {['periodic-cycle', 'bridge', 'transform', 'field-scan', 'partition-check', 'create-default-columns', 'drop-gin-index',
            'kafka-consume-batch', 'cmd-standardize', 'cmd-discover', 'cmd-backfill', 'cmd-bridge-airbyte', 'cmd-batch-transform',
            'recon-check', 'recon-check-all', 'recon-heal', 'retry-failed', 'debezium-signal',
            'registry-update', 'auto-approve-fields', 'scan-airbyte-streams', 'auto-register-stream',
            'bridge-sql', 'bridge-batch-pgx', 'scan-fields', 'standardize', 'discover'].map(op =>
            <Select.Option key={op} value={op}><Tag color={operationColor[op] || 'default'}>{op}</Tag></Select.Option>
          )}
        </Select>
        <Select placeholder="Filter status" allowClear style={{ width: 120 }} value={statusFilter || undefined}
          onChange={(v) => { setStatusFilter(v || ''); setPage(1); }}>
          {['success', 'error', 'running', 'skipped'].map(s =>
            <Select.Option key={s} value={s}><Tag color={statusColor[s]}>{s}</Tag></Select.Option>
          )}
        </Select>
        <Button icon={<ReloadOutlined />} onClick={() => { fetchLogs(); fetchStats(); }}>Refresh</Button>
      </Space>

      {/* Log Table */}
      <Table
        columns={columns}
        dataSource={logs}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={{
          current: page,
          pageSize: 30,
          total,
          onChange: setPage,
          showTotal: (t) => `${t} entries`,
        }}
        rowClassName={(r) => r.status === 'error' ? 'ant-table-row-error' : ''}
      />
    </div>
  );
}
