import { useEffect, useState, useCallback, useMemo } from 'react';
import { Table, Tag, Select, Switch, Button, Space, Modal, Form, Input, message, Upload, Badge, Collapse, Typography, Progress, Tooltip } from 'antd';
import { PlusOutlined, UploadOutlined, SyncOutlined, DatabaseOutlined, SearchOutlined, ToolOutlined, ThunderboltOutlined, RocketOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useNavigate } from 'react-router-dom';
import { cmsApi } from '../services/api';
import type { TableRegistry as TRegistry } from '../types';
import { useScanFields } from '../hooks/useRegistry';
import DispatchStatusBadge from '../components/DispatchStatusBadge';
import ConfirmDestructiveModal from '../components/ConfirmDestructiveModal';

const { Panel } = Collapse;
const { Title } = Typography;

// SyncStatusIndicator — Gap 3 Option B: fetch real Debezium connector status
// via /api/v1/system/connectors. Match by collection.include.list entry.
const SyncStatusIndicator = ({ sourceDB, sourceTable }: { sourceDB: string; sourceTable: string }) => {
  const [status, setStatus] = useState<string>('loading');
  const [connectorName, setConnectorName] = useState<string>('');

  const fetchStatus = useCallback(async () => {
    setStatus('loading');
    try {
      const { data: res } = await cmsApi.get('/api/v1/system/connectors');
      const list = res.data || res || [];
      const needle = `${sourceDB}.${sourceTable}`;
      const match = list.find((c: { config?: Record<string, string> }) => {
        const include = c.config?.['collection.include.list'] || '';
        return include.split(',').map((s) => s.trim()).includes(needle);
      });
      if (match) {
        setConnectorName(match.name || '');
        setStatus(match.state || 'UNKNOWN');
      } else {
        setStatus('not_configured');
      }
    } catch {
      setStatus('error');
    }
  }, [sourceDB, sourceTable]);

  useEffect(() => { fetchStatus(); }, [fetchStatus]);

  const handleRefresh = (e: React.MouseEvent) => { e.stopPropagation(); fetchStatus(); };

  const badgeStatus =
    status === 'RUNNING' ? 'success' :
    status === 'PAUSED' ? 'warning' :
    status === 'FAILED' ? 'error' :
    status === 'not_configured' ? 'default' :
    status === 'loading' ? 'processing' : 'default';

  const label =
    status === 'not_configured' ? 'Chưa có connector' :
    status === 'loading' ? '...' :
    status === 'error' ? 'Lỗi' :
    status;

  return (
    <Tooltip title={connectorName ? `Connector: ${connectorName}` : 'Không có Debezium connector match collection này'}>
      <Space size={4}>
        <Badge status={badgeStatus as 'success' | 'warning' | 'error' | 'default' | 'processing'} text={label} />
        <Button icon={<SyncOutlined />} size="small" type="text" onClick={handleRefresh} title="Refresh" />
      </Space>
    </Tooltip>
  );
};

const TransformProgress = ({ id }: { id: number }) => {
  const [status, setStatus] = useState<{ total_rows: number; transformed_rows: number; pending_rows: number } | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    cmsApi.get(`/api/registry/${id}/transform-status`)
      .then(({ data }) => { setStatus(data); setError(false); })
      .catch(() => { setStatus(null); setError(true); });
  }, [id]);

  if (error) return <Tag color="default">-</Tag>;
  if (!status || status.total_rows === 0) return <Tag>Chưa có data</Tag>;

  const pct = Math.round((status.transformed_rows / status.total_rows) * 100);
  return (
    <Tooltip title={`${status.transformed_rows?.toLocaleString() || 0} / ${status.total_rows?.toLocaleString() || 0} rows`}>
      <Progress percent={pct} size="small" style={{ width: 100 }} status={pct === 100 ? 'success' : 'active'} />
    </Tooltip>
  );
};

// -----------------------------------------------------------------------------
// Async-dispatch actions (202 + polling) — one component per row so hooks stay
// top-level. Wires scan-fields / sync / refresh-catalog to `useAsyncDispatch`.
// -----------------------------------------------------------------------------
type AsyncActionKind = 'scan-fields';

interface AsyncActionsProps {
  record: TRegistry;
  onChange?: () => void;
}

function AsyncRowActions({ record, onChange }: AsyncActionsProps) {
  const scan = useScanFields(record.id, record.target_table);

  const [confirm, setConfirm] = useState<{ open: boolean; kind: AsyncActionKind | null }>({
    open: false,
    kind: null,
  });

  useEffect(() => {
    if (scan.state.status === 'success') {
      message.success(`Quét field: ${scan.state.message || 'hoàn tất'}`);
      onChange?.();
    } else if (scan.state.status === 'error') {
      message.error(`Quét field: ${scan.state.error}`);
    } else if (scan.state.status === 'timeout') {
      message.warning('Quét field: quá thời gian chờ, kiểm tra Activity Log');
    }
  }, [scan.state.status, scan.state.message, scan.state.error, onChange]);

  const openConfirm = (kind: AsyncActionKind) => setConfirm({ open: true, kind });
  const closeConfirm = () => setConfirm({ open: false, kind: null });

  const runConfirmed = async (reason: string) => {
    if (!confirm.kind) return;
    try {
      if (confirm.kind === 'scan-fields') await scan.dispatchAsync({ reason });
      closeConfirm();
    } catch {
      // hook already surfaced the error via message.error
    }
  };

  const scanBusy = scan.isPending;

  const confirmMeta: Record<
    AsyncActionKind,
    { title: string; description: string; actionLabel: string; danger: boolean; loading: boolean }
  > = {
    'scan-fields': {
      title: 'Quét field mới',
      description: 'Gửi lệnh scan-fields cho Worker (async). Worker sẽ đọc _raw_data / schema và ghi field mới vào review queue.',
      actionLabel: 'Gửi scan-fields',
      danger: false,
      loading: scanBusy,
    },
  };

  const active = confirm.kind ? confirmMeta[confirm.kind] : null;

  return (
    <Space direction="vertical" size={4} onClick={(e) => e.stopPropagation()}>
      <Space wrap>
        <Tooltip title="Quét tìm field mới từ dữ liệu (async, 202)">
          <Button
            size="small"
            icon={<SearchOutlined />}
            loading={scanBusy}
            onClick={(e) => { e.stopPropagation(); openConfirm('scan-fields'); }}
          >
            Quét field
          </Button>
        </Tooltip>
      </Space>
      <Space wrap>
        {scan.state.status !== 'idle' && <DispatchStatusBadge state={scan.state} />}
      </Space>
      {active && (
        <ConfirmDestructiveModal
          open={confirm.open}
          title={active.title}
          description={active.description}
          targetName={record.target_table}
          actionLabel={active.actionLabel}
          danger={active.danger}
          loading={active.loading}
          onConfirm={runConfirmed}
          onCancel={closeConfirm}
        />
      )}
    </Space>
  );
}

export default function TableRegistry() {
  const navigate = useNavigate();
  const [data, setData] = useState<TRegistry[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [sourceDBFilter, setSourceDBFilter] = useState<string>('');
  const [registerVisible, setRegisterVisible] = useState(false);
  const [actionLoadingId, setActionLoadingId] = useState<number | null>(null);
  const [activeLoadingId, setActiveLoadingId] = useState<number | null>(null);
  const [form] = Form.useForm();

  // Nhóm dữ liệu theo source_db
  const groupedData = useMemo(() => {
    const groups: Record<string, TRegistry[]> = {};
    data.forEach(item => {
      const db = item.source_db || 'unknown';
      if (!groups[db]) groups[db] = [];
      groups[db].push(item);
    });
    return groups;
  }, [data]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, unknown> = { page, page_size: 100 };
      if (sourceDBFilter) params.source_db = sourceDBFilter;
      const { data: res } = await cmsApi.get('/api/registry', { params });
      setData(res.data || []);
    } catch { /* interceptor */ }
    finally { setLoading(false); }
  }, [page, sourceDBFilter]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const updateEntry = async (id: number, updates: Record<string, unknown>) => {
    if ('is_active' in updates) setActiveLoadingId(id);
    try {
      await cmsApi.patch(`/api/registry/${id}`, updates);
      message.success('Cập nhật thành công');
      fetchData();
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Cập nhật thất bại');
    } finally {
      setActiveLoadingId(null);
    }
  };

  const handleRegister = async (values: Record<string, unknown>) => {
    try {
      await cmsApi.post('/api/registry', values);
      message.success('Table registered');
      setRegisterVisible(false);
      form.resetFields();
      fetchData();
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Register failed');
    }
  };

  // Gap 5b — Snapshot Now: publish NATS cdc.cmd.debezium-signal via CMS
  // /api/tools/trigger-snapshot/:table. Worker writes Mongo debezium_signal
  // collection → Debezium performs incremental snapshot.
  const handleSnapshot = (e: React.MouseEvent, record: TRegistry) => {
    e.stopPropagation();
    Modal.confirm({
      title: `Trigger Debezium snapshot: ${record.source_table}?`,
      content: 'Debezium sẽ thực hiện incremental snapshot collection này. Dùng khi connector vừa add hoặc sau rebuild shadow.',
      okText: 'Snapshot',
      onOk: async () => {
        setActionLoadingId(record.id);
        try {
          await cmsApi.post(
            `/api/tools/trigger-snapshot/${encodeURIComponent(record.source_table)}`,
            { database: record.source_db, collection: record.source_table },
            { headers: { 'Idempotency-Key': `snapshot-${record.id}-${Date.now()}` } },
          );
          message.success(`Snapshot dispatched: ${record.source_table}`);
        } catch (err) {
          const e = err as { response?: { data?: { error?: string; detail?: string } } };
          message.error(e.response?.data?.error || e.response?.data?.detail || 'Snapshot failed');
        } finally {
          setActionLoadingId(null);
        }
      },
    });
  };

  const handleCreateTable = (e: React.MouseEvent, id: number) => {
    e.stopPropagation();
    setActionLoadingId(id);
    cmsApi.post(`/api/registry/${id}/create-default-columns`)
      .then(() => { message.success('Đang tạo table đích + field mặc định...'); fetchData(); })
      .catch((err) => {
        const e = err as { response?: { data?: { error?: string } } };
        message.error(e.response?.data?.error || 'Tạo table thất bại');
      })
      .finally(() => setActionLoadingId(null));
  };

  const handleCreateDefaultFields = (e: React.MouseEvent, id: number) => {
    e.stopPropagation();
    setActionLoadingId(id);
    cmsApi.post(`/api/registry/${id}/standardize`)
      .then(() => { message.success('Đang tạo System Default Fields...'); fetchData(); })
      .catch((err) => {
        const e = err as { response?: { data?: { error?: string } } };
        message.error(e.response?.data?.error || 'Tạo field mặc định thất bại');
      })
      .finally(() => setActionLoadingId(null));
  };

  const handleBulkImport = async (file: File) => {
    const hide = message.loading('Importing...', 0);
    try {
      const text = await file.text();
      const entries = JSON.parse(text);
      await cmsApi.post('/api/registry/batch', entries);
      hide();
      message.success(`Imported ${entries.length} tables`);
      fetchData();
    } catch (err) {
      hide();
      const e = err as { message?: string; response?: { data?: { error?: string } } };
      message.error('Import failed: ' + (e.message || e.response?.data?.error));
    }
    return false; // prevent default upload
  };

  const uniqueSourceDBs = [...new Set(data.map(d => d.source_db))];

  const columns: ColumnsType<TRegistry> = [
    { title: 'Type', dataIndex: 'source_type', width: 80, render: (t: string) => <Tag color={t === 'mongodb' ? 'green' : 'blue'}>{t}</Tag> },
    { title: 'Source DB', dataIndex: 'source_db', width: 120 },
    { title: 'Source Table', dataIndex: 'source_table', width: 180, render: (t) => <strong style={{color: '#1890ff'}}>{t}</strong> },
    { title: 'Target Table', dataIndex: 'target_table', width: 180 },
    {
      title: 'Sync Engine', dataIndex: 'sync_engine', width: 160,
      render: (v: string, record) => (
        <Space direction="vertical" size={0} onClick={e => e.stopPropagation()}>
          <Tag color="blue">{v || 'debezium'}</Tag>
          <SyncStatusIndicator sourceDB={record.source_db} sourceTable={record.source_table} />
        </Space>
      ),
    },
    {
      title: 'Priority', dataIndex: 'priority', width: 110,
      render: (v: string, record) => (
        <Select value={v} size="small" style={{ width: 100 }} onClick={e => e.stopPropagation()}
          onChange={(val) => updateEntry(record.id, { priority: val })}>
          <Select.Option value="critical">Critical</Select.Option>
          <Select.Option value="high">High</Select.Option>
          <Select.Option value="normal">Normal</Select.Option>
          <Select.Option value="low">Low</Select.Option>
        </Select>
      ),
    },
    { title: 'PK', dataIndex: 'primary_key_field', width: 80 },
    {
      title: 'Trạng thái', dataIndex: 'is_active', width: 100,
      render: (v: boolean, record) => (
        <div onClick={e => e.stopPropagation()}>
          <Switch checked={v} size="small" loading={activeLoadingId === record.id}
            onChange={(checked) => updateEntry(record.id, { is_active: checked })} />
        </div>
      ),
    },
    {
      title: 'Data Status', dataIndex: 'sync_status', width: 120,
      render: (v: string, record: TRegistry & { recon_drift?: number }) => {
        const colors: Record<string, string> = { healthy: 'green', drift: 'orange', source_error: 'red', unknown: 'default' };
        const labels: Record<string, string> = { healthy: 'Khớp', drift: `Lệch (${record.recon_drift || 0})`, source_error: 'Lỗi nguồn', unknown: 'Chưa kiểm' };
        return <Tag color={colors[v] || 'default'}>{labels[v] || v || 'Chưa kiểm'}</Tag>;
      },
    },
    { title: 'Transform', width: 130, render: (_, record) => <TransformProgress id={record.id} /> },
    { title: 'Created At', dataIndex: 'created_at', width: 140, render: (v) => new Date(v).toLocaleString() },
    {
      title: 'Thao tác', width: 520, fixed: 'right',
      render: (_, record) => (
        <Space direction="vertical" size={4} onClick={e => e.stopPropagation()}>
          <Space wrap>
            {!record.is_table_created ? (
              <Tooltip title="Tạo table đích + thêm tất cả field đã duyệt">
                <Button size="small" type="primary" icon={<DatabaseOutlined />} loading={actionLoadingId === record.id}
                  onClick={(e) => handleCreateTable(e, record.id)}>Tạo Table</Button>
              </Tooltip>
            ) : (
              <Tooltip title="Thêm System Default Fields vào table đã có">
                <Button size="small" icon={<ToolOutlined />} loading={actionLoadingId === record.id}
                  onClick={(e) => handleCreateDefaultFields(e, record.id)}>Tạo Field MĐ</Button>
              </Tooltip>
            )}
            <Tooltip title="Trigger Debezium incremental snapshot cho collection này">
              <Button size="small" icon={<ThunderboltOutlined />} type="primary" ghost
                loading={actionLoadingId === record.id}
                onClick={(e) => handleSnapshot(e, record)}>Snapshot Now</Button>
            </Tooltip>
            <Tooltip title="Đi tới Master Registry để tạo / chạy Transmute">
              <Button size="small" icon={<RocketOutlined />}
                onClick={(e) => { e.stopPropagation(); navigate(`/masters?source_shadow=${record.target_table}`); }}>
                Manage Masters
              </Button>
            </Tooltip>
          </Space>
          <AsyncRowActions record={record} onChange={fetchData} />
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Title level={4} style={{ marginBottom: 24, textAlign: 'left' }}>CDC Table Registry</Title>

      <Space style={{ marginBottom: 16 }}>
        <Select placeholder="Filter Source DB" allowClear style={{ width: 180 }}
          value={sourceDBFilter || undefined} onChange={(v) => { setSourceDBFilter(v || ''); setPage(1); }}>
          {uniqueSourceDBs.map(db => <Select.Option key={db} value={db}>{db}</Select.Option>)}
        </Select>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setRegisterVisible(true)}>
          Register Table
        </Button>
        <Upload accept=".json" showUploadList={false} beforeUpload={handleBulkImport as unknown as (f: File) => boolean}>
          <Button icon={<UploadOutlined />}>Bulk Import (JSON)</Button>
        </Upload>
        <Button onClick={fetchData}>Refresh</Button>
      </Space>

      <Collapse defaultActiveKey={Object.keys(groupedData)} ghost expandIconPosition="end">
        {Object.entries(groupedData).map(([db, tables]) => (
          <Panel header={
            <Space>
              <DatabaseOutlined style={{ color: '#1890ff' }} />
              <span style={{ fontWeight: 600 }}>Source Database: {db}</span>
              <Tag color="blue">{tables.length} tables</Tag>
            </Space>
          } key={db} style={{ marginBottom: 16, border: '1px solid #f0f0f0', borderRadius: 8, background: '#fafafa' }}>
            <Table
              columns={columns}
              dataSource={tables}
              rowKey="id"
              loading={loading}
              size="small"
              pagination={false}
              scroll={{ x: 1000 }}
              onRow={(record) => ({
                onClick: () => navigate(`/registry/${record.id}/mappings`),
                style: { cursor: 'pointer' }
              })}
            />
          </Panel>
        ))}
      </Collapse>

      {/* Register Modal */}
      <Modal title="Register New Table" open={registerVisible} onOk={() => form.submit()}
        onCancel={() => setRegisterVisible(false)} width={500}>
        <Form form={form} layout="vertical" onFinish={handleRegister}
          initialValues={{ sync_engine: 'debezium', priority: 'normal', primary_key_field: '_id', primary_key_type: 'VARCHAR(24)', source_type: 'mongodb' }}>
          <Form.Item name="source_db" label="Source DB" rules={[{ required: true }]}>
            <Input placeholder="e.g. payment-bill-service" />
          </Form.Item>
          <Form.Item name="source_type" label="Source Type" rules={[{ required: true }]}>
            <Select disabled>
              <Select.Option value="mongodb">MongoDB</Select.Option>
              <Select.Option value="mysql">MySQL</Select.Option>
              <Select.Option value="postgres">Postgres</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item name="source_table" label="Source Table" rules={[{ required: true }]}><Input placeholder="wallet_transactions" /></Form.Item>
          <Form.Item name="target_table" label="Target Table" rules={[{ required: true }]}><Input placeholder="wallet_transactions" /></Form.Item>
          <Form.Item name="sync_engine" label="Sync Engine" initialValue="debezium">
            <Input disabled />
          </Form.Item>
          <Form.Item name="priority" label="Priority">
            <Select><Select.Option value="critical">Critical</Select.Option><Select.Option value="high">High</Select.Option><Select.Option value="normal">Normal</Select.Option><Select.Option value="low">Low</Select.Option></Select>
          </Form.Item>
          <Form.Item name="primary_key_field" label="PK Field"><Input /></Form.Item>
          <Form.Item name="primary_key_type" label="PK Type"><Input /></Form.Item>
          <Form.Item
            name="timestamp_field"
            label="Timestamp Field"
            tooltip="Mongo field used by reconciliation to filter window ($gte/$lt). Default: updated_at. Common overrides: updatedAt, createdAt, lastUpdatedAt. Fallback: _id (extract ObjectID time)."
            initialValue="updated_at"
          >
            <Input placeholder="updated_at" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
