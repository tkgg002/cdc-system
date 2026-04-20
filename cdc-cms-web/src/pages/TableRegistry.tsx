import { useEffect, useState, useCallback, useMemo } from 'react';
import { Table, Tag, Select, Switch, Button, Space, Modal, Form, Input, message, Upload, Badge, Collapse, Typography, Progress, Tooltip } from 'antd';
import { PlusOutlined, UploadOutlined, SyncOutlined, DatabaseOutlined, SearchOutlined, ToolOutlined, BranchesOutlined, ThunderboltOutlined, SwapOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useNavigate } from 'react-router-dom';
import { cmsApi } from '../services/api';
import type { TableRegistry as TRegistry } from '../types';
import { useScanFields, useSyncAirbyte, useRefreshCatalog } from '../hooks/useRegistry';
import DispatchStatusBadge from '../components/DispatchStatusBadge';
import ConfirmDestructiveModal from '../components/ConfirmDestructiveModal';

const { Panel } = Collapse;
const { Title } = Typography;

const SyncStatusIndicator = ({ id, engine }: { id: number, engine: string }) => {
  const [status, setStatus] = useState<string>('loading');

  const fetchStatus = useCallback(async () => {
    if (engine === 'airbyte' || engine === 'both') {
      try {
        const { data: res } = await cmsApi.get(`/api/registry/${id}/status`);
        setStatus(res.status);
      } catch {
        setStatus('error');
      }
    } else {
      setStatus('n/a');
    }
  }, [id, engine]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleRefresh = (e: React.MouseEvent) => {
    e.stopPropagation();
    fetchStatus();
  };

  if (status === 'n/a') return <Tag>N/A</Tag>;
  if (status === 'loading') return <span>...</span>;
  if (status === 'error') return <Tag color="error">Error</Tag>;

  const displayStatus = status === 'stream_disabled' ? 'disabled' : status;
  const badgeStatus = (status === 'active' || status === 'running') ? 'success' :
                      (status === 'inactive' || status === 'stream_disabled' ? 'warning' : 'default');

  return (
    <Space>
      <Badge status={badgeStatus as 'success' | 'warning' | 'default'} text={displayStatus} style={{ textTransform: 'capitalize' }} />
      <Space>
         <Button icon={<SyncOutlined />} size="small" type="text" onClick={handleRefresh} title="Refresh Status" />
      </Space>
    </Space>
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
type AsyncActionKind = 'scan-fields' | 'sync' | 'refresh-catalog';

interface AsyncActionsProps {
  record: TRegistry;
  onChange?: () => void;
}

function AsyncRowActions({ record, onChange }: AsyncActionsProps) {
  const scan = useScanFields(record.id, record.target_table);
  const sync = useSyncAirbyte(record.id, record.target_table);
  const refresh = useRefreshCatalog(record.id, record.target_table);

  const [confirm, setConfirm] = useState<{ open: boolean; kind: AsyncActionKind | null }>({
    open: false,
    kind: null,
  });

  // Surface terminal status via Ant Design notifications.
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

  useEffect(() => {
    if (sync.state.status === 'success') {
      message.success(`Airbyte sync: ${sync.state.message || 'đã dispatch'}`);
      onChange?.();
    } else if (sync.state.status === 'error') {
      message.error(`Airbyte sync: ${sync.state.error}`);
    }
  }, [sync.state.status, sync.state.message, sync.state.error, onChange]);

  useEffect(() => {
    if (refresh.state.status === 'success') {
      message.success(`Refresh catalog: ${refresh.state.message || 'hoàn tất'}`);
      onChange?.();
    } else if (refresh.state.status === 'error') {
      message.error(`Refresh catalog: ${refresh.state.error}`);
    }
  }, [refresh.state.status, refresh.state.message, refresh.state.error, onChange]);

  const openConfirm = (kind: AsyncActionKind) => setConfirm({ open: true, kind });
  const closeConfirm = () => setConfirm({ open: false, kind: null });

  const runConfirmed = async (reason: string) => {
    if (!confirm.kind) return;
    try {
      if (confirm.kind === 'scan-fields') await scan.dispatchAsync({ reason });
      else if (confirm.kind === 'sync') await sync.dispatchAsync({ reason });
      else if (confirm.kind === 'refresh-catalog') await refresh.dispatchAsync({ reason });
      closeConfirm();
    } catch {
      // hook already surfaced the error via message.error
    }
  };

  const scanBusy = scan.isPending;
  const syncBusy = sync.isPending;
  const refreshBusy = refresh.isPending;

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
    sync: {
      title: 'Airbyte Sync',
      description: 'Kích hoạt full Airbyte sync cho table này. Có thể tốn nhiều phút và ảnh hưởng connector quota.',
      actionLabel: 'Gửi Airbyte sync',
      danger: true,
      loading: syncBusy,
    },
    'refresh-catalog': {
      title: 'Refresh catalog',
      description: 'Re-discover schema connection trên Airbyte. Không destructive nhưng sẽ trigger schema-change review.',
      actionLabel: 'Refresh catalog',
      danger: false,
      loading: refreshBusy,
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
        {(record.sync_engine === 'airbyte' || record.sync_engine === 'both') && (
          <>
            <Tooltip title="Trigger Airbyte full sync (async, 202)">
              <Button
                size="small"
                icon={<SyncOutlined />}
                loading={syncBusy}
                onClick={(e) => { e.stopPropagation(); openConfirm('sync'); }}
              >
                Sync
              </Button>
            </Tooltip>
            <Tooltip title="Refresh Airbyte catalog (async, 202)">
              <Button
                size="small"
                icon={<BranchesOutlined />}
                loading={refreshBusy}
                onClick={(e) => { e.stopPropagation(); openConfirm('refresh-catalog'); }}
              >
                Refresh catalog
              </Button>
            </Tooltip>
          </>
        )}
      </Space>
      <Space wrap>
        {scan.state.status !== 'idle' && <DispatchStatusBadge state={scan.state} />}
        {sync.state.status !== 'idle' && <DispatchStatusBadge state={sync.state} />}
        {refresh.state.status !== 'idle' && <DispatchStatusBadge state={refresh.state} />}
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
  const [airbyteSources, setAirbyteSources] = useState<Array<{ sourceId: string; sourceName: string; database?: string; name?: string }>>([]);
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

  const fetchAirbyteSources = async () => {
    try {
      const { data: sources } = await cmsApi.get('/api/airbyte/sources');
      setAirbyteSources(sources);
    } catch { /* error handled by interceptor */ }
  };

  useEffect(() => {
    fetchData();
    fetchAirbyteSources();
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

  const handleBridge = (e: React.MouseEvent, id: number, batch = false) => {
    e.stopPropagation();
    setActionLoadingId(id);
    const url = batch ? `/api/registry/${id}/bridge?mode=batch` : `/api/registry/${id}/bridge`;
    cmsApi.post(url)
      .then(() => message.success(batch ? 'Batch bridge (pgx) submitted' : 'Bridge command submitted'))
      .catch((err) => {
        const e = err as { response?: { data?: { error?: string } } };
        message.error(e.response?.data?.error || 'Bridge failed');
      })
      .finally(() => setActionLoadingId(null));
  };

  const handleTransform = (e: React.MouseEvent, id: number) => {
    e.stopPropagation();
    setActionLoadingId(id);
    cmsApi.post(`/api/registry/${id}/transform`)
      .then(() => message.success('Transform command submitted'))
      .catch((err) => {
        const e = err as { response?: { data?: { error?: string } } };
        message.error(e.response?.data?.error || 'Transform failed');
      })
      .finally(() => setActionLoadingId(null));
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

  const uniqueSourceDBs = [...new Set([
    ...data.map(d => d.source_db),
    ...airbyteSources.map((s) => s.name || (s as { sourceName?: string }).sourceName).filter(Boolean),
  ])];

  const columns: ColumnsType<TRegistry> = [
    { title: 'Type', dataIndex: 'source_type', width: 80, render: (t: string) => <Tag color={t === 'mongodb' ? 'green' : 'blue'}>{t}</Tag> },
    { title: 'Source DB', dataIndex: 'source_db', width: 120 },
    { title: 'Source Table', dataIndex: 'source_table', width: 180, render: (t) => <strong style={{color: '#1890ff'}}>{t}</strong> },
    { title: 'Target Table', dataIndex: 'target_table', width: 180 },
    { title: 'Destination', dataIndex: 'airbyte_destination_name', width: 120, render: (v: string) => v ? <Tag color="blue">{v}</Tag> : '-' },
    { title: 'Connection ID', dataIndex: 'airbyte_connection_id', width: 150, render: (v) => v ? <code style={{ fontSize: '11px' }}>{v.substring(0, 8)}...</code> : '-' },
    {
      title: 'Sync Engine', dataIndex: 'sync_engine', width: 120,
      render: (v: string, record) => (
        <Space direction="vertical" size={0} onClick={e => e.stopPropagation()}>
          <Select value={v} size="small" style={{ width: 110 }}
            onChange={(val) => updateEntry(record.id, { sync_engine: val })}>
            <Select.Option value="airbyte">Airbyte</Select.Option>
            <Select.Option value="debezium">Debezium</Select.Option>
            <Select.Option value="both">Both</Select.Option>
          </Select>
          <SyncStatusIndicator id={record.id} engine={v} />
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
            <Tooltip title="Đồng bộ dữ liệu từ Airbyte sang table đích (SQL)">
              <Button size="small" icon={<SwapOutlined />} loading={actionLoadingId === record.id}
                onClick={(e) => handleBridge(e, record.id)}>Đồng bộ</Button>
            </Tooltip>
            <Tooltip title="Đồng bộ hiệu suất cao (Go + Sonyflake ID, cho data lớn >100K)">
              <Button size="small" icon={<ThunderboltOutlined />} loading={actionLoadingId === record.id}
                onClick={(e) => handleBridge(e, record.id, true)} type="primary" ghost>Batch</Button>
            </Tooltip>
            <Tooltip title="Chuyển _raw_data sang các cột đã mapping">
              <Button size="small" loading={actionLoadingId === record.id}
                onClick={(e) => handleTransform(e, record.id)}>Chuyển đổi</Button>
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
          initialValues={{ sync_engine: 'airbyte', priority: 'normal', primary_key_field: '_id', primary_key_type: 'VARCHAR(24)', source_type: 'mongodb' }}>
          <Form.Item name="source_db" label="Source DB" rules={[{ required: true }]}>
            <Select showSearch placeholder="Select source database"
              onChange={(val) => {
                const source = airbyteSources.find(s => s.database === val);
                if (source) {
                  let type = 'postgres';
                  const sourceName = source.sourceName.toLowerCase();
                  if (sourceName.includes('mongodb')) type = 'mongodb';
                  else if (sourceName.includes('mysql')) type = 'mysql';

                  form.setFieldsValue({ source_type: type });
                }
              }}>
              {airbyteSources.map(s => (
                <Select.Option key={s.sourceId} value={s.database}>
                   {s.database} (Type: {s.sourceName})
                </Select.Option>
              ))}
            </Select>
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
          <Form.Item name="sync_engine" label="Sync Engine">
            <Select><Select.Option value="airbyte">Airbyte</Select.Option><Select.Option value="debezium">Debezium</Select.Option><Select.Option value="both">Both</Select.Option></Select>
          </Form.Item>
          <Form.Item name="priority" label="Priority">
            <Select><Select.Option value="critical">Critical</Select.Option><Select.Option value="high">High</Select.Option><Select.Option value="normal">Normal</Select.Option><Select.Option value="low">Low</Select.Option></Select>
          </Form.Item>
          <Form.Item name="primary_key_field" label="PK Field"><Input /></Form.Item>
          <Form.Item name="primary_key_type" label="PK Type"><Input /></Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
