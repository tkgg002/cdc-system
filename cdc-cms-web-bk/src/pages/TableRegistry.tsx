import { useEffect, useState, useCallback, useMemo } from 'react';
import { Table, Tag, Select, Switch, Button, Space, Modal, Form, Input, message, Upload, Badge, Collapse, Typography, Progress, Tooltip, Tabs } from 'antd';
import { PlusOutlined, UploadOutlined, SyncOutlined, DatabaseOutlined, SearchOutlined, ToolOutlined, ThunderboltOutlined, RocketOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useNavigate } from 'react-router-dom';
import { cmsApi } from '../services/api';
import type { SourceObjectRow as TRegistry, ShadowBindingRow, ProvisioningMode, ProvisioningState } from '../types';
import { useScanFields } from '../hooks/useRegistry';
import { useProvisioningMode } from '../hooks/useProvisioningMode';
import DispatchStatusBadge from '../components/DispatchStatusBadge';
import ConfirmDestructiveModal from '../components/ConfirmDestructiveModal';

const { Panel } = Collapse;
const { Title, Text } = Typography;

const ENGINE_COLOR: Record<string, string> = {
  postgresql: 'blue',
  mongodb: 'green',
  mysql: 'orange',
  mariadb: 'orange',
};

const STATE_COLOR: Record<ProvisioningState, string> = {
  draft: 'default',
  shadow_pending: 'processing',
  shadow_active: 'cyan',
  master_pending: 'processing',
  master_active: 'cyan',
  mapping_pending: 'processing',
  mapping_ready: 'cyan',
  schedule_pending: 'processing',
  running: 'green',
  paused: 'gold',
  failed: 'red',
  archived: 'default',
};

const STATE_NEEDS_CONFIRM = (s?: ProvisioningState) =>
  !!s && (s.endsWith('_pending') || s === 'failed');

function renderMetadataStatus(record: TRegistry) {
  const status = record.metadata_status || 'v2_source_only';
  if (status === 'v2_ready') {
    return <Tag color="green">V2 Ready</Tag>;
  }
  if (status === 'v2_shadow_only') {
    return <Tag color="blue">Shadow Bound</Tag>;
  }
  return <Tag color="gold">Source Only</Tag>;
}

function renderBridgeStatus(record: TRegistry) {
  return record.bridge_status === 'bridged'
    ? <Tag color="cyan">Bridge OK</Tag>
    : <Tag color="default">No Bridge</Tag>;
}

function normalizeShadowSchema(sourceDB: string) {
  const normalized = sourceDB
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '');
  return `shadow_${normalized || 'unknown'}`;
}

function getShadowFqn(record: Pick<TRegistry, 'source_db' | 'target_table' | 'shadow_schema' | 'physical_table_fqn'>) {
  if (record.physical_table_fqn) return record.physical_table_fqn;
  const schema = record.shadow_schema || normalizeShadowSchema(record.source_db);
  return `${schema}.${record.target_table}`;
}

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

const TransformProgress = ({ registryId, sourceObjectId }: { registryId?: number | null; sourceObjectId?: number | null }) => {
  const [status, setStatus] = useState<{ total_rows: number; transformed_rows: number; pending_rows: number } | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    const canUseV2 = sourceObjectId != null && sourceObjectId > 0;
    const canUseBridge = registryId != null && registryId > 0;
    if (!canUseV2 && !canUseBridge) {
      setStatus(null);
      setError(false);
      return;
    }
    const url = canUseV2
      ? `/api/v1/source-objects/${sourceObjectId}/transform-status`
      : `/api/v1/source-objects/registry/${registryId}/transform-status`;
    cmsApi.get(url)
      .then(({ data }) => { setStatus(data); setError(false); })
      .catch(() => { setStatus(null); setError(true); });
  }, [registryId, sourceObjectId]);

  if (!registryId && !sourceObjectId) return <Tag color="default">No Scope</Tag>;

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
  const registryId = record.registry_id ?? null;
  const sourceObjectId = record.id ?? null;
  const scan = useScanFields(sourceObjectId, registryId, record.target_table);

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
    }
  };

  const scanBusy = scan.isPending;
  const canUseScan = Boolean(record.id || record.registry_id);

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
            disabled={!canUseScan}
            loading={scanBusy}
            onClick={(e) => { e.stopPropagation(); openConfirm('scan-fields'); }}
          >
            Quét field
          </Button>
        </Tooltip>
      </Space>
      <Space wrap>
        {!canUseScan && <Tag color="gold">Thiếu scope để quét field</Tag>}
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

// Systematic Flow F-1.2/1.3 — Source dropdown shape returned by
// GET /api/v1/sources. Keep in sync with source registry metadata exposed by CMS.
interface SourceRow {
  id: number;
  connector_name: string;
  source_type: string;
  database_include_list?: string;
  collection_include_list?: string;
}

export default function TableRegistry() {
  const navigate = useNavigate();
  const [data, setData] = useState<TRegistry[]>([]);
  const [shadowBindings, setShadowBindings] = useState<ShadowBindingRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [sourceDBFilter, setSourceDBFilter] = useState<string>('');
  const [engineFilter, setEngineFilter] = useState<string>('');
  const [modeLoadingId, setModeLoadingId] = useState<number | null>(null);
  const setModeMutation = useProvisioningMode();
  const [registerVisible, setRegisterVisible] = useState(false);
  const [actionLoadingId, setActionLoadingId] = useState<number | null>(null);
  const [activeLoadingId, setActiveLoadingId] = useState<number | null>(null);
  const [form] = Form.useForm();

  const [sources, setSources] = useState<SourceRow[]>([]);
  const [selectedSourceId, setSelectedSourceId] = useState<number | null>(null);

  const filteredByEngine = useMemo(() => {
    if (!engineFilter) return data;
    return data.filter(d => {
      const engine = d.source_engine_type || d.source_type;
      return engine === engineFilter;
    });
  }, [data, engineFilter]);

  const groupedData = useMemo(() => {
    const groups: Record<string, TRegistry[]> = {};
    filteredByEngine.forEach(item => {
      const db = item.source_db || 'unknown';
      if (!groups[db]) groups[db] = [];
      groups[db].push(item);
    });
    return groups;
  }, [filteredByEngine]);

  const groupedBindings = useMemo(() => {
    const groups: Record<string, ShadowBindingRow[]> = {};
    shadowBindings.forEach(item => {
      const db = item.source_db || 'unknown';
      if (!groups[db]) groups[db] = [];
      groups[db].push(item);
    });
    return groups;
  }, [shadowBindings]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, unknown> = { page, page_size: 100 };
      if (sourceDBFilter) params.source_db = sourceDBFilter;
      const { data: res } = await cmsApi.get('/api/v1/source-objects', { params });
      setData(res.data || []);
    } catch { /* interceptor */ }
    finally { setLoading(false); }
  }, [page, sourceDBFilter]);

  const fetchShadowBindings = useCallback(async () => {
    try {
      const params: Record<string, unknown> = { page, page_size: 100 };
      if (sourceDBFilter) params.source_db = sourceDBFilter;
      const { data: res } = await cmsApi.get('/api/v1/shadow-bindings', { params });
      setShadowBindings(res.data || []);
    } catch {
      setShadowBindings([]);
    }
  }, [page, sourceDBFilter]);

  useEffect(() => {
    fetchData();
    fetchShadowBindings();
  }, [fetchData, fetchShadowBindings]);

  useEffect(() => {
    if (!registerVisible) return;
    cmsApi.get('/api/v1/sources')
      .then(({ data: res }) => setSources(res.data || []))
      .catch(() => setSources([]));
  }, [registerVisible]);

  const selectedSource = useMemo(
    () => sources.find((s) => s.id === selectedSourceId) || null,
    [sources, selectedSourceId],
  );
  const collectionOptions = useMemo(() => {
    if (!selectedSource?.collection_include_list) return [] as { label: string; value: string }[];
    return selectedSource.collection_include_list
      .split(',')
      .map((c) => c.trim())
      .filter(Boolean)
      .map((c) => {
        // Debezium collection_include_list format is "<db>.<collection>".
        const parts = c.split('.');
        const collection = parts.length > 1 ? parts.slice(1).join('.') : c;
        return { label: c, value: collection };
      });
  }, [selectedSource]);

  const updateEntry = async (record: TRegistry, updates: Record<string, unknown>) => {
    const registryId = record.registry_id;
    const sourceObjectId = record.id;
    const usesLegacyBridge = Boolean(registryId);
    const updatePriorityOnly = 'priority' in updates || 'sync_interval' in updates;

    if (!usesLegacyBridge && updatePriorityOnly) {
      message.warning('Priority hiện vẫn cần bridge cũ; row này mới update trực tiếp được các field V2 như active hoặc timestamp');
      return;
    }

    if ('is_active' in updates) setActiveLoadingId(record.id);
    try {
      if (usesLegacyBridge) {
        await cmsApi.patch(`/api/v1/source-objects/registry/${registryId}`, updates);
      } else {
        await cmsApi.patch(`/api/v1/source-objects/${sourceObjectId}`, updates);
      }
      message.success('Cập nhật thành công');
      fetchData();
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Cập nhật thất bại');
    } finally {
      setActiveLoadingId(null);
    }
  };

  const performModeFlip = async (id: number, nextMode: ProvisioningMode, sourceObjectName: string) => {
    setModeLoadingId(id);
    try {
      await setModeMutation.mutateAsync({
        id,
        mode: nextMode,
        reason: `flip provisioning_mode → ${nextMode} for ${sourceObjectName}`,
      });
      message.success(`Mode set to ${nextMode}`);
      fetchData();
    } catch (err) {
      const e = err as { response?: { status?: number; data?: { error?: string } } };
      const status = e.response?.status;
      const detail = e.response?.data?.error || 'Unknown error';
      if (status === 409) {
        message.warning('CAS conflict — orchestrator advanced ahead of UI. Refresh to see latest state.');
        fetchData();
      } else if (status === 422) {
        message.error(`Invalid transition: ${detail}`);
      } else {
        message.error(detail);
      }
    } finally {
      setModeLoadingId(null);
    }
  };

  const handleToggleMode = (record: TRegistry, checked: boolean) => {
    const nextMode: ProvisioningMode = checked ? 'auto' : 'manual';
    const currentState = record.provisioning_state;
    if (nextMode === 'manual' && STATE_NEEDS_CONFIRM(currentState)) {
      Modal.confirm({
        title: `Switch to Manual while state = ${currentState}?`,
        content: 'Có command đang chạy. Switch sang Manual sẽ giữ state hiện tại và KHÔNG cancel cmd in-flight; orchestrator sẽ ngừng tự advance bước kế tiếp.',
        okText: 'Switch to Manual',
        okButtonProps: { danger: true },
        cancelText: 'Cancel',
        onOk: () => performModeFlip(record.id, nextMode, record.source_table || record.object_code),
      });
      return;
    }
    performModeFlip(record.id, nextMode, record.source_table || record.object_code);
  };

  const handleRegister = async (values: Record<string, unknown>) => {
    try {
      await cmsApi.post('/api/v1/source-objects/register', values);
      message.success('Source object registered');
      setRegisterVisible(false);
      form.resetFields();
      setSelectedSourceId(null);
      fetchData();
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Register failed');
    }
  };

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

  const handleCreateTable = (e: React.MouseEvent, record: TRegistry) => {
    e.stopPropagation();
    setActionLoadingId(record.id);
    const endpoint = record.registry_id
      ? `/api/v1/source-objects/registry/${record.registry_id}/create-default-columns`
      : `/api/v1/source-objects/${record.id}/create-default-columns`;
    cmsApi.post(endpoint)
      .then(() => { message.success('Đang tạo table đích + field mặc định...'); fetchData(); })
      .catch((err) => {
        const e = err as { response?: { data?: { error?: string } } };
        message.error(e.response?.data?.error || 'Tạo table thất bại');
      })
      .finally(() => setActionLoadingId(null));
  };

  const handleCreateDefaultFields = (e: React.MouseEvent, record: TRegistry) => {
    e.stopPropagation();
    setActionLoadingId(record.id);
    const endpoint = record.registry_id
      ? `/api/v1/source-objects/registry/${record.registry_id}/standardize`
      : `/api/v1/source-objects/${record.id}/standardize`;
    cmsApi.post(endpoint)
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
      await cmsApi.post('/api/v1/source-objects/register-batch', entries);
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
  const uniqueBindingSourceDBs = [...new Set(shadowBindings.map(d => d.source_db))];
  const allSourceDBs = [...new Set([...uniqueSourceDBs, ...uniqueBindingSourceDBs])];

  const columns: ColumnsType<TRegistry> = [
    {
      title: 'Engine',
      key: 'engine',
      width: 100,
      render: (_, record) => {
        const engine = record.source_engine_type || record.source_type;
        return <Tag color={ENGINE_COLOR[engine] || 'default'}>{engine}</Tag>;
      },
    },
    {
      title: 'Mode',
      key: 'provisioning_mode',
      width: 110,
      render: (_, record) => {
        const mode = record.provisioning_mode;
        if (!mode) return <Tag color="default">—</Tag>;
        return (
          <Tooltip title="Auto: orchestrator tự advance state. Manual: operator click /advance.">
            <div onClick={(e) => e.stopPropagation()}>
              <Switch
                size="small"
                checked={mode === 'auto'}
                checkedChildren="Auto"
                unCheckedChildren="Manual"
                loading={modeLoadingId === record.id}
                onChange={(checked) => handleToggleMode(record, checked)}
              />
            </div>
          </Tooltip>
        );
      },
    },
    {
      title: 'State',
      key: 'provisioning_state',
      width: 140,
      render: (_, record) => {
        const state = record.provisioning_state;
        if (!state) return <Tag color="default">—</Tag>;
        return <Tag color={STATE_COLOR[state] || 'default'}>{state}</Tag>;
      },
    },
    { title: 'Source DB', dataIndex: 'source_db', width: 120 },
    { title: 'Source Table', dataIndex: 'source_table', width: 180, render: (t) => <strong style={{ color: '#1890ff' }}>{t}</strong> },
    { title: 'Shadow Table', dataIndex: 'target_table', width: 180 },
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
          disabled={!record.registry_id}
          onChange={(val) => updateEntry(record, { priority: val })}>
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
            onChange={(checked) => updateEntry(record, { is_active: checked })} />
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
    {
      title: 'Metadata',
      key: 'metadata_status',
      width: 180,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          {renderMetadataStatus(record)}
          {renderBridgeStatus(record)}
        </Space>
      ),
    },
    {
      title: 'Shadow Target',
      key: 'shadow_target',
      width: 240,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Text code>{getShadowFqn(record)}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>
            schema={record.shadow_schema || normalizeShadowSchema(record.source_db)}
          </Text>
        </Space>
      ),
    },
    { title: 'Transform', width: 130, render: (_, record) => <TransformProgress registryId={record.registry_id} sourceObjectId={record.id} /> },
    { title: 'Created At', dataIndex: 'created_at', width: 140, render: (v) => new Date(v).toLocaleString() },
    {
      title: 'Thao tác', width: 520, fixed: 'right',
      render: (_, record) => (
        <Space direction="vertical" size={4} onClick={e => e.stopPropagation()}>
          <Space wrap>
            {!record.is_table_created ? (
              <Tooltip title="Tạo table đích + thêm tất cả field đã duyệt">
                <Button size="small" type="primary" icon={<DatabaseOutlined />} loading={actionLoadingId === record.id}
                  onClick={(e) => handleCreateTable(e, record)}>Tạo Table</Button>
              </Tooltip>
            ) : (
              <Tooltip title="Thêm System Default Fields vào table đã có">
                <Button size="small" icon={<ToolOutlined />} loading={actionLoadingId === record.id}
                  onClick={(e) => handleCreateDefaultFields(e, record)}>Tạo Field MĐ</Button>
              </Tooltip>
            )}
            <Tooltip title="Trigger Debezium incremental snapshot cho collection này">
              <Button size="small" icon={<ThunderboltOutlined />} type="primary" ghost
                loading={actionLoadingId === record.id}
                onClick={(e) => handleSnapshot(e, record)}>Snapshot Now</Button>
            </Tooltip>
            <Tooltip title="Đi tới Master Registry để tạo / chạy Transmute">
              <Button size="small" icon={<RocketOutlined />}
                onClick={(e) => {
                  e.stopPropagation();
                  const params = new URLSearchParams({
                    source_shadow: record.target_table,
                    source_label: getShadowFqn(record),
                    source_db: record.source_db,
                    source_table: record.source_table,
                    shadow_schema: record.shadow_schema || normalizeShadowSchema(record.source_db),
                    shadow_table: record.target_table,
                  });
                  navigate(`/masters?${params.toString()}`);
                }}>
                Manage Masters
              </Button>
            </Tooltip>
          </Space>
          <AsyncRowActions record={record} onChange={fetchData} />
        </Space>
      ),
    },
  ];

  const bindingColumns: ColumnsType<ShadowBindingRow> = [
    { title: 'Source DB', dataIndex: 'source_db', width: 120 },
    { title: 'Source Table', dataIndex: 'source_table', width: 180, render: (t) => <strong style={{ color: '#1890ff' }}>{t}</strong> },
    { title: 'Binding Code', dataIndex: 'binding_code', width: 220, render: (t) => <Text code>{t}</Text> },
    { title: 'Shadow Schema', dataIndex: 'shadow_schema', width: 160, render: (t) => <Text code>{t}</Text> },
    { title: 'Shadow Table', dataIndex: 'shadow_table', width: 180 },
    { title: 'Physical Target', dataIndex: 'physical_table_fqn', width: 260, render: (t) => <Text code>{t}</Text> },
    { title: 'Write Mode', dataIndex: 'write_mode', width: 110, render: (v) => <Tag color="blue">{v}</Tag> },
    { title: 'DDL', dataIndex: 'ddl_status', width: 110, render: (v) => <Tag color={v === 'created' ? 'green' : v === 'failed' ? 'red' : v === 'drifted' ? 'orange' : 'default'}>{v}</Tag> },
    { title: 'Recon Drift', dataIndex: 'recon_drift', width: 110, render: (v: number) => v ? <Tag color="orange">{v}</Tag> : <Tag color="green">0</Tag> },
    { title: 'Active', dataIndex: 'is_active', width: 90, render: (v: boolean) => <Tag color={v ? 'green' : 'default'}>{v ? 'active' : 'inactive'}</Tag> },
    { title: 'Last Recon', dataIndex: 'last_recon_at', width: 160, render: (v?: string | null) => v ? new Date(v).toLocaleString() : <Text type="secondary">-</Text> },
  ];

  return (
    <div>
      <Title level={4} style={{ marginBottom: 24, textAlign: 'left' }}>Source Objects</Title>
      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        Mỗi row đại diện cho 1 source object đã được route sang 1 shadow target. Ở phase hiện tại, shadow namespace hiển thị theo quy ước <Text code>shadow_{"<source_db>"}</Text>.
      </Text>
      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        Danh sách này đã đọc từ metadata V2 ở <Text code>cdc_system</Text>. Badge <Text code>V2 Ready</Text> nghĩa là row đã có đủ source object + shadow binding + bridge compatibility. Những row chưa có <Text code>Bridge OK</Text> vẫn monitor được bình thường, nhưng vài action cũ sẽ còn ở chế độ read-only.
      </Text>

      <Space style={{ marginBottom: 16 }}>
        <Select placeholder="Filter Source DB" allowClear style={{ width: 180 }}
          value={sourceDBFilter || undefined} onChange={(v) => { setSourceDBFilter(v || ''); setPage(1); }}>
          {allSourceDBs.map(db => <Select.Option key={db} value={db}>{db}</Select.Option>)}
        </Select>
        <Select placeholder="Filter Engine" allowClear style={{ width: 160 }}
          value={engineFilter || undefined} onChange={(v) => setEngineFilter(v || '')}>
          <Select.Option value="postgresql">PostgreSQL</Select.Option>
          <Select.Option value="mongodb">MongoDB</Select.Option>
          <Select.Option value="mysql">MySQL</Select.Option>
          <Select.Option value="mariadb">MariaDB</Select.Option>
        </Select>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setRegisterVisible(true)}>
          Register Source Object
        </Button>
        <Upload accept=".json" showUploadList={false} beforeUpload={handleBulkImport as unknown as (f: File) => boolean}>
          <Button icon={<UploadOutlined />}>Bulk Import Source Objects</Button>
        </Upload>
        <Button onClick={fetchData}>Refresh</Button>
      </Space>
      <Tabs
        defaultActiveKey="source-objects"
        items={[
          {
            key: 'source-objects',
            label: 'Source Objects',
            children: (
              <Collapse defaultActiveKey={Object.keys(groupedData)} ghost expandIconPosition="end">
                {Object.entries(groupedData).map(([db, tables]) => (
                  <Panel header={
                    <Space>
                      <DatabaseOutlined style={{ color: '#1890ff' }} />
                      <span style={{ fontWeight: 600 }}>Source Database: {db}</span>
                      <Tag color="blue">{tables.length} objects</Tag>
                    </Space>
                  } key={db} style={{ marginBottom: 16, border: '1px solid #f0f0f0', borderRadius: 8, background: '#fafafa' }}>
                    <Table
                      columns={columns}
                      dataSource={tables}
                      rowKey="object_code"
                      loading={loading}
                      size="small"
                      pagination={false}
                      scroll={{ x: 1000 }}
                      onRow={(record) => ({
                        onClick: () => {
                          if (record.registry_id) navigate(`/registry/${record.registry_id}/mappings`);
                        },
                        style: { cursor: record.registry_id ? 'pointer' : 'default' }
                      })}
                    />
                  </Panel>
                ))}
              </Collapse>
            ),
          },
          {
            key: 'shadow-bindings',
            label: 'Shadow Bindings',
            children: (
              <>
                <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
                  Tab này cho operator nhìn trực tiếp binding layer của shadow: source object nào đang route vào schema/table nào, DDL đang ở trạng thái gì, và drift gần nhất ra sao.
                </Text>
                <Collapse defaultActiveKey={Object.keys(groupedBindings)} ghost expandIconPosition="end">
                  {Object.entries(groupedBindings).map(([db, bindings]) => (
                    <Panel header={
                      <Space>
                        <DatabaseOutlined style={{ color: '#1890ff' }} />
                        <span style={{ fontWeight: 600 }}>Source Database: {db}</span>
                        <Tag color="cyan">{bindings.length} bindings</Tag>
                      </Space>
                    } key={db} style={{ marginBottom: 16, border: '1px solid #f0f0f0', borderRadius: 8, background: '#fafafa' }}>
                      <Table
                        columns={bindingColumns}
                        dataSource={bindings}
                        rowKey="binding_code"
                        loading={loading}
                        size="small"
                        pagination={false}
                        scroll={{ x: 1200 }}
                      />
                    </Panel>
                  ))}
                </Collapse>
              </>
            ),
          },
        ]}
      />

      {/* Register Modal */}
      <Modal title="Register New Source Object" open={registerVisible} onOk={() => form.submit()}
        onCancel={() => { setRegisterVisible(false); setSelectedSourceId(null); }} width={500}>
        <Form form={form} layout="vertical" onFinish={handleRegister}
          initialValues={{ sync_engine: 'debezium', priority: 'normal', primary_key_field: '_id', primary_key_type: 'VARCHAR(24)' }}>
          <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
            Luồng hiện tại sẽ ghi bridge compatibility và đồng thời sync ngay metadata V2. Sau khi tạo xong, row mới nên hiện trạng thái <Text code>V2 Ready</Text>.
          </Text>
          <Form.Item label="Source (Connector)" required tooltip="Pick a registered connector — source_db + source_type auto-fill.">
            <Select
              placeholder="Select a source"
              value={selectedSourceId ?? undefined}
              onChange={(id: number) => {
                setSelectedSourceId(id);
                const src = sources.find((s) => s.id === id);
                if (src) {
                  form.setFieldsValue({
                    source_db: src.database_include_list || '',
                    source_type: src.source_type,
                    source_table: undefined,
                  });
                }
              }}
              options={sources.map((s) => ({
                label: `${s.connector_name} (${s.source_type})`,
                value: s.id,
              }))}
              notFoundContent="No sources registered yet — create a connector first."
            />
          </Form.Item>
          <Form.Item name="source_db" label="Source DB" rules={[{ required: true }]}>
            <Input disabled placeholder="auto-filled from source" />
          </Form.Item>
          <Form.Item name="source_type" label="Source Type" rules={[{ required: true }]}>
            <Input disabled placeholder="auto-filled from source" />
          </Form.Item>
          <Form.Item name="source_table" label="Source Collection" rules={[{ required: true }]}>
            {collectionOptions.length > 0 ? (
              <Select placeholder="Pick collection from connector" options={collectionOptions} />
            ) : (
              <Input placeholder="Select a source above, or type manually" />
            )}
          </Form.Item>
          <Form.Item
            name="target_table"
            label="Shadow Table Name"
            tooltip="Tên physical table trong shadow schema. Shadow schema sẽ được derive theo source_db."
            rules={[{ required: true }]}
          >
            <Input placeholder="wallet_transactions" />
          </Form.Item>
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
