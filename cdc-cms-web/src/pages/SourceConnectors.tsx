import { useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Modal,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  Tag,
  Typography,
  message,
} from 'antd';
import {
  DatabaseOutlined,
  DeleteOutlined,
  EditOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  ReloadOutlined,
  SyncOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

type DbKind = 'mongodb' | 'mysql' | 'postgresql';
type MutationOp = 'restart' | 'pause' | 'resume' | 'restartTask';
type EditorMode = 'create' | 'edit';

interface ConnectorTask {
  id: number;
  state: string;
  worker_id?: string;
  trace?: string;
}

interface ConnectorView {
  name: string;
  state: string;
  type: string;
  connector_class: string;
  tasks: ConnectorTask[];
  config?: Record<string, string>;
}

interface SourceFingerprint {
  id: number;
  connector_name: string;
  source_type: string;
  connector_class: string;
  topic_prefix?: string | null;
  server_address?: string | null;
  database_include_list?: string | null;
  collection_include_list?: string | null;
  raw_config_sanitized?: Record<string, unknown> | null;
  status: string;
  created_by?: string | null;
  created_at: string;
  updated_at: string;
}

interface PendingAction {
  op: MutationOp;
  connector: string;
  taskId?: number;
}

interface ConnectionFormValues {
  dbKind: DbKind;
  connectorName: string;
  topicPrefix: string;
  host: string;
  port: number;
  database: string;
  username?: string;
  password?: string;
  replicaSet?: string;
  collectionNames?: string;
  tableIncludeList?: string;
  schemaIncludeList?: string;
  serverId?: number;
  slotName?: string;
  publicationName?: string;
  reason: string;
}

const KEEP_SECRET_SENTINEL = '__KEEP__';

const STATE_COLOR: Record<string, string> = {
  RUNNING: 'green',
  PAUSED: 'orange',
  FAILED: 'red',
  UNASSIGNED: 'default',
  DESTROYED: 'black',
};

const DB_OPTIONS = [
  { label: 'MongoDB', value: 'mongodb' },
  { label: 'MySQL', value: 'mysql' },
  { label: 'PostgreSQL', value: 'postgresql' },
] as const;

function compactConfig(cfg: Record<string, string>) {
  return Object.fromEntries(Object.entries(cfg).filter(([, value]) => value !== ''));
}

function normalizeCsv(input?: string) {
  return (input || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
    .join(',');
}

function maskAddress(address?: string | null) {
  if (!address) return '-';
  return address.replace(/\/\/([^:@/]+):([^@/]+)@/, '//***:***@');
}

function detectDbKind(connectorClass?: string, sourceType?: string): DbKind {
  if ((sourceType || '').toLowerCase() === 'mongodb') return 'mongodb';
  if ((sourceType || '').toLowerCase() === 'mysql') return 'mysql';
  if ((sourceType || '').toLowerCase() === 'postgresql') return 'postgresql';
  if ((connectorClass || '').includes('MongoDb')) return 'mongodb';
  if ((connectorClass || '').includes('MySql')) return 'mysql';
  return 'postgresql';
}

function buildMongoConnectionString(values: ConnectionFormValues, fallbackPassword?: string) {
  const auth =
    values.username
      ? `${encodeURIComponent(values.username)}:${encodeURIComponent(values.password || fallbackPassword || '')}@`
      : '';
  const replica = values.replicaSet ? `?replicaSet=${encodeURIComponent(values.replicaSet)}` : '';
  return `mongodb://${auth}${values.host}:${values.port}/${replica}`;
}

function buildConnectorConfig(values: ConnectionFormValues, mode: EditorMode, fallbackPassword?: string) {
  const topicPrefix = values.topicPrefix.trim() || `cdc.${values.database}`;
  if (values.dbKind === 'mongodb') {
    const collections = normalizeCsv(values.collectionNames);
    const collectionIncludeList = collections
      ? collections
          .split(',')
          .map((item) => `${values.database}.${item.trim()}`)
          .join(',')
      : '';
    return compactConfig({
      'connector.class': 'io.debezium.connector.mongodb.MongoDbConnector',
      'mongodb.connection.string': buildMongoConnectionString(values, fallbackPassword),
      'database.include.list': values.database,
      'collection.include.list': collectionIncludeList,
      'topic.prefix': topicPrefix,
      'signal.data.collection': `${values.database}.debezium_signal`,
      'capture.mode': 'change_streams_update_full_with_pre_image',
      'snapshot.mode': 'initial',
      'key.converter': 'io.confluent.connect.avro.AvroConverter',
      'key.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
      'value.converter': 'io.confluent.connect.avro.AvroConverter',
      'value.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
      'schema.history.internal.kafka.bootstrap.servers': 'gpay-kafka:9092',
    });
  }

  if (values.dbKind === 'mysql') {
    return compactConfig({
      'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
      'database.hostname': values.host,
      'database.port': String(values.port),
      'database.user': values.username || '',
      'database.password': values.password || (mode === 'edit' ? KEEP_SECRET_SENTINEL : ''),
      'database.include.list': values.database,
      'table.include.list': normalizeCsv(values.tableIncludeList),
      'topic.prefix': topicPrefix,
      'database.server.id': String(values.serverId || 5401),
      'snapshot.mode': 'initial',
      'schema.history.internal.kafka.bootstrap.servers': 'gpay-kafka:9092',
      'schema.history.internal.kafka.topic': `schemahistory.${values.connectorName}`,
      'key.converter': 'io.confluent.connect.avro.AvroConverter',
      'key.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
      'value.converter': 'io.confluent.connect.avro.AvroConverter',
      'value.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
    });
  }

  return compactConfig({
    'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
    'database.hostname': values.host,
    'database.port': String(values.port),
    'database.user': values.username || '',
    'database.password': values.password || (mode === 'edit' ? KEEP_SECRET_SENTINEL : ''),
    'database.dbname': values.database,
    'schema.include.list': normalizeCsv(values.schemaIncludeList) || 'public',
    'table.include.list': normalizeCsv(values.tableIncludeList),
    'topic.prefix': topicPrefix,
    'plugin.name': 'pgoutput',
    'slot.name': values.slotName || `${values.connectorName}_slot`,
    'publication.name': values.publicationName || `${values.connectorName}_pub`,
    'publication.autocreate.mode': 'filtered',
    'snapshot.mode': 'initial',
    'schema.history.internal.kafka.bootstrap.servers': 'gpay-kafka:9092',
    'schema.history.internal.kafka.topic': `schemahistory.${values.connectorName}`,
    'key.converter': 'io.confluent.connect.avro.AvroConverter',
    'key.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
    'value.converter': 'io.confluent.connect.avro.AvroConverter',
    'value.converter.schema.registry.url': 'http://gpay-schema-registry:8081',
  });
}

function parseConnectionSeed(source: SourceFingerprint, connector?: ConnectorView): Partial<ConnectionFormValues> {
  const dbKind = detectDbKind(connector?.connector_class, source.source_type) as DbKind;
  const cfg = connector?.config || {};

  if (dbKind === 'mongodb') {
    let host = 'gpay-mongo';
    let port = 27017;
    let username = '';
    let database = source.database_include_list || '';
    let replicaSet = '';
    try {
      if (source.server_address) {
        const url = new URL(source.server_address);
        host = url.hostname || host;
        port = Number(url.port || port);
        username = decodeURIComponent(url.username || '');
        replicaSet = url.searchParams.get('replicaSet') || '';
        database = database || url.pathname.replace(/^\//, '');
      }
    } catch {
      // ignore malformed URL
    }
    const collectionNames = (source.collection_include_list || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => item.includes('.') ? item.split('.').slice(1).join('.') : item)
      .join(',');
    return {
      dbKind,
      connectorName: source.connector_name,
      topicPrefix: source.topic_prefix || cfg['topic.prefix'] || `cdc.${database}`,
      host,
      port,
      database,
      username,
      password: '',
      replicaSet,
      collectionNames,
    };
  }

  return {
    dbKind,
    connectorName: source.connector_name,
    topicPrefix: source.topic_prefix || cfg['topic.prefix'] || '',
    host: cfg['database.hostname'] || source.server_address?.split(':')[0] || 'localhost',
    port: Number(cfg['database.port'] || source.server_address?.split(':')[1] || (dbKind === 'mysql' ? 3306 : 5432)),
    database: cfg['database.include.list'] || cfg['database.dbname'] || source.database_include_list || '',
    username: cfg['database.user'] || '',
    password: '',
    tableIncludeList: cfg['table.include.list'] || source.collection_include_list || '',
    schemaIncludeList: cfg['schema.include.list'] || 'public',
    serverId: Number(cfg['database.server.id'] || 5401),
    slotName: cfg['slot.name'] || '',
    publicationName: cfg['publication.name'] || '',
  };
}

export default function SourceConnectors() {
  const qc = useQueryClient();
  const [actionReason, setActionReason] = useState('');
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [deletePending, setDeletePending] = useState<string | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorMode, setEditorMode] = useState<EditorMode>('create');
  const [editingSource, setEditingSource] = useState<SourceFingerprint | null>(null);
  const [form] = Form.useForm<ConnectionFormValues>();
  const dbKind = Form.useWatch('dbKind', form) || 'mongodb';

  const { data: connectorsData, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['debezium-connectors'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: ConnectorView[]; count: number }>('/api/v1/system/connectors');
      // Defensive: backend may return tasks=null on a freshly-registered
      // connector that has not yet produced workers. Downstream
      // .tasks.filter/.length would crash the whole page.
      return (r.data.data ?? []).map((c) => ({ ...c, tasks: c.tasks ?? [] }));
    },
    refetchInterval: 15000,
  });

  const { data: sourcesData, isLoading: sourcesLoading, refetch: refetchSources, isFetching: isFetchingSources } = useQuery({
    queryKey: ['source-fingerprints'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: SourceFingerprint[]; count: number }>('/api/v1/sources');
      return r.data.data;
    },
    refetchInterval: 30000,
  });

  const connectors = connectorsData || [];
  const fingerprints = sourcesData || [];

  const connectorByName = useMemo(
    () => new Map(connectors.map((item) => [item.name, item])),
    [connectors],
  );

  const linkedFingerprints = fingerprints.filter((item) => connectorByName.has(item.connector_name));
  const orphanFingerprints = fingerprints.filter((item) => !connectorByName.has(item.connector_name));
  const connectorsWithoutFingerprint = connectors.filter((item) => !fingerprints.some((fp) => fp.connector_name === item.name));

  const failedTasksCount = connectors.reduce(
    (acc, c) => acc + c.tasks.filter((t) => t.state === 'FAILED').length,
    0,
  );

  const createMut = useMutation({
    mutationFn: async (values: ConnectionFormValues) => {
      const config = buildConnectorConfig(values, 'create');
      const r = await cmsApi.post(
        '/api/v1/system/connectors',
        { name: values.connectorName, config, reason: values.reason },
        { headers: { 'Idempotency-Key': `cc-create-${values.connectorName}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: () => {
      message.success('New connect created');
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      qc.invalidateQueries({ queryKey: ['source-fingerprints'] });
      setEditorOpen(false);
      form.resetFields();
      setEditingSource(null);
    },
    onError: (err: unknown) => {
      const e = err as { response?: { data?: { error?: string; detail?: string } } };
      message.error(e.response?.data?.detail || e.response?.data?.error || 'Create failed');
    },
  });

  const updateMut = useMutation({
    mutationFn: async (values: ConnectionFormValues) => {
      const config = buildConnectorConfig(values, 'edit');
      const r = await cmsApi.patch(
        `/api/v1/system/connectors/${encodeURIComponent(values.connectorName)}/config`,
        { config, reason: values.reason },
        { headers: { 'Idempotency-Key': `cc-update-${values.connectorName}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: () => {
      message.success('Connector config updated');
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      qc.invalidateQueries({ queryKey: ['source-fingerprints'] });
      setEditorOpen(false);
      form.resetFields();
      setEditingSource(null);
    },
    onError: (err: unknown) => {
      const e = err as { response?: { data?: { error?: string; detail?: string } } };
      message.error(e.response?.data?.detail || e.response?.data?.error || 'Update failed');
    },
  });

  const mutation = useMutation({
    mutationFn: async (p: PendingAction & { reason: string }) => {
      const path = `/api/v1/system/connectors/${encodeURIComponent(p.connector)}/${p.op === 'restartTask' ? `tasks/${p.taskId}/restart` : p.op}`;
      const r = await cmsApi.post(
        path,
        { reason: p.reason },
        { headers: { 'Idempotency-Key': `cc-${p.op}-${p.connector}-${p.taskId ?? ''}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`${vars.op} triggered: ${vars.connector}`);
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      setPending(null);
      setActionReason('');
    },
    onError: (err: unknown) => {
      const e = err as { response?: { data?: { error?: string; detail?: string } } };
      message.error(e.response?.data?.detail || e.response?.data?.error || 'Request failed');
    },
  });

  const deleteMut = useMutation({
    mutationFn: async (args: { name: string; reason: string }) => {
      const r = await cmsApi.delete(
        `/api/v1/system/connectors/${encodeURIComponent(args.name)}`,
        { data: { reason: args.reason }, headers: { 'Idempotency-Key': `cc-delete-${args.name}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_d, vars) => {
      message.success(`Deleted: ${vars.name}`);
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      qc.invalidateQueries({ queryKey: ['source-fingerprints'] });
      setDeletePending(null);
      setActionReason('');
    },
    onError: () => message.error('Delete failed'),
  });

  const openCreate = () => {
    setEditorMode('create');
    setEditingSource(null);
    form.setFieldsValue({
      dbKind: 'mongodb',
      connectorName: '',
      topicPrefix: '',
      host: 'gpay-mongo',
      port: 27017,
      database: '',
      username: '',
      password: '',
      replicaSet: 'rs0',
      collectionNames: '',
      tableIncludeList: '',
      schemaIncludeList: 'public',
      serverId: 5401,
      slotName: '',
      publicationName: '',
      reason: '',
    });
    setEditorOpen(true);
  };

  const openEdit = (source: SourceFingerprint) => {
    const connector = connectorByName.get(source.connector_name);
    const seed = parseConnectionSeed(source, connector);
    setEditorMode('edit');
    setEditingSource(source);
    form.setFieldsValue({
      dbKind: seed.dbKind || 'mongodb',
      connectorName: seed.connectorName || source.connector_name,
      topicPrefix: seed.topicPrefix || '',
      host: seed.host || 'gpay-mongo',
      port: seed.port || 27017,
      database: seed.database || '',
      username: seed.username || '',
      password: '',
      replicaSet: seed.replicaSet || 'rs0',
      collectionNames: seed.collectionNames || '',
      tableIncludeList: seed.tableIncludeList || '',
      schemaIncludeList: seed.schemaIncludeList || 'public',
      serverId: seed.serverId || 5401,
      slotName: seed.slotName || '',
      publicationName: seed.publicationName || '',
      reason: '',
    });
    setEditorOpen(true);
  };

  const submitEditor = async () => {
    try {
      const values = await form.validateFields();
      if (editorMode === 'create') {
        createMut.mutate(values);
      } else {
        updateMut.mutate(values);
      }
    } catch {
      // form validation already shown by antd
    }
  };

  const submitAction = () => {
    if (!pending) return;
    if (actionReason.trim().length < 10) {
      message.warning('Lý do phải ≥ 10 ký tự');
      return;
    }
    mutation.mutate({ ...pending, reason: actionReason.trim() });
  };

  const submitDelete = () => {
    if (!deletePending) return;
    if (actionReason.trim().length < 10) {
      message.warning('Lý do phải ≥ 10 ký tự');
      return;
    }
    deleteMut.mutate({ name: deletePending, reason: actionReason.trim() });
  };

  const taskColumnsBase = [
    { title: 'Task ID', dataIndex: 'id', width: 80 },
    {
      title: 'State',
      dataIndex: 'state',
      width: 120,
      render: (s: string) => <Tag color={STATE_COLOR[s] || 'default'}>{s}</Tag>,
    },
    { title: 'Worker', dataIndex: 'worker_id', render: (v: string) => <Text code>{v || '-'}</Text> },
    {
      title: 'Trace',
      dataIndex: 'trace',
      render: (t: string) => t ? <Text type="danger" style={{ fontSize: 12 }}>{t.slice(0, 160)}…</Text> : <Text type="secondary">-</Text>,
    },
  ];

  const expandedTasks = (row: ConnectorView) => (
    <div style={{ background: '#fafafa', padding: 12 }}>
      <Descriptions size="small" column={2} style={{ marginBottom: 12 }}>
        <Descriptions.Item label="Connector class">
          <Text code>{row.connector_class || '-'}</Text>
        </Descriptions.Item>
        <Descriptions.Item label="Type">
          <Tag color="blue">{row.type || '-'}</Tag>
        </Descriptions.Item>
        {row.config && Object.entries(row.config).slice(0, 6).map(([k, v]) => (
          <Descriptions.Item label={k} key={k}>
            <Text style={{ fontSize: 12 }}>{v}</Text>
          </Descriptions.Item>
        ))}
      </Descriptions>
      <Table
        size="small"
        rowKey="id"
        dataSource={row.tasks}
        columns={[
          ...taskColumnsBase,
          {
            title: 'Action',
            width: 130,
            render: (_: unknown, task: ConnectorTask) => (
              <Button
                size="small"
                icon={<SyncOutlined />}
                danger={task.state === 'FAILED'}
                onClick={() => setPending({ op: 'restartTask', connector: row.name, taskId: task.id })}
              >
                Restart task
              </Button>
            ),
          },
        ]}
        pagination={false}
      />
    </div>
  );

  const connectionColumns = [
    {
      title: 'Connection',
      key: 'connector_name',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Text strong>{row.connector_name}</Text>
          <Tag color="blue">{detectDbKind(undefined, row.source_type)}</Tag>
        </Space>
      ),
    },
    {
      title: 'Database / Include',
      key: 'source',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Text>{row.database_include_list || '-'}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{row.collection_include_list || '-'}</Text>
        </Space>
      ),
    },
    {
      title: 'URL / Host',
      key: 'infra',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Text code>{row.topic_prefix || '-'}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{maskAddress(row.server_address)}</Text>
        </Space>
      ),
    },
    {
      title: 'Link Status',
      key: 'status',
      width: 180,
      render: (_: unknown, row: SourceFingerprint) => {
        const live = connectorByName.get(row.connector_name);
        if (!live) {
          return (
            <Space direction="vertical" size={0}>
              <Tag color="orange">Fingerprint only</Tag>
              <Text type="secondary" style={{ fontSize: 12 }}>Không thấy connector runtime</Text>
            </Space>
          );
        }
        return (
          <Space direction="vertical" size={0}>
            <Tag color="green">Linked</Tag>
            <Tag color={STATE_COLOR[live.state] || 'default'}>{live.state || 'UNKNOWN'}</Tag>
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      width: 240,
      render: (_: unknown, row: SourceFingerprint) => {
        const live = connectorByName.get(row.connector_name);
        return (
          <Space>
            <Button
              size="small"
              icon={<EditOutlined />}
              type="primary"
              onClick={() => openEdit(row)}
              disabled={!live}
            >
              Edit Config
            </Button>
            <Button
              size="small"
              icon={<SyncOutlined />}
              onClick={() => live && setPending({ op: 'restart', connector: live.name })}
              disabled={!live}
            >
              Refresh Active
            </Button>
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => setDeletePending(row.connector_name)}
            >
              Delete
            </Button>
          </Space>
        );
      },
    },
  ];

  const fingerprintColumns = [
    {
      title: 'Connector / Fingerprint',
      key: 'connector_name',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Text strong>{row.connector_name}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{row.connector_class || '-'}</Text>
        </Space>
      ),
    },
    {
      title: 'Source',
      key: 'source',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Tag color="blue">{row.source_type}</Tag>
          <Text>{row.database_include_list || '-'}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{row.collection_include_list || '-'}</Text>
        </Space>
      ),
    },
    {
      title: 'Topic / Server',
      key: 'infra',
      render: (_: unknown, row: SourceFingerprint) => (
        <Space direction="vertical" size={0}>
          <Text code>{row.topic_prefix || '-'}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{maskAddress(row.server_address)}</Text>
        </Space>
      ),
    },
    {
      title: 'Updated',
      dataIndex: 'updated_at',
      width: 180,
      render: (v: string) => new Date(v).toLocaleString('vi-VN', { hour12: false }),
    },
  ];

  const connectorColumns = [
    {
      title: 'Connector',
      dataIndex: 'name',
      render: (v: string) => (
        <Space><DatabaseOutlined /><Text strong>{v}</Text></Space>
      ),
    },
    {
      title: 'State',
      dataIndex: 'state',
      width: 130,
      render: (s: string) => <Tag color={STATE_COLOR[s] || 'default'}>{s || 'UNKNOWN'}</Tag>,
    },
    {
      title: 'Tasks',
      render: (_: unknown, r: ConnectorView) => {
        const total = r.tasks.length;
        const failed = r.tasks.filter((t) => t.state === 'FAILED').length;
        const running = r.tasks.filter((t) => t.state === 'RUNNING').length;
        return (
          <Space size={4}>
            <Tag color="green">{running}/{total} running</Tag>
            {failed > 0 && <Tag color="red" icon={<WarningOutlined />}>{failed} failed</Tag>}
          </Space>
        );
      },
    },
    {
      title: 'Class',
      dataIndex: 'connector_class',
      render: (v: string) => <Text type="secondary" style={{ fontSize: 12 }}>{(v || '').split('.').pop()}</Text>,
    },
    {
      title: 'Actions',
      width: 320,
      render: (_: unknown, r: ConnectorView) => (
        <Space>
          <Button size="small" icon={<SyncOutlined />} onClick={() => setPending({ op: 'restart', connector: r.name })}>
            Restart
          </Button>
          {r.state === 'RUNNING' ? (
            <Button size="small" icon={<PauseCircleOutlined />} onClick={() => setPending({ op: 'pause', connector: r.name })}>
              Pause
            </Button>
          ) : (
            <Button size="small" type="primary" icon={<PlayCircleOutlined />} onClick={() => setPending({ op: 'resume', connector: r.name })}>
              Resume
            </Button>
          )}
          <Button size="small" danger icon={<DeleteOutlined />} onClick={() => setDeletePending(r.name)}>
            Delete
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Card bordered={false}>
      <Space style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }}>
        <Title level={4} style={{ margin: 0 }}>Sources & Connectors</Title>
        <Space>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            New Connect
          </Button>
          <Button icon={<ReloadOutlined />} loading={isFetching || isFetchingSources} onClick={() => { refetch(); refetchSources(); }}>
            Refresh
          </Button>
        </Space>
      </Space>

      <Text type="secondary">
        Trang này là điểm vào thực dụng cho connection, connector runtime và source fingerprint. Không cần đi qua Flow 1 nữa.
      </Text>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="Connectors" value={connectors.length} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="Fingerprints" value={fingerprints.length} prefix={<DatabaseOutlined />} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="Linked" value={linkedFingerprints.length} prefix={<SyncOutlined />} /></Card></Col>
        <Col xs={12} sm={6}><Card size="small"><Statistic title="Orphans" value={orphanFingerprints.length + connectorsWithoutFingerprint.length} prefix={<WarningOutlined />} /></Card></Col>
      </Row>

      {failedTasksCount > 0 && (
        <Alert
          style={{ marginTop: 16 }}
          type="error"
          showIcon
          message={`${failedTasksCount} task đang FAILED`}
          description="Mở tab Connectors để restart task hoặc restart cả connector."
        />
      )}

      <Tabs
        style={{ marginTop: 16 }}
        items={[
          {
            key: 'connections',
            label: `Connections (${fingerprints.length})`,
            children: (
              <>
                <Alert
                  type="info"
                  showIcon
                  style={{ marginBottom: 16 }}
                  message="Tạo và chỉnh connection cho MongoDB, MySQL, PostgreSQL"
                  description="Edit Config sẽ update connector config đang active và đồng thời refresh source fingerprint."
                />
                <Table
                  size="middle"
                  loading={sourcesLoading}
                  dataSource={fingerprints}
                  rowKey="id"
                  columns={connectionColumns}
                  pagination={false}
                />
              </>
            ),
          },
          {
            key: 'fingerprints',
            label: `Source Fingerprints (${fingerprints.length})`,
            children: (
              <Table
                size="middle"
                loading={sourcesLoading}
                dataSource={fingerprints}
                rowKey="id"
                columns={fingerprintColumns}
                pagination={false}
              />
            ),
          },
          {
            key: 'connectors',
            label: `Connectors (${connectors.length})`,
            children: (
              <Table
                size="middle"
                loading={isLoading}
                dataSource={connectors}
                rowKey="name"
                columns={connectorColumns}
                expandable={{ expandedRowRender: expandedTasks, rowExpandable: (r) => (r.tasks?.length || 0) > 0 }}
                pagination={false}
              />
            ),
          },
        ]}
      />

      <Modal
        open={!!pending}
        title={pending ? (
          pending.op === 'restartTask'
            ? `Restart task ${pending.taskId} của ${pending.connector}?`
            : `${pending.op.charAt(0).toUpperCase()}${pending.op.slice(1)} ${pending.connector}?`
        ) : ''}
        onOk={submitAction}
        onCancel={() => { setPending(null); setActionReason(''); }}
        confirmLoading={mutation.isPending}
        okText="Xác nhận"
        cancelText="Hủy"
      >
        <p>Thao tác này ghi audit log. Cần lý do ≥ 10 ký tự.</p>
        <Input.TextArea rows={3} value={actionReason} onChange={(e) => setActionReason(e.target.value)} />
      </Modal>

      <Modal
        open={!!deletePending}
        title={`Delete connector: ${deletePending}?`}
        onOk={submitDelete}
        confirmLoading={deleteMut.isPending}
        onCancel={() => { setDeletePending(null); setActionReason(''); }}
        okText="Delete"
        okButtonProps={{ danger: true }}
      >
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Xóa connector sẽ dừng stream CDC."
        />
        <Input.TextArea rows={3} value={actionReason} onChange={(e) => setActionReason(e.target.value)} placeholder="Reason ≥ 10 ký tự" />
      </Modal>

      <Modal
        open={editorOpen}
        title={editorMode === 'create' ? 'New Connect' : `Edit Config: ${editingSource?.connector_name || ''}`}
        onOk={submitEditor}
        onCancel={() => { setEditorOpen(false); setEditingSource(null); form.resetFields(); }}
        confirmLoading={createMut.isPending || updateMut.isPending}
        okText={editorMode === 'create' ? 'Create' : 'Update'}
        width={760}
      >
        <Form form={form} layout="vertical" initialValues={{ dbKind: 'mongodb' }}>
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="dbKind" label="Database Type" rules={[{ required: true }]}>
                <Select options={DB_OPTIONS as unknown as { label: string; value: string }[]} disabled={editorMode === 'edit'} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="connectorName" label="Connector Name" rules={[{ required: true, pattern: /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,128}$/, message: 'Tên chỉ gồm chữ/số/._-' }]}>
                <Input disabled={editorMode === 'edit'} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="host" label="Host" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="port" label="Port" rules={[{ required: true }]}>
                <InputNumber min={1} max={65535} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="database" label={dbKind === 'mongodb' ? 'Database' : 'DB Name'} rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="topicPrefix" label="Topic Prefix" rules={[{ required: true }]}>
                <Input placeholder="cdc.<database>" />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="username" label="Username">
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item
                name="password"
                label="Password"
                tooltip={editorMode === 'edit' && dbKind !== 'mongodb' ? 'Để trống để giữ password hiện tại' : undefined}
                rules={editorMode === 'create' && dbKind !== 'mongodb' ? [{ required: true }] : []}
              >
                <Input.Password placeholder={editorMode === 'edit' ? 'Leave blank to keep current' : ''} />
              </Form.Item>
            </Col>
          </Row>

          {dbKind === 'mongodb' && (
            <>
              <Row gutter={12}>
                <Col span={12}>
                  <Form.Item name="replicaSet" label="Replica Set">
                    <Input />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item name="collectionNames" label="Collections">
                    <Input placeholder="users,orders,payments" />
                  </Form.Item>
                </Col>
              </Row>
            </>
          )}

          {dbKind === 'mysql' && (
            <Row gutter={12}>
              <Col span={12}>
                <Form.Item name="tableIncludeList" label="Table Include List">
                  <Input placeholder="db.orders,db.payments" />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item name="serverId" label="Server ID">
                  <InputNumber min={1} style={{ width: '100%' }} />
                </Form.Item>
              </Col>
            </Row>
          )}

          {dbKind === 'postgresql' && (
            <>
              <Row gutter={12}>
                <Col span={12}>
                  <Form.Item name="schemaIncludeList" label="Schema Include List">
                    <Input placeholder="public" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item name="tableIncludeList" label="Table Include List">
                    <Input placeholder="public.orders,public.payments" />
                  </Form.Item>
                </Col>
              </Row>
              <Row gutter={12}>
                <Col span={12}>
                  <Form.Item name="slotName" label="Slot Name">
                    <Input />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item name="publicationName" label="Publication Name">
                    <Input />
                  </Form.Item>
                </Col>
              </Row>
            </>
          )}

          <Form.Item
            name="reason"
            label="Reason"
            rules={[{ required: true }, { min: 10, message: 'Lý do phải ≥ 10 ký tự' }]}
          >
            <Input.TextArea rows={3} />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}
