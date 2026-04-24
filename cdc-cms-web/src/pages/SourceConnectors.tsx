import { useState } from 'react';
import {
  Table, Card, Typography, Space, Button, Tag, Modal, Input, message, Alert, Descriptions,
} from 'antd';
import {
  ReloadOutlined, DatabaseOutlined, PlayCircleOutlined,
  PauseCircleOutlined, WarningOutlined, SyncOutlined, PlusOutlined, DeleteOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

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

const STATE_COLOR: Record<string, string> = {
  RUNNING: 'green',
  PAUSED: 'orange',
  FAILED: 'red',
  UNASSIGNED: 'default',
  DESTROYED: 'black',
};

type MutationOp = 'restart' | 'pause' | 'resume' | 'restartTask';

interface PendingAction {
  op: MutationOp;
  connector: string;
  taskId?: number;
}

const MONGO_TEMPLATE = `{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "mongodb.connection.string": "mongodb://gpay-mongo:27017/?replicaSet=rs0",
  "database.include.list": "<service-db-name>",
  "collection.include.list": "<service-db-name>.<collection-name>",
  "topic.prefix": "cdc.goopay",
  "signal.data.collection": "<service-db-name>.debezium_signal",
  "capture.mode": "change_streams_update_full",
  "snapshot.mode": "initial",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://gpay-schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://gpay-schema-registry:8081",
  "schema.history.internal.kafka.bootstrap.servers": "gpay-kafka:9092"
}`;

export default function SourceConnectors() {
  const qc = useQueryClient();
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [reason, setReason] = useState('');
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState('');
  const [createConfig, setCreateConfig] = useState(MONGO_TEMPLATE);
  const [deletePending, setDeletePending] = useState<string | null>(null);

  const createMut = useMutation({
    mutationFn: async (args: { name: string; config: Record<string, string>; reason: string }) => {
      const r = await cmsApi.post(
        '/api/v1/system/connectors',
        { name: args.name, config: args.config, reason: args.reason },
        { headers: { 'Idempotency-Key': `cc-create-${args.name}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: () => {
      message.success('Connector created');
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      setCreateOpen(false);
      setCreateName('');
      setCreateConfig(MONGO_TEMPLATE);
      setReason('');
    },
    onError: (err: unknown) => {
      let msg = 'Create failed';
      if (err && typeof err === 'object' && 'response' in err) {
        const r = (err as { response?: { data?: { error?: string; detail?: string } } }).response;
        if (r?.data) msg = `${r.data.error ?? 'error'}${r.data.detail ? `: ${r.data.detail}` : ''}`;
      }
      message.error(msg);
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
      setDeletePending(null);
      setReason('');
    },
    onError: () => message.error('Delete failed'),
  });

  const submitCreate = () => {
    if (!createName.match(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,128}$/)) {
      message.error('Tên connector: bắt đầu bằng chữ/số, cho phép . _ -, tối đa 128 ký tự');
      return;
    }
    if (reason.trim().length < 10) {
      message.warning('Lý do ≥ 10 ký tự');
      return;
    }
    let cfg: Record<string, string>;
    try {
      cfg = JSON.parse(createConfig);
    } catch {
      message.error('Config không phải JSON hợp lệ');
      return;
    }
    if (!cfg['connector.class']) {
      message.error('Config thiếu "connector.class"');
      return;
    }
    createMut.mutate({ name: createName, config: cfg, reason: reason.trim() });
  };

  const submitDelete = () => {
    if (!deletePending) return;
    if (reason.trim().length < 10) { message.warning('Lý do ≥ 10 ký tự'); return; }
    deleteMut.mutate({ name: deletePending, reason: reason.trim() });
  };

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['debezium-connectors'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: ConnectorView[]; count: number }>('/api/v1/system/connectors');
      return r.data.data;
    },
    refetchInterval: 15_000,
  });

  const mutation = useMutation({
    mutationFn: async (p: PendingAction & { reason: string }) => {
      let path = `/api/v1/system/connectors/${encodeURIComponent(p.connector)}/${p.op === 'restartTask' ? `tasks/${p.taskId}/restart` : p.op}`;
      const r = await cmsApi.post(
        path,
        { reason: p.reason },
        { headers: { 'Idempotency-Key': `cc-${p.op}-${p.connector}-${p.taskId ?? ''}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`${vars.op} triggered: ${vars.connector}${vars.taskId !== undefined ? ' task ' + vars.taskId : ''}`);
      qc.invalidateQueries({ queryKey: ['debezium-connectors'] });
      setPending(null);
      setReason('');
    },
    onError: (err: unknown) => {
      let msg = 'Request failed';
      if (err && typeof err === 'object' && 'response' in err) {
        const r = (err as { response?: { data?: { error?: string; detail?: string } } }).response;
        if (r?.data) msg = `${r.data.error ?? 'error'}${r.data.detail ? `: ${r.data.detail}` : ''}`;
      }
      message.error(msg);
    },
  });

  const submit = () => {
    if (!pending) return;
    if (reason.trim().length < 10) {
      message.warning('Lý do phải ≥ 10 ký tự (audit).');
      return;
    }
    mutation.mutate({ ...pending, reason: reason.trim() });
  };

  const failedTasksCount = (data || []).reduce(
    (acc, c) => acc + c.tasks.filter((t) => t.state === 'FAILED').length, 0,
  );

  const taskColumnsBase = [
    { title: 'Task ID', dataIndex: 'id', width: 80 },
    {
      title: 'State', dataIndex: 'state', width: 120,
      render: (s: string) => <Tag color={STATE_COLOR[s] || 'default'}>{s}</Tag>,
    },
    { title: 'Worker', dataIndex: 'worker_id', render: (v: string) => <Text code>{v || '-'}</Text> },
    {
      title: 'Trace', dataIndex: 'trace',
      render: (t: string) => t ? <Text type="danger" style={{ fontSize: 12 }}>{t.slice(0, 200)}…</Text> : <Text type="secondary">-</Text>,
    },
  ];

  // Renders the task sub-table for an expanded connector row.
  const expandedTasks = (row: ConnectorView) => {
    const cols = [
      ...taskColumnsBase,
      {
        title: 'Action', width: 130,
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
    ];
    return (
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
          columns={cols}
          pagination={false}
        />
      </div>
    );
  };

  const columns = [
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
      width: 300,
      render: (_: unknown, r: ConnectorView) => (
        <Space>
          <Button
            size="small"
            icon={<SyncOutlined />}
            onClick={() => setPending({ op: 'restart', connector: r.name })}
          >
            Restart
          </Button>
          {r.state === 'RUNNING' ? (
            <Button
              size="small"
              icon={<PauseCircleOutlined />}
              onClick={() => setPending({ op: 'pause', connector: r.name })}
            >
              Pause
            </Button>
          ) : (
            <Button
              size="small"
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={() => setPending({ op: 'resume', connector: r.name })}
            >
              Resume
            </Button>
          )}
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => setDeletePending(r.name)}
          >
            Delete
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Card bordered={false}>
      <Space style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }}>
        <Title level={4} style={{ margin: 0 }}>Debezium Command Center</Title>
        <Space>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            New Connector
          </Button>
          <Button icon={<ReloadOutlined />} loading={isFetching} onClick={() => refetch()}>Refresh</Button>
        </Space>
      </Space>

      <Text type="secondary">
        Kafka Connect REST proxy (/api/v1/system/connectors). Bật/tắt Debezium connector, restart task lẻ.
        Mọi thao tác destructive cần reason ≥ 10 ký tự cho audit.
      </Text>

      {failedTasksCount > 0 && (
        <Alert
          style={{ marginTop: 16 }}
          type="error"
          showIcon
          message={`${failedTasksCount} task đang FAILED`}
          description="Click row để mở rộng task list, restart từng task hoặc restart cả connector."
        />
      )}

      <Table
        style={{ marginTop: 16 }}
        size="middle"
        loading={isLoading}
        dataSource={data || []}
        rowKey="name"
        columns={columns}
        expandable={{ expandedRowRender: expandedTasks, rowExpandable: (r) => (r.tasks?.length || 0) > 0 }}
        pagination={false}
      />

      <Modal
        open={!!pending}
        title={pending ? (
          pending.op === 'restartTask'
            ? `Restart task ${pending.taskId} của ${pending.connector}?`
            : `${pending.op.charAt(0).toUpperCase()}${pending.op.slice(1)} ${pending.connector}?`
        ) : ''}
        onOk={submit}
        onCancel={() => { setPending(null); setReason(''); }}
        confirmLoading={mutation.isPending}
        okText="Xác nhận"
        cancelText="Hủy"
      >
        <p>Thao tác này ghi vào audit log. Cần nêu lý do ≥ 10 ký tự.</p>
        <Input.TextArea
          rows={3}
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          placeholder="Lý do (ghi audit, ≥ 10 ký tự)"
        />
      </Modal>

      <Modal
        open={createOpen}
        title="New Debezium Connector"
        onOk={submitCreate}
        confirmLoading={createMut.isPending}
        onCancel={() => { setCreateOpen(false); setReason(''); }}
        okText="Create"
        width={720}
      >
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="Mongo Debezium template pre-filled — edit placeholders <service-db-name> và <collection-name>"
        />
        <Space direction="vertical" style={{ width: '100%' }} size={12}>
          <Input
            placeholder="Connector name (e.g. goopay-wallet-cdc) — [a-zA-Z0-9._-]+"
            value={createName}
            onChange={(e) => setCreateName(e.target.value)}
          />
          <Input.TextArea
            rows={18}
            value={createConfig}
            onChange={(e) => setCreateConfig(e.target.value)}
            style={{ fontFamily: 'ui-monospace, monospace', fontSize: 12 }}
          />
          <Input.TextArea
            rows={2}
            placeholder="Reason ≥ 10 ký tự (audit)"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
          />
        </Space>
      </Modal>

      <Modal
        open={!!deletePending}
        title={`Delete connector: ${deletePending}?`}
        onOk={submitDelete}
        confirmLoading={deleteMut.isPending}
        onCancel={() => { setDeletePending(null); setReason(''); }}
        okText="Delete"
        okButtonProps={{ danger: true }}
      >
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="Xoá connector sẽ ngừng stream. Consumer offset có thể replay từ snapshot mới khi tạo lại."
        />
        <Input.TextArea
          rows={3}
          placeholder="Reason ≥ 10 ký tự"
          value={reason}
          onChange={(e) => setReason(e.target.value)}
        />
      </Modal>
    </Card>
  );
}
