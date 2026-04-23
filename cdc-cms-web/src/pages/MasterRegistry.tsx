import { useState } from 'react';
import {
  Table, Card, Typography, Space, Button, Tag, Modal, Input, Select, message, Alert,
  Descriptions, Switch,
} from 'antd';
import {
  ReloadOutlined, PlusOutlined, CheckCircleOutlined, CloseCircleOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

interface MasterRow {
  id: number;
  master_name: string;
  source_shadow: string;
  transform_type: string;
  spec: unknown;
  is_active: boolean;
  schema_status: 'pending_review' | 'approved' | 'rejected' | 'failed';
  schema_reviewed_by?: string | null;
  schema_reviewed_at?: string | null;
  rejection_reason?: string | null;
  created_by?: string | null;
  created_at: string;
  updated_at: string;
}

const STATUS_COLOR: Record<string, string> = {
  pending_review: 'gold',
  approved: 'green',
  rejected: 'red',
  failed: 'volcano',
};

const TRANSFORM_TYPES = ['copy_1_to_1', 'filter', 'aggregate', 'group_by', 'join'];

export default function MasterRegistry() {
  const qc = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [pending, setPending] = useState<{ row: MasterRow; op: 'approve' | 'reject' | 'toggle' } | null>(null);
  const [reason, setReason] = useState('');
  const [form, setForm] = useState({
    master_name: '',
    source_shadow: '',
    transform_type: 'copy_1_to_1',
    spec: '{"pk":"_gpay_source_id"}',
  });

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['master-registry'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: MasterRow[]; count: number }>('/api/v1/masters');
      return r.data.data;
    },
    refetchInterval: 15_000,
  });

  const createMut = useMutation({
    mutationFn: async (args: typeof form & { reason: string }) => {
      const r = await cmsApi.post(
        '/api/v1/masters',
        {
          master_name: args.master_name,
          source_shadow: args.source_shadow,
          transform_type: args.transform_type,
          spec: JSON.parse(args.spec),
          reason: args.reason,
        },
        { headers: { 'Idempotency-Key': `master-create-${args.master_name}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: () => {
      message.success('Master registered — pending schema review');
      qc.invalidateQueries({ queryKey: ['master-registry'] });
      setCreateOpen(false);
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

  const opMut = useMutation({
    mutationFn: async (args: { name: string; op: 'approve' | 'reject' | 'toggle'; reason: string }) => {
      const path = args.op === 'toggle' ? 'toggle-active' : args.op;
      const r = await cmsApi.post(
        `/api/v1/masters/${encodeURIComponent(args.name)}/${path}`,
        { reason: args.reason },
        { headers: { 'Idempotency-Key': `master-${args.op}-${args.name}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`${vars.op}: ${vars.name}`);
      qc.invalidateQueries({ queryKey: ['master-registry'] });
      setPending(null);
      setReason('');
    },
    onError: (err: unknown) => {
      let msg = 'Operation failed';
      if (err && typeof err === 'object' && 'response' in err) {
        const r = (err as { response?: { data?: { error?: string; detail?: string } } }).response;
        if (r?.data) msg = `${r.data.error ?? 'error'}${r.data.detail ? `: ${r.data.detail}` : ''}`;
      }
      message.error(msg);
    },
  });

  const submitCreate = () => {
    if (reason.trim().length < 10) {
      message.warning('Lý do ≥ 10 ký tự cho audit');
      return;
    }
    try { JSON.parse(form.spec); } catch { message.error('Spec phải là JSON hợp lệ'); return; }
    createMut.mutate({ ...form, reason: reason.trim() });
  };

  const submitOp = () => {
    if (!pending) return;
    if (reason.trim().length < 10) {
      message.warning('Lý do ≥ 10 ký tự cho audit');
      return;
    }
    opMut.mutate({ name: pending.row.master_name, op: pending.op, reason: reason.trim() });
  };

  const columns = [
    {
      title: 'Master',
      dataIndex: 'master_name',
      render: (v: string) => <Space><DatabaseOutlined /><Text code>{v}</Text></Space>,
    },
    {
      title: 'Shadow',
      dataIndex: 'source_shadow',
      render: (v: string) => <Text type="secondary">{v}</Text>,
    },
    {
      title: 'Transform',
      dataIndex: 'transform_type',
      width: 120,
      render: (v: string) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: 'Status',
      dataIndex: 'schema_status',
      width: 140,
      render: (s: string) => <Tag color={STATUS_COLOR[s] || 'default'}>{s}</Tag>,
    },
    {
      title: 'Active',
      dataIndex: 'is_active',
      width: 90,
      render: (v: boolean, r: MasterRow) => (
        <Switch
          checked={v}
          size="small"
          disabled={r.schema_status !== 'approved'}
          onChange={() => setPending({ row: r, op: 'toggle' })}
        />
      ),
    },
    {
      title: 'Reviewed',
      render: (_: unknown, r: MasterRow) => (
        r.schema_reviewed_at ? (
          <Text style={{ fontSize: 12 }}>
            {new Date(r.schema_reviewed_at).toLocaleString()} • {r.schema_reviewed_by}
          </Text>
        ) : <Text type="secondary">—</Text>
      ),
    },
    {
      title: 'Actions',
      width: 240,
      render: (_: unknown, r: MasterRow) => (
        <Space>
          <Button
            size="small"
            type="primary"
            icon={<CheckCircleOutlined />}
            disabled={r.schema_status === 'approved'}
            onClick={() => setPending({ row: r, op: 'approve' })}
          >
            Approve
          </Button>
          <Button
            size="small"
            danger
            icon={<CloseCircleOutlined />}
            disabled={r.schema_status === 'rejected'}
            onClick={() => setPending({ row: r, op: 'reject' })}
          >
            Reject
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Card bordered={false}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Master Table Registry</Title>
        <Space>
          <Button icon={<ReloadOutlined />} loading={isFetching} onClick={() => refetch()}>Refresh</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            Create Master
          </Button>
        </Space>
      </Space>
      <Text type="secondary">
        Sprint 5 §R8 — Approve triggers worker <code>cdc.cmd.master-create</code> → auto DDL (CREATE TABLE + indexes + RLS).
        Active gate L2: is_active chỉ flip được khi schema_status='approved'.
      </Text>

      <Table
        style={{ marginTop: 16 }}
        size="middle"
        loading={isLoading}
        dataSource={data || []}
        rowKey="id"
        columns={columns}
        expandable={{
          expandedRowRender: (r) => (
            <Descriptions size="small" column={2} bordered>
              <Descriptions.Item label="Spec" span={2}>
                <pre style={{ margin: 0, fontSize: 11 }}>{JSON.stringify(r.spec, null, 2)}</pre>
              </Descriptions.Item>
              {r.rejection_reason && (
                <Descriptions.Item label="Rejection reason" span={2}>
                  <Text type="danger">{r.rejection_reason}</Text>
                </Descriptions.Item>
              )}
            </Descriptions>
          ),
        }}
        pagination={false}
      />

      {/* Create Modal */}
      <Modal
        open={createOpen}
        title="Create Master Table"
        onOk={submitCreate}
        onCancel={() => { setCreateOpen(false); setReason(''); }}
        confirmLoading={createMut.isPending}
        okText="Submit for review"
        cancelText="Cancel"
        width={640}
      >
        <Alert
          type="info"
          showIcon
          message="Status sẽ là 'pending_review' sau khi tạo. Click 'Approve' để trigger DDL worker."
          style={{ marginBottom: 16 }}
        />
        <Space direction="vertical" style={{ width: '100%' }} size={12}>
          <Input
            placeholder="master_name (e.g. refund_requests_master)"
            value={form.master_name}
            onChange={(e) => setForm({ ...form, master_name: e.target.value })}
          />
          <Input
            placeholder="source_shadow (cdc_internal table)"
            value={form.source_shadow}
            onChange={(e) => setForm({ ...form, source_shadow: e.target.value })}
          />
          <Select
            style={{ width: '100%' }}
            value={form.transform_type}
            onChange={(v) => setForm({ ...form, transform_type: v })}
            options={TRANSFORM_TYPES.map((t) => ({ label: t, value: t }))}
          />
          <Input.TextArea
            rows={4}
            placeholder='spec (JSON) — e.g. {"pk":"_gpay_source_id"}'
            value={form.spec}
            onChange={(e) => setForm({ ...form, spec: e.target.value })}
          />
          <Input.TextArea
            rows={2}
            placeholder="Reason (≥ 10 chars, ghi audit)"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
          />
        </Space>
      </Modal>

      {/* Approve/Reject/Toggle Modal */}
      <Modal
        open={!!pending}
        title={pending ? `${pending.op.toUpperCase()}: ${pending.row.master_name}` : ''}
        onOk={submitOp}
        confirmLoading={opMut.isPending}
        onCancel={() => { setPending(null); setReason(''); }}
        okText="Confirm"
        cancelText="Cancel"
      >
        <p>
          {pending?.op === 'approve' && 'Duyệt master → worker sẽ auto CREATE TABLE + indexes + RLS.'}
          {pending?.op === 'reject' && 'Từ chối master (is_active sẽ tắt). Lý do sẽ ghi vào rejection_reason.'}
          {pending?.op === 'toggle' && 'Bật/tắt is_active gate L2. Transmuter sẽ skip runs khi tắt.'}
        </p>
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
