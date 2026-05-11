import { useEffect, useState } from 'react';
import {
  Table, Card, Typography, Space, Button, Tag, Modal, Input, Select, message, Alert,
  Descriptions, Switch,
} from 'antd';
import {
  ReloadOutlined, PlusOutlined, CheckCircleOutlined, CloseCircleOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
import { cmsApi } from '../services/api';
import { useAsyncJob } from '../hooks/useAsyncJob';

const { Title, Text } = Typography;

interface MasterRow {
  id: number;
  binding_code: string;
  master_name: string;
  master_schema: string;
  master_database?: string | null;
  master_connection_code?: string | null;
  source_shadow: string;
  source_database?: string | null;
  source_schema?: string | null;
  source_namespace?: string | null;
  source_table?: string | null;
  shadow_binding_id?: number | null;
  shadow_schema?: string | null;
  shadow_table?: string | null;
  physical_table_fqn?: string | null;
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
  const [searchParams, setSearchParams] = useSearchParams();
  const [createOpen, setCreateOpen] = useState(false);
  const [pending, setPending] = useState<{ row: MasterRow; op: 'approve' | 'reject' | 'toggle' } | null>(null);
  const [swapRow, setSwapRow] = useState<MasterRow | null>(null);
  const [swapForm, setSwapForm] = useState({ new_table_name: '', reason: '' });
  const [reason, setReason] = useState('');
  const [form, setForm] = useState({
    master_name: '',
    master_schema: 'dw_public',
    shadow_schema: '',
    shadow_table: '',
    transform_type: 'copy_1_to_1',
    spec: '{"pk":"_gpay_source_id"}',
  });
  const sourceLabel = searchParams.get('source_label');
  const sourceDB = searchParams.get('source_db');
  const sourceTable = searchParams.get('source_table');
  const shadowSchema = searchParams.get('shadow_schema');
  const shadowTable = searchParams.get('shadow_table');

  const normalizeMasterSchema = (value: string) =>
    `dw_${value.toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '') || 'default'}`;

  useEffect(() => {
    const sourceShadow = searchParams.get('source_shadow');
    if (!sourceShadow && !shadowTable) return;
    setForm((prev) => ({
      ...prev,
      master_schema: sourceDB ? normalizeMasterSchema(sourceDB) : prev.master_schema,
      shadow_schema: shadowSchema || prev.shadow_schema,
      shadow_table: shadowTable || sourceShadow || prev.shadow_table,
    }));
    setCreateOpen(true);
  }, [searchParams, shadowSchema, shadowTable, sourceDB]);

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
          master_schema: args.master_schema,
          source_shadow: args.shadow_table,
          source_database: sourceDB || undefined,
          source_table: sourceTable || undefined,
          shadow_schema: args.shadow_schema,
          shadow_table: args.shadow_table,
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

  const { dispatch: dispatchSwap, state: swapState, isPending: isSwapPending, reset: resetSwap } = useAsyncJob({
    endpoint: '', // Dynamic via endpoint override
    invalidateKeys: [['master-registry']],
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

  const submitSwap = () => {
    if (!swapRow) return;
    if (swapForm.reason.trim().length < 10) {
      message.warning('Lý do ≥ 10 ký tự cho audit');
      return;
    }
    dispatchSwap({
      endpoint: `/api/v1/masters/${encodeURIComponent(swapRow.master_name)}/swap`,
      payload: {
        new_table_name: swapForm.new_table_name,
        reason: swapForm.reason.trim(),
      },
    });
  };

  // Listen to swap success
  useEffect(() => {
    if (swapState.status === 'success') {
      message.success(`Swap succeeded for master table`);
      setSwapRow(null);
      setSwapForm({ new_table_name: '', reason: '' });
      resetSwap();
    } else if (swapState.status === 'failed' || swapState.status === 'timeout') {
      message.error(`Swap failed: ${swapState.error}`);
      resetSwap();
    }
  }, [swapState.status, swapState.error, resetSwap]);

  const columns = [
    {
      title: 'Master',
      dataIndex: 'master_name',
      render: (v: string, r: MasterRow) => (
        <Space direction="vertical" size={0}>
          <Space><DatabaseOutlined /><Text code>{r.master_schema}.{v}</Text></Space>
          {r.master_connection_code ? <Text type="secondary">{r.master_connection_code}</Text> : null}
        </Space>
      ),
    },
    {
      title: 'Source / Shadow',
      dataIndex: 'source_shadow',
      render: (v: string, r: MasterRow) => (
        <Space direction="vertical" size={0}>
          {r.source_database && r.source_table ? <Text>{r.source_database}.{r.source_table}</Text> : null}
          <Text type="secondary">{v}</Text>
        </Space>
      ),
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
          <Button
            size="small"
            onClick={() => {
              setSwapRow(r);
              setSwapForm({ new_table_name: r.master_name, reason: '' });
            }}
          >
            Swap
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Card bordered={false}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Master Registry</Title>
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
      {sourceLabel && (
        <Alert
          style={{ marginTop: 16 }}
          type="info"
          showIcon
          message="Source object context"
          description={
            <span>
              Shadow hiện hành: <Text code>{sourceLabel}</Text>
              {sourceDB && sourceTable ? <> từ source <Text code>{sourceDB}.{sourceTable}</Text></> : null}.
              API hiện tại sẽ ưu tiên resolve theo <Text code>shadow_schema</Text> + <Text code>shadow_table</Text>; <Text code>source_shadow</Text> chỉ còn là compatibility fallback.
            </span>
          }
        />
      )}

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
              <Descriptions.Item label="Master FQN">
                <Text code>{r.master_schema}.{r.master_name}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Shadow FQN">
                <Text code>{r.source_shadow}</Text>
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
        onCancel={() => {
          setCreateOpen(false);
          setReason('');
          if (searchParams.get('source_shadow')) {
            const next = new URLSearchParams(searchParams);
            next.delete('source_shadow');
            next.delete('source_label');
            next.delete('source_db');
            next.delete('source_table');
            setSearchParams(next, { replace: true });
          }
        }}
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
            placeholder="master schema (e.g. dw_payment)"
            value={form.master_schema}
            onChange={(e) => setForm({ ...form, master_schema: e.target.value })}
          />
          <Input
            placeholder="shadow schema (e.g. shadow_goopay_payment)"
            value={form.shadow_schema}
            onChange={(e) => setForm({ ...form, shadow_schema: e.target.value })}
          />
          <Input
            placeholder="shadow table (e.g. payments)"
            value={form.shadow_table}
            onChange={(e) => setForm({ ...form, shadow_table: e.target.value })}
          />
          {sourceLabel && (
            <Text type="secondary">
              Shadow namespace hiển thị cho operator: <Text code>{sourceLabel}</Text>. Form sẽ submit theo schema/table thực để API resolve master binding trên metadata V2.
            </Text>
          )}
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

      {/* Swap Modal */}
      <Modal
        open={!!swapRow}
        title={`Swap Master Table: ${swapRow?.master_name}`}
        onOk={submitSwap}
        confirmLoading={isSwapPending}
        onCancel={() => {
          if (!isSwapPending) {
            setSwapRow(null);
            setSwapForm({ new_table_name: '', reason: '' });
          }
        }}
        okText="Swap"
        cancelText="Cancel"
        maskClosable={!isSwapPending}
        closable={!isSwapPending}
      >
        <Alert
          type="warning"
          showIcon
          message="Atomic Swap"
          description="Việc swap sẽ đổi tên bảng thực tế trong DB. Chú ý: Worker sẽ nhận job thông qua NATS và thực thi async (202 Accepted). Đừng đóng ứng dụng cho đến khi xong."
          style={{ marginBottom: 16 }}
        />
        {isSwapPending && (
          <Alert
            type="info"
            message={`Trạng thái Job: ${swapState.status}`}
            description="Đang xử lý, vui lòng đợi..."
            style={{ marginBottom: 16 }}
          />
        )}
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input
            placeholder="New Table Name"
            value={swapForm.new_table_name}
            onChange={(e) => setSwapForm({ ...swapForm, new_table_name: e.target.value })}
            disabled={isSwapPending}
          />
          <Input.TextArea
            rows={3}
            placeholder="Reason ≥ 10 ký tự"
            value={swapForm.reason}
            onChange={(e) => setSwapForm({ ...swapForm, reason: e.target.value })}
            disabled={isSwapPending}
          />
        </Space>
      </Modal>
    </Card>
  );
}
