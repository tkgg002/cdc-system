import { useState } from 'react';
import {
  Table, Card, Typography, Space, Button, Tag, Modal, Input, Badge, message,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, ReloadOutlined, WarningOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

interface ProposalRow {
  id: number;
  table_name: string;
  table_layer: 'shadow' | 'master';
  column_name: string;
  proposed_data_type: string;
  proposed_jsonpath?: string | null;
  proposed_transform_fn?: string | null;
  proposed_is_nullable: boolean;
  sample_values?: Record<string, unknown>;
  status: 'pending' | 'approved' | 'rejected' | 'auto_applied' | 'failed';
  submitted_by: string;
  submitted_at: string;
  reviewed_by?: string | null;
  reviewed_at?: string | null;
  rejection_reason?: string | null;
  error_message?: string | null;
}

const STATUS_COLOR: Record<string, string> = {
  pending: 'gold',
  approved: 'green',
  rejected: 'red',
  auto_applied: 'blue',
  failed: 'volcano',
};

export default function SchemaProposals() {
  const qc = useQueryClient();
  const [statusFilter, setStatusFilter] = useState<string>('pending');
  const [pending, setPending] = useState<{ row: ProposalRow; op: 'approve' | 'reject' } | null>(null);
  const [reason, setReason] = useState('');
  const [override, setOverride] = useState({ dataType: '', jsonpath: '', transformFn: '' });

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['schema-proposals', statusFilter],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: ProposalRow[]; count: number }>(
        `/api/v1/schema-proposals${statusFilter ? `?status=${statusFilter}` : ''}`,
      );
      return r.data.data;
    },
    refetchInterval: 10_000,
  });

  const opMut = useMutation({
    mutationFn: async (args: {
      id: number;
      op: 'approve' | 'reject';
      reason: string;
      override_data_type?: string;
      override_jsonpath?: string;
      override_transform_fn?: string;
    }) => {
      const body: Record<string, unknown> = { reason: args.reason };
      if (args.op === 'approve') {
        if (args.override_data_type) body.override_data_type = args.override_data_type;
        if (args.override_jsonpath) body.override_jsonpath = args.override_jsonpath;
        if (args.override_transform_fn) body.override_transform_fn = args.override_transform_fn;
      }
      const r = await cmsApi.post(
        `/api/v1/schema-proposals/${args.id}/${args.op}`,
        body,
        { headers: { 'Idempotency-Key': `proposal-${args.op}-${args.id}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`${vars.op}: proposal #${vars.id}`);
      qc.invalidateQueries({ queryKey: ['schema-proposals'] });
      setPending(null);
      setReason('');
      setOverride({ dataType: '', jsonpath: '', transformFn: '' });
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

  const submit = () => {
    if (!pending) return;
    if (reason.trim().length < 10) {
      message.warning('Reason ≥ 10 ký tự');
      return;
    }
    opMut.mutate({
      id: pending.row.id,
      op: pending.op,
      reason: reason.trim(),
      override_data_type: override.dataType || undefined,
      override_jsonpath: override.jsonpath || undefined,
      override_transform_fn: override.transformFn || undefined,
    });
  };

  const pendingCount = (data || []).filter((r) => r.status === 'pending').length;

  const columns = [
    { title: 'Table', dataIndex: 'table_name', render: (v: string) => <Text code>{v}</Text> },
    {
      title: 'Layer',
      dataIndex: 'table_layer',
      width: 90,
      render: (l: string) => <Tag color={l === 'master' ? 'purple' : 'cyan'}>{l}</Tag>,
    },
    {
      title: 'Column',
      dataIndex: 'column_name',
      render: (v: string) => <Text code style={{ color: '#1890ff' }}>{v}</Text>,
    },
    {
      title: 'Proposed Type',
      dataIndex: 'proposed_data_type',
      render: (v: string) => <Tag color="blue">{v}</Tag>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      width: 120,
      render: (s: string) => <Tag color={STATUS_COLOR[s] || 'default'}>{s}</Tag>,
    },
    {
      title: 'Submitted',
      dataIndex: 'submitted_at',
      render: (v: string) => <Text style={{ fontSize: 12 }}>{new Date(v).toLocaleString()}</Text>,
    },
    {
      title: 'Actions',
      width: 200,
      render: (_: unknown, r: ProposalRow) => (
        <Space>
          <Button
            size="small"
            type="primary"
            icon={<CheckCircleOutlined />}
            disabled={r.status !== 'pending'}
            onClick={() => setPending({ row: r, op: 'approve' })}
          >
            Approve
          </Button>
          <Button
            size="small"
            danger
            icon={<CloseCircleOutlined />}
            disabled={r.status !== 'pending'}
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
        <Title level={4} style={{ margin: 0 }}>
          Schema Proposals
          {pendingCount > 0 && (
            <Badge
              count={pendingCount}
              style={{ backgroundColor: '#faad14', marginLeft: 12 }}
              offset={[0, -4]}
            />
          )}
        </Title>
        <Space>
          <Button
            size="small"
            type={statusFilter === 'pending' ? 'primary' : 'default'}
            onClick={() => setStatusFilter('pending')}
          >
            Pending
          </Button>
          <Button
            size="small"
            type={statusFilter === '' ? 'primary' : 'default'}
            onClick={() => setStatusFilter('')}
          >
            All
          </Button>
          <Button icon={<ReloadOutlined />} loading={isFetching} onClick={() => refetch()}>
            Refresh
          </Button>
        </Space>
      </Space>
      <Text type="secondary">
        Sprint 5 §R9 — SinkWorker auto-emit khi gặp field mới trong financial shadow.
        Approve → ALTER TABLE + mapping_rule INSERT (transaction). Reject → field stays in _raw_data.
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
            <div style={{ fontSize: 12 }}>
              <p><strong>Sample values:</strong></p>
              <pre style={{ margin: 0 }}>{JSON.stringify(r.sample_values, null, 2)}</pre>
              {r.error_message && <p style={{ color: 'red' }}>Error: {r.error_message}</p>}
              {r.rejection_reason && <p style={{ color: 'orange' }}>Rejected: {r.rejection_reason}</p>}
            </div>
          ),
        }}
        pagination={{ pageSize: 20 }}
      />

      <Modal
        open={!!pending}
        title={pending ? `${pending.op.toUpperCase()} Proposal #${pending.row.id}` : ''}
        onOk={submit}
        confirmLoading={opMut.isPending}
        onCancel={() => { setPending(null); setReason(''); setOverride({ dataType: '', jsonpath: '', transformFn: '' }); }}
        okText="Confirm"
        width={560}
      >
        {pending?.op === 'approve' && (
          <Space direction="vertical" style={{ width: '100%' }} size={12}>
            <div>
              <Text type="secondary">
                Proposed: <Text code>{pending.row.proposed_data_type}</Text> on{' '}
                <Text code>{pending.row.table_layer}.{pending.row.table_name}.{pending.row.column_name}</Text>
              </Text>
            </div>
            <div>
              <Text type="warning" style={{ fontSize: 12 }}>
                <WarningOutlined /> Override chỉ điền nếu muốn đổi type/path/transform khi approve.
              </Text>
            </div>
            <Input
              placeholder={`Override data_type (default: ${pending.row.proposed_data_type})`}
              value={override.dataType}
              onChange={(e) => setOverride({ ...override, dataType: e.target.value })}
            />
            <Input
              placeholder="Override jsonpath (master layer only)"
              value={override.jsonpath}
              onChange={(e) => setOverride({ ...override, jsonpath: e.target.value })}
            />
            <Input
              placeholder="Override transform_fn (e.g. mongo_date_ms)"
              value={override.transformFn}
              onChange={(e) => setOverride({ ...override, transformFn: e.target.value })}
            />
            <Input.TextArea
              rows={2}
              placeholder="Reason ≥ 10 ký tự"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
            />
          </Space>
        )}
        {pending?.op === 'reject' && (
          <Space direction="vertical" style={{ width: '100%' }} size={12}>
            <Text>Reject proposal — field sẽ tiếp tục nằm trong <code>_raw_data</code> only, không thêm column.</Text>
            <Input.TextArea
              rows={3}
              placeholder="Rejection reason ≥ 10 ký tự"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
            />
          </Space>
        )}
      </Modal>
    </Card>
  );
}
