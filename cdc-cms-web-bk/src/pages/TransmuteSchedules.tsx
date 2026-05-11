import { useState } from 'react';
import {
  Table, Card, Typography, Space, Button, Tag, Modal, Input, Select, Switch, message,
} from 'antd';
import { ReloadOutlined, PlusOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

interface ScheduleRow {
  id: number;
  master_table: string;
  mode: 'cron' | 'immediate' | 'post_ingest';
  cron_expr?: string | null;
  last_run_at?: string | null;
  next_run_at?: string | null;
  last_status?: string | null;
  last_error?: string | null;
  last_stats?: Record<string, unknown>;
  is_enabled: boolean;
  created_by?: string | null;
  created_at: string;
  updated_at: string;
}

const MODE_COLOR: Record<string, string> = {
  cron: 'blue',
  immediate: 'green',
  post_ingest: 'purple',
};

const STATUS_COLOR: Record<string, string> = {
  running: 'processing',
  success: 'success',
  failed: 'error',
  skipped: 'default',
};

export default function TransmuteSchedules() {
  const qc = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [runPending, setRunPending] = useState<ScheduleRow | null>(null);
  const [togglePending, setTogglePending] = useState<ScheduleRow | null>(null);
  const [reason, setReason] = useState('');
  const [form, setForm] = useState({
    master_table: '',
    mode: 'cron',
    cron_expr: '0 */6 * * *',
    is_enabled: true,
  });

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['transmute-schedules'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: ScheduleRow[]; count: number }>('/api/v1/schedules');
      return r.data.data;
    },
    refetchInterval: 15_000,
  });

  const createMut = useMutation({
    mutationFn: async (args: typeof form & { reason: string }) => {
      const r = await cmsApi.post(
        '/api/v1/schedules',
        args,
        { headers: { 'Idempotency-Key': `sched-create-${args.master_table}-${args.mode}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: () => {
      message.success('Schedule saved');
      qc.invalidateQueries({ queryKey: ['transmute-schedules'] });
      setCreateOpen(false);
      setReason('');
    },
    onError: (err: unknown) => {
      let msg = 'Save failed';
      if (err && typeof err === 'object' && 'response' in err) {
        const r = (err as { response?: { data?: { error?: string; detail?: string } } }).response;
        if (r?.data) msg = `${r.data.error ?? 'error'}${r.data.detail ? `: ${r.data.detail}` : ''}`;
      }
      message.error(msg);
    },
  });

  const toggleMut = useMutation({
    mutationFn: async (args: { id: number; is_enabled: boolean; reason: string }) => {
      const r = await cmsApi.patch(
        `/api/v1/schedules/${args.id}`,
        { is_enabled: args.is_enabled, reason: args.reason },
        { headers: { 'Idempotency-Key': `sched-toggle-${args.id}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`Schedule #${vars.id} ${vars.is_enabled ? 'enabled' : 'disabled'}`);
      qc.invalidateQueries({ queryKey: ['transmute-schedules'] });
      setTogglePending(null);
      setReason('');
    },
    onError: () => message.error('Toggle failed'),
  });

  const runNowMut = useMutation({
    mutationFn: async (args: { id: number; reason: string }) => {
      const r = await cmsApi.post(
        `/api/v1/schedules/${args.id}/run-now`,
        { reason: args.reason },
        { headers: { 'Idempotency-Key': `sched-run-${args.id}-${Date.now()}` } },
      );
      return r.data;
    },
    onSuccess: (_data, vars) => {
      message.success(`Run-now dispatched: #${vars.id}`);
      qc.invalidateQueries({ queryKey: ['transmute-schedules'] });
      setRunPending(null);
      setReason('');
    },
    onError: () => message.error('Run-now failed'),
  });

  const submitCreate = () => {
    if (reason.trim().length < 10) { message.warning('Reason ≥ 10 ký tự'); return; }
    createMut.mutate({ ...form, reason: reason.trim() });
  };

  const columns = [
    { title: 'Master', dataIndex: 'master_table', render: (v: string) => <Text code>{v}</Text> },
    {
      title: 'Mode',
      dataIndex: 'mode',
      width: 110,
      render: (v: string) => <Tag color={MODE_COLOR[v] || 'default'}>{v}</Tag>,
    },
    {
      title: 'Cron',
      dataIndex: 'cron_expr',
      render: (v: string | null) => v ? <Text code>{v}</Text> : <Text type="secondary">—</Text>,
    },
    {
      title: 'Next run',
      dataIndex: 'next_run_at',
      render: (v: string | null) => v ? <Text style={{ fontSize: 12 }}>{new Date(v).toLocaleString()}</Text> : '—',
    },
    {
      title: 'Last run',
      render: (_: unknown, r: ScheduleRow) => r.last_run_at ? (
        <Space size={4}>
          <Text style={{ fontSize: 12 }}>{new Date(r.last_run_at).toLocaleString()}</Text>
          {r.last_status && <Tag color={STATUS_COLOR[r.last_status] || 'default'}>{r.last_status}</Tag>}
        </Space>
      ) : <Text type="secondary">—</Text>,
    },
    {
      title: 'Enabled',
      dataIndex: 'is_enabled',
      width: 90,
      render: (v: boolean, r: ScheduleRow) => (
        <Switch checked={v} size="small" onChange={() => setTogglePending(r)} />
      ),
    },
    {
      title: 'Actions',
      width: 160,
      render: (_: unknown, r: ScheduleRow) => (
        <Button
          size="small"
          icon={<PlayCircleOutlined />}
          onClick={() => setRunPending(r)}
        >
          Run now
        </Button>
      ),
    },
  ];

  return (
    <Card bordered={false}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Transmute Schedules</Title>
        <Space>
          <Button icon={<ReloadOutlined />} loading={isFetching} onClick={() => refetch()}>Refresh</Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            New schedule
          </Button>
        </Space>
      </Space>
      <Text type="secondary">
        Sprint 5 Dashboard.2 — Cron / Immediate / Post-ingest modes. Scheduler worker 60s poll + FOR UPDATE SKIP LOCKED + fencing.
      </Text>

      <Table
        style={{ marginTop: 16 }}
        size="middle"
        loading={isLoading}
        dataSource={data || []}
        rowKey="id"
        columns={columns}
        pagination={false}
      />

      <Modal
        open={createOpen}
        title="New transmute schedule"
        onOk={submitCreate}
        confirmLoading={createMut.isPending}
        onCancel={() => { setCreateOpen(false); setReason(''); }}
      >
        <Space direction="vertical" style={{ width: '100%' }} size={12}>
          <Input
            placeholder="master_table"
            value={form.master_table}
            onChange={(e) => setForm({ ...form, master_table: e.target.value })}
          />
          <Select
            style={{ width: '100%' }}
            value={form.mode}
            onChange={(v) => setForm({ ...form, mode: v })}
            options={[
              { label: 'cron (scheduled)', value: 'cron' },
              { label: 'immediate (manual-only)', value: 'immediate' },
              { label: 'post_ingest (real-time)', value: 'post_ingest' },
            ]}
          />
          {form.mode === 'cron' && (
            <Input
              placeholder="cron_expr (5-field, e.g. 0 */6 * * *)"
              value={form.cron_expr}
              onChange={(e) => setForm({ ...form, cron_expr: e.target.value })}
            />
          )}
          <Space>
            <Text>Enabled:</Text>
            <Switch checked={form.is_enabled} onChange={(c) => setForm({ ...form, is_enabled: c })} />
          </Space>
          <Input.TextArea
            rows={2}
            placeholder="Reason ≥ 10 ký tự"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
          />
        </Space>
      </Modal>

      <Modal
        open={!!runPending}
        title={runPending ? `Run now: ${runPending.master_table}` : ''}
        onOk={() => {
          if (!runPending) return;
          if (reason.trim().length < 10) { message.warning('Reason ≥ 10'); return; }
          runNowMut.mutate({ id: runPending.id, reason: reason.trim() });
        }}
        onCancel={() => { setRunPending(null); setReason(''); }}
        confirmLoading={runNowMut.isPending}
      >
        <p>Gửi ngay NATS <code>cdc.cmd.transmute</code> cho master này. Run-now không ảnh hưởng lịch cron.</p>
        <Input.TextArea rows={2} placeholder="Reason ≥ 10 ký tự" value={reason} onChange={(e) => setReason(e.target.value)} />
      </Modal>

      <Modal
        open={!!togglePending}
        title={togglePending ? `Toggle schedule: ${togglePending.master_table}` : ''}
        onOk={() => {
          if (!togglePending) return;
          if (reason.trim().length < 10) { message.warning('Reason ≥ 10'); return; }
          toggleMut.mutate({ id: togglePending.id, is_enabled: !togglePending.is_enabled, reason: reason.trim() });
        }}
        onCancel={() => { setTogglePending(null); setReason(''); }}
        confirmLoading={toggleMut.isPending}
      >
        <p>Flip is_enabled cho schedule #{togglePending?.id}.</p>
        <Input.TextArea rows={2} placeholder="Reason ≥ 10 ký tự" value={reason} onChange={(e) => setReason(e.target.value)} />
      </Modal>
    </Card>
  );
}
