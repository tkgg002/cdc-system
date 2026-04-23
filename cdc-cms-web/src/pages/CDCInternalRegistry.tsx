import { useState } from 'react';
import {
  Table, Switch, Tag, Typography, Modal, Input, Button, message, Space, Tooltip,
} from 'antd';
import { InfoCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { cmsApi } from '../services/api';

const { Title, Text } = Typography;

interface CDCInternalRegistryEntry {
  target_table: string;
  source_db: string;
  source_collection: string;
  profile_status: 'pending_data' | 'syncing' | 'active' | 'failed';
  is_financial: boolean;
  schema_approved_at?: string | null;
  schema_approved_by?: string | null;
  created_at: string;
  updated_at: string;
}

const STATUS_COLOR: Record<string, string> = {
  pending_data: 'default',
  syncing: 'blue',
  active: 'green',
  failed: 'red',
};

export default function CDCInternalRegistry() {
  const qc = useQueryClient();
  const [modalRow, setModalRow] = useState<CDCInternalRegistryEntry | null>(null);
  const [modalNext, setModalNext] = useState<boolean>(false);
  const [reason, setReason] = useState('');

  const { data, isLoading, refetch, isFetching } = useQuery({
    queryKey: ['cdc-internal-registry'],
    queryFn: async () => {
      const r = await cmsApi.get<{ data: CDCInternalRegistryEntry[]; count: number }>(
        '/api/v1/tables',
      );
      return r.data.data;
    },
    refetchInterval: 15_000,
  });

  const patch = useMutation({
    mutationFn: async (args: {
      name: string;
      is_financial: boolean;
      reason: string;
    }) => {
      const r = await cmsApi.patch<{ data: CDCInternalRegistryEntry }>(
        `/api/v1/tables/${encodeURIComponent(args.name)}`,
        { is_financial: args.is_financial, reason: args.reason },
        {
          headers: {
            'Idempotency-Key': `cdc-internal-patch-${args.name}-${Date.now()}`,
          },
        },
      );
      return r.data.data;
    },
    onSuccess: (row) => {
      message.success(
        `${row.target_table}: is_financial=${row.is_financial ? 'true' : 'false'}`,
      );
      qc.invalidateQueries({ queryKey: ['cdc-internal-registry'] });
      setModalRow(null);
      setReason('');
    },
    onError: (err: unknown) => {
      let msg = 'Không thể cập nhật';
      if (err && typeof err === 'object' && 'response' in err) {
        const resp = (err as { response?: { data?: { error?: string; detail?: string } } }).response;
        if (resp?.data?.error) msg = `${resp.data.error}${resp.data.detail ? `: ${resp.data.detail}` : ''}`;
      }
      message.error(msg);
    },
  });

  const openConfirm = (row: CDCInternalRegistryEntry, next: boolean) => {
    setModalRow(row);
    setModalNext(next);
    setReason('');
  };

  const submit = () => {
    if (!modalRow) return;
    if (reason.trim().length < 10) {
      message.warning('Lý do phải ≥ 10 ký tự để phục vụ audit trail.');
      return;
    }
    patch.mutate({
      name: modalRow.target_table,
      is_financial: modalNext,
      reason: reason.trim(),
    });
  };

  const columns = [
    {
      title: 'Table',
      dataIndex: 'target_table',
      width: 220,
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: 'Source',
      render: (_: unknown, r: CDCInternalRegistryEntry) => (
        <Text type="secondary">
          {r.source_db}.{r.source_collection}
        </Text>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'profile_status',
      width: 130,
      render: (s: string) => <Tag color={STATUS_COLOR[s] || 'default'}>{s}</Tag>,
    },
    {
      title: (
        <Space size={4}>
          is_financial
          <Tooltip
            title={
              <>
                Khi <b>true</b>: SinkWorker từ chối auto-ALTER cho field mới
                (giữ trong <code>_raw_data</code>, chờ admin review).
                <br />
                Khi <b>false</b>: admin đã duyệt schema, SinkWorker tự ALTER
                ADD COLUMN mọi field mới từ payload.
                <br />
                Toggle có TTL 60s — worker picks up không cần restart.
              </>
            }
          >
            <InfoCircleOutlined tabIndex={0} aria-label="is_financial help" />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'is_financial',
      width: 160,
      render: (v: boolean, r: CDCInternalRegistryEntry) => (
        <Switch
          checked={v}
          checkedChildren="financial"
          unCheckedChildren="open"
          loading={patch.isPending && modalRow?.target_table === r.target_table}
          onChange={(next) => openConfirm(r, next)}
        />
      ),
    },
    {
      title: 'Schema approved',
      render: (_: unknown, r: CDCInternalRegistryEntry) =>
        r.schema_approved_at ? (
          <Text>
            {new Date(r.schema_approved_at).toLocaleString()} by{' '}
            <Text code>{r.schema_approved_by}</Text>
          </Text>
        ) : (
          <Text type="secondary">—</Text>
        ),
    },
  ];

  return (
    <div>
      <Space
        style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }}
      >
        <Title level={3} style={{ margin: 0 }}>
          Shadow Registry (<code>cdc_internal.table_registry</code>)
        </Title>
        <Button
          icon={<ReloadOutlined />}
          loading={isFetching}
          onClick={() => refetch()}
        >
          Refresh
        </Button>
      </Space>

      <Text type="secondary">
        Pháo đài v1.25. Bật/tắt <code>is_financial</code> để cho phép auto-ALTER
        ADD COLUMN từ Debezium payload. Toggle ghi audit + cần reason ≥ 10 ký tự.
      </Text>

      <Table
        style={{ marginTop: 16 }}
        size="middle"
        loading={isLoading}
        dataSource={data || []}
        rowKey="target_table"
        columns={columns}
        pagination={false}
      />

      <Modal
        open={!!modalRow}
        title={
          modalRow
            ? `Đổi is_financial: ${modalRow.target_table} → ${modalNext ? 'true' : 'false'}`
            : ''
        }
        onOk={submit}
        confirmLoading={patch.isPending}
        onCancel={() => {
          setModalRow(null);
          setReason('');
        }}
        okText="Xác nhận"
        cancelText="Hủy"
      >
        <p>
          {modalNext
            ? 'Bật cờ financial sẽ CHẶN auto-ALTER. Field mới từ payload sẽ chỉ nằm trong _raw_data.'
            : 'Tắt cờ financial sẽ MỞ auto-ALTER. SinkWorker tự thêm column mỗi khi gặp field chưa có.'}
        </p>
        <Input.TextArea
          rows={3}
          placeholder="Lý do thao tác (ghi audit, ≥ 10 ký tự)"
          value={reason}
          onChange={(e) => setReason(e.target.value)}
        />
      </Modal>
    </div>
  );
}
