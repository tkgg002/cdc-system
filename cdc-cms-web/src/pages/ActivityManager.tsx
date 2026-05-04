import { useEffect, useState, useCallback, useMemo } from 'react';
import { Table, Switch, InputNumber, Button, Space, Tag, message, Typography, Tooltip, Modal, Form, Select, Input, Alert } from 'antd';
import { ReloadOutlined, PlusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { cmsApi } from '../services/api';
import type { SourceObjectRow } from '../types';

const { Title, Text } = Typography;

interface Schedule {
  id: number;
  operation: string;
  target_table: string | null;
  interval_minutes: number;
  is_enabled: boolean;
  last_run_at: string | null;
  next_run_at: string | null;
  run_count: number;
  last_error: string | null;
  notes: string | null;
  scope?: {
    source_object_id?: number;
    source_database?: string | null;
    source_schema?: string | null;
    source_namespace?: string | null;
    source_table?: string | null;
    shadow_binding_id?: number;
    shadow_schema?: string | null;
    shadow_table?: string | null;
    physical_table_fqn?: string | null;
    scope_ambiguous?: boolean;
  };
}

const ALL_OPERATIONS = [
  { value: 'transform', label: 'Chuyển đổi field (Transform)' },
  { value: 'field-scan', label: 'Quét field mới (Field Scan)' },
  { value: 'partition-check', label: 'Kiểm tra partition' },
  { value: 'drop-gin-index', label: 'Xoá GIN Index' },
  { value: 'create-default-columns', label: 'Tạo field mặc định' },
];

const opLabels: Record<string, string> = {};
ALL_OPERATIONS.forEach(o => { opLabels[o.value] = o.label; });

const opColors: Record<string, string> = {
  'transform': 'purple',
  'field-scan': 'geekblue',
  'partition-check': 'lime',
  'drop-gin-index': 'volcano',
  'create-default-columns': 'magenta',
};

export default function ActivityManager() {
  const [data, setData] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(false);
  const [updatingId, setUpdatingId] = useState<number | null>(null);
  const [createVisible, setCreateVisible] = useState(false);
  const [createLoading, setCreateLoading] = useState(false);
  const [sourceObjects, setSourceObjects] = useState<SourceObjectRow[]>([]);
  const [form] = Form.useForm();

  const normalizeShadowSchema = (sourceDB: string) =>
    `shadow_${sourceDB.toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '') || 'unknown'}`;

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const { data: res } = await cmsApi.get('/api/worker-schedule');
      setData(res.data || []);
    } catch { /* */ }
    finally { setLoading(false); }
  }, []);

  const fetchSourceObjects = useCallback(async () => {
    try {
      const { data: res } = await cmsApi.get('/api/v1/source-objects', { params: { page_size: 500 } });
      setSourceObjects(res.data || []);
    } catch { /* */ }
  }, []);

  useEffect(() => { fetchData(); fetchSourceObjects(); }, [fetchData, fetchSourceObjects]);

  const updateSchedule = async (id: number, updates: any) => {
    setUpdatingId(id);
    try {
      await cmsApi.patch(`/api/worker-schedule/${id}`, updates);
      message.success('Cập nhật thành công');
      fetchData();
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Cập nhật thất bại');
    } finally {
      setUpdatingId(null);
    }
  };

  const handleCreate = async (values: any) => {
    setCreateLoading(true);
    try {
      const selected = tableOptions.find((option) => option.value === values.scope_key);
      await cmsApi.post('/api/worker-schedule', {
        operation: values.operation,
        target_table: selected?.targetTable || null,
        source_database: selected?.sourceDB || null,
        source_table: selected?.sourceTable || null,
        shadow_schema: selected?.shadowSchema || null,
        shadow_table: selected?.shadowTable || null,
        interval_minutes: values.interval_minutes,
        is_enabled: true,
        notes: values.notes || null,
      });
      message.success('Tạo lịch trình thành công');
      setCreateVisible(false);
      form.resetFields();
      fetchData();
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Tạo thất bại');
    } finally {
      setCreateLoading(false);
    }
  };

  const registryByTarget = useMemo(() => {
    const map = new Map<string, SourceObjectRow>();
    sourceObjects.forEach((row) => map.set(row.target_table, row));
    return map;
  }, [sourceObjects]);

  const tableOptions = useMemo(
    () =>
      sourceObjects.map((row) => ({
        value: `${row.source_db}::${row.source_table}::${row.target_table}`,
        label: `${row.source_db}.${row.source_table} -> ${(row.shadow_schema || normalizeShadowSchema(row.source_db))}.${row.target_table}`,
        sourceDB: row.source_db,
        sourceTable: row.source_table,
        shadowSchema: row.shadow_schema || normalizeShadowSchema(row.source_db),
        shadowTable: row.target_table,
        targetTable: row.target_table,
      })),
    [sourceObjects],
  );

  const columns: ColumnsType<Schedule> = [
    {
      title: 'Tác vụ', dataIndex: 'operation', width: 200,
      render: (v) => (
        <Space direction="vertical" size={0}>
          <Tag color={opColors[v] || 'default'}>{opLabels[v] || v}</Tag>
          <span style={{ fontSize: 11, color: '#999' }}>{v}</span>
        </Space>
      ),
    },
    {
      title: 'Scope', dataIndex: 'target_table', width: 320,
      render: (v, record) => {
        if (!v) return <Tag color="blue">Tất cả source objects active</Tag>;
        const scope = record.scope;
        if (scope?.source_database && scope?.source_table && scope?.shadow_schema && scope?.shadow_table) {
          return (
            <Space direction="vertical" size={0}>
              <Text>{scope.source_database}.{scope.source_table}</Text>
              <Space size={6}>
                <Text type="secondary" code>{scope.shadow_schema}.{scope.shadow_table}</Text>
                {scope.scope_ambiguous ? <Tag color="orange">Ambiguous</Tag> : null}
              </Space>
            </Space>
          );
        }
        const meta = registryByTarget.get(v);
        if (!meta) return <Text code>{v}</Text>;
        return (
          <Space direction="vertical" size={0}>
            <Text>{meta.source_db}.{meta.source_table}</Text>
            <Text type="secondary" code>{normalizeShadowSchema(meta.source_db)}.{meta.target_table}</Text>
          </Space>
        );
      },
    },
    {
      title: 'Chu kỳ (phút)', dataIndex: 'interval_minutes', width: 130,
      render: (v, record) => (
        <InputNumber size="small" min={1} max={10080} value={v}
          style={{ width: 80 }}
          onBlur={(e) => {
            const val = parseInt(e.target.value);
            if (val && val !== v) updateSchedule(record.id, { interval_minutes: val });
          }}
          onClick={e => e.stopPropagation()}
        />
      ),
    },
    {
      title: 'Trạng thái', dataIndex: 'is_enabled', width: 100,
      render: (v, record) => (
        <Switch checked={v} size="small" loading={updatingId === record.id}
          onChange={(checked) => updateSchedule(record.id, { is_enabled: checked })}
          checkedChildren="Bật" unCheckedChildren="Tắt"
        />
      ),
    },
    {
      title: 'Lần chạy cuối', dataIndex: 'last_run_at', width: 160,
      render: (v) => v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : <Tag>Chưa chạy</Tag>,
    },
    {
      title: 'Lần chạy kế', dataIndex: 'next_run_at', width: 160,
      render: (v) => v ? new Date(v).toLocaleString('vi-VN', { hour12: false }) : '-',
    },
    {
      title: 'Số lần', dataIndex: 'run_count', width: 80,
      render: (v) => v > 0 ? v.toLocaleString() : '-',
    },
    {
      title: 'Lỗi gần nhất', dataIndex: 'last_error', ellipsis: true, width: 200,
      render: (v) => v ? <Tooltip title={v}><Tag color="red">Có lỗi</Tag></Tooltip> : <Tag color="green">OK</Tag>,
    },
    {
      title: 'Ghi chú', dataIndex: 'notes', ellipsis: true,
      render: (v) => v || '-',
    },
  ];

  return (
    <div>
      <Title level={4} style={{ marginBottom: 16 }}>Operations</Title>
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="Operational scope"
        description="Hệ CMS hiện chạy theo luồng Debezium-only. Các lịch trình được giữ lại chỉ cho transform, field scan, partition check và default-column maintenance; những operation kiểu bridge/Airbyte đã bị loại khỏi UI."
      />

      <Space style={{ marginBottom: 12 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateVisible(true)}>Tạo lịch trình</Button>
        <Button icon={<ReloadOutlined />} onClick={fetchData}>Làm mới</Button>
      </Space>

      <Table
        columns={columns}
        dataSource={data}
        rowKey="id"
        loading={loading}
        pagination={false}
        size="small"
      />

      <Modal
        title="Tạo lịch trình mới"
        open={createVisible}
        onCancel={() => setCreateVisible(false)}
        footer={null}
      >
        <Form form={form} onFinish={handleCreate} layout="vertical">
          <Form.Item name="operation" label="Tác vụ" rules={[{ required: true, message: 'Chọn tác vụ' }]}>
            <Select placeholder="Chọn tác vụ">
              {ALL_OPERATIONS.map(op => (
                <Select.Option key={op.value} value={op.value}>
                  <Tag color={opColors[op.value] || 'default'}>{op.label}</Tag>
                </Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item
            name="scope_key"
            label="Source Object / Shadow Scope (để trống = tất cả)"
            tooltip="API worker-schedule giờ sẽ cố resolve scope theo source/shadow metadata V2. UI vẫn gửi target_table compatibility để không làm gãy dữ liệu cũ."
          >
            <Select placeholder="Tất cả bảng" allowClear>
              {tableOptions.map((t) => <Select.Option key={t.value} value={t.value}>{t.label}</Select.Option>)}
            </Select>
          </Form.Item>
          <Form.Item name="interval_minutes" label="Chu kỳ (phút)" rules={[{ required: true }]} initialValue={5}>
            <InputNumber min={1} max={10080} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="notes" label="Ghi chú">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={createLoading} block>Tạo</Button>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
