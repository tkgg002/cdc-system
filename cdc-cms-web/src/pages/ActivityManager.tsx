import { useEffect, useState, useCallback } from 'react';
import { Table, Switch, InputNumber, Button, Space, Tag, message, Typography, Tooltip, Modal, Form, Select, Input } from 'antd';
import { ReloadOutlined, PlusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { cmsApi } from '../services/api';

const { Title } = Typography;

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
}

const ALL_OPERATIONS = [
  { value: 'bridge', label: 'Đồng bộ dữ liệu (Bridge)' },
  { value: 'transform', label: 'Chuyển đổi field (Transform)' },
  { value: 'field-scan', label: 'Quét field mới (Field Scan)' },
  { value: 'partition-check', label: 'Kiểm tra partition' },
  { value: 'airbyte-sync', label: 'Quét stream Airbyte' },
  { value: 'drop-gin-index', label: 'Xoá GIN Index' },
  { value: 'create-default-columns', label: 'Tạo field mặc định' },
];

const opLabels: Record<string, string> = {};
ALL_OPERATIONS.forEach(o => { opLabels[o.value] = o.label; });

const opColors: Record<string, string> = {
  'bridge': 'cyan',
  'transform': 'purple',
  'field-scan': 'geekblue',
  'partition-check': 'lime',
  'airbyte-sync': 'blue',
  'drop-gin-index': 'volcano',
  'create-default-columns': 'magenta',
};

export default function ActivityManager() {
  const [data, setData] = useState<Schedule[]>([]);
  const [loading, setLoading] = useState(false);
  const [updatingId, setUpdatingId] = useState<number | null>(null);
  const [createVisible, setCreateVisible] = useState(false);
  const [createLoading, setCreateLoading] = useState(false);
  const [tables, setTables] = useState<string[]>([]);
  const [form] = Form.useForm();

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const { data: res } = await cmsApi.get('/api/worker-schedule');
      setData(res.data || []);
    } catch { /* */ }
    finally { setLoading(false); }
  }, []);

  const fetchTables = useCallback(async () => {
    try {
      const { data: res } = await cmsApi.get('/api/registry', { params: { page_size: 200 } });
      setTables((res.data || []).map((r: any) => r.target_table));
    } catch { /* */ }
  }, []);

  useEffect(() => { fetchData(); fetchTables(); }, [fetchData, fetchTables]);

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
      await cmsApi.post('/api/worker-schedule', {
        operation: values.operation,
        target_table: values.target_table || null,
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
      title: 'Bảng đích', dataIndex: 'target_table', width: 180,
      render: (v) => v ? <strong>{v}</strong> : <Tag color="blue">Tất cả</Tag>,
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
      <Title level={4} style={{ marginBottom: 16 }}>Quản lý tác vụ Worker</Title>

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
          <Form.Item name="target_table" label="Bảng đích (để trống = tất cả)">
            <Select placeholder="Tất cả bảng" allowClear>
              {tables.map(t => <Select.Option key={t} value={t}>{t}</Select.Option>)}
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
