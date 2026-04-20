import { useEffect, useState, useCallback } from 'react';
import { Table, Tag, Button, Space, Modal, Input, Select, Form, message } from 'antd';
import { CheckCircleOutlined, CloseCircleOutlined, FilterOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { useSearchParams } from 'react-router-dom';
import { cmsApi } from '../services/api';
import type { MappingRule } from '../types';

const { TextArea } = Input;

const TYPE_OPTIONS = [
  'VARCHAR(50)', 'VARCHAR(100)', 'VARCHAR(255)', 'TEXT',
  'INTEGER', 'BIGINT', 'DECIMAL(18,6)',
  'BOOLEAN', 'TIMESTAMP', 'JSONB',
];

export default function SchemaChanges() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData] = useState<MappingRule[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState('pending');
  
  // URL Filters
  const [sourceDBFilter, setSourceDBFilter] = useState(searchParams.get('source_db') || '');
  const [tableFilter, setTableFilter] = useState(searchParams.get('table_name') || '');

  // Approve modal
  const [approveVisible, setApproveVisible] = useState(false);
  const [selected, setSelected] = useState<MappingRule | null>(null);
  const [approveForm] = Form.useForm();

  // Reject modal
  const [rejectVisible, setRejectVisible] = useState(false);
  const [rejectReason, setRejectReason] = useState('');
  
  // Loading state for modal actions
  const [actionLoading, setActionLoading] = useState(false);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params: any = { status: statusFilter, page, page_size: 15 };
      if (sourceDBFilter) params.source_db = sourceDBFilter;
      if (tableFilter) params.table_name = tableFilter;

      const { data: res } = await cmsApi.get('/api/mapping-rules', { params });
      // Mapping API return is usually just a list or { data, total }
      if (Array.isArray(res)) {
        setData(res);
        setTotal(res.length);
      } else {
        setData(res.data || []);
        setTotal(res.total || 0);
      }
    } catch { /* interceptor handles 401 */ }
    finally { setLoading(false); }
  }, [statusFilter, page, sourceDBFilter, tableFilter]);

  useEffect(() => { fetchData(); }, [fetchData]);

  // Update URL when local filters change
  useEffect(() => {
    const newParams = new URLSearchParams();
    if (sourceDBFilter) newParams.set('source_db', sourceDBFilter);
    if (tableFilter) newParams.set('table_name', tableFilter);
    // Keep status filter in URL too? Maybe.
    setSearchParams(newParams);
  }, [sourceDBFilter, tableFilter, setSearchParams]);

  // Auto-refresh every 30s
  useEffect(() => {
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleApprove = async (values: any) => {
    if (!selected) return;
    setActionLoading(true);
    try {
      // For MappingRule, approval might be a PATCH to the rule status
      // or a specific approve endpoint. Let's assume PATCH /api/mapping-rules/:id
      // based on the new flow
      await cmsApi.patch(`/api/mapping-rules/${selected.id}`, { 
        status: 'approved',
        target_column: values.target_column,
        data_type: values.data_type,
        is_active: true
      });
      message.success('Field mapping approved');
      setApproveVisible(false);
      approveForm.resetFields();
      fetchData();
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Approve failed');
    } finally {
      setActionLoading(false);
    }
  };

  const handleReject = async () => {
    if (!selected || !rejectReason) return;
    setActionLoading(true);
    try {
      await cmsApi.patch(`/api/mapping-rules/${selected.id}`, { 
        status: 'rejected',
        notes: rejectReason,
        is_active: false
      });
      message.success('Field mapping rejected');
      setRejectVisible(false);
      setRejectReason('');
      fetchData();
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Reject failed');
    } finally {
      setActionLoading(false);
    }
  };

  const columns: ColumnsType<MappingRule> = [
    { title: 'Source DB', dataIndex: 'source_db', width: 120, render: (v, rec) => v || rec.source_table.split('.')[0] },
    { title: 'Table', dataIndex: 'source_table', width: 150 },
    { title: 'Field', dataIndex: 'source_field', width: 140 },
    {
      title: 'Type', dataIndex: 'data_type', width: 130, render: (t: string) => <Tag color="blue">{t}</Tag>
    },
    { title: 'Rule Type', dataIndex: 'rule_type', width: 110, render: (t: string) => <Tag>{t.toUpperCase()}</Tag> },
    {
      title: 'Status', dataIndex: 'status', width: 100,
      render: (s: string) => <Tag color={{ pending: 'orange', approved: 'green', rejected: 'red' }[s]}>{s.toUpperCase()}</Tag>,
    },
    {
      title: 'Actions', width: 180,
      render: (_, record) => (
        <Space>
          <Button type="primary" size="small" icon={<CheckCircleOutlined />}
            disabled={record.status !== 'pending'}
            onClick={() => { setSelected(record); approveForm.setFieldsValue({ target_column: record.target_column || record.source_field, data_type: record.data_type }); setApproveVisible(true); }}>
            Approve
          </Button>
          <Button danger size="small" icon={<CloseCircleOutlined />}
            disabled={record.status !== 'pending'}
            onClick={() => { setSelected(record); setRejectVisible(true); }}>
            Reject
          </Button>
        </Space>
      ),
    },
  ];

  const clearFilters = () => {
    setSourceDBFilter('');
    setTableFilter('');
    setSearchParams({});
  };

  return (
    <div>
      <Space style={{ marginBottom: 16 }} wrap>
        <Select value={statusFilter} onChange={(v) => { setStatusFilter(v); setPage(1); }} style={{ width: 130 }}>
          <Select.Option value="pending">Pending</Select.Option>
          <Select.Option value="approved">Approved</Select.Option>
          <Select.Option value="rejected">Rejected</Select.Option>
        </Select>
        
        <Input.Group compact>
          <Input 
            style={{ width: 150 }} 
            placeholder="Source DB" 
            value={sourceDBFilter} 
            onChange={e => setSourceDBFilter(e.target.value)} 
          />
          <Input 
            style={{ width: 200 }} 
            placeholder="Table Name" 
            value={tableFilter} 
            onChange={e => setTableFilter(e.target.value)} 
          />
          <Button icon={<CloseCircleOutlined />} onClick={clearFilters} title="Clear Filters" />
        </Input.Group>

        <Button onClick={fetchData}>Refresh</Button>
        {(sourceDBFilter || tableFilter) && <Tag closable onClose={clearFilters} color="blue" icon={<FilterOutlined />}>Filtering active</Tag>}
      </Space>

      <Table columns={columns} dataSource={data} rowKey="id" loading={loading}
        pagination={{ current: page, total, pageSize: 15, onChange: setPage }} size="small" />

      {/* Approve Modal */}
      <Modal title="Approve Schema Change" open={approveVisible} onOk={() => approveForm.submit()}
        onCancel={() => setApproveVisible(false)} confirmLoading={actionLoading} width={500}>
        {selected && (
          <>
            <p><strong>Table:</strong> {selected.source_table} | <strong>Field:</strong> {selected.source_field}</p>
            <Form form={approveForm} layout="vertical" onFinish={handleApprove}>
              <Form.Item name="target_column" label="Target Column" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
              <Form.Item name="data_type" label="Data Type" rules={[{ required: true }]}>
                <Select>{TYPE_OPTIONS.map(t => <Select.Option key={t} value={t}>{t}</Select.Option>)}</Select>
              </Form.Item>
              <Form.Item name="approval_notes" label="Notes">
                <TextArea rows={2} />
              </Form.Item>
            </Form>
          </>
        )}
      </Modal>

      {/* Reject Modal */}
      <Modal title="Reject Schema Change" open={rejectVisible} onOk={handleReject}
        confirmLoading={actionLoading} onCancel={() => { setRejectVisible(false); setRejectReason(''); }}>
        {selected && <p><strong>Table:</strong> {selected.source_table} | <strong>Field:</strong> {selected.source_field}</p>}
        <TextArea rows={3} placeholder="Rejection reason..." value={rejectReason} onChange={e => setRejectReason(e.target.value)} />
      </Modal>
    </div>
  );
}
