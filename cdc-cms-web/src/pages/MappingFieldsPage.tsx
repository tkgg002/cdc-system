import { useEffect, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Table, Tag, Button, Space, Switch, message, Spin, Typography, Card, Row, Col, Descriptions, Tooltip, Alert, Modal, Input } from 'antd';
import { ArrowLeftOutlined, PlusOutlined, SearchOutlined, ReloadOutlined, CheckCircleOutlined, CloseCircleOutlined, SyncOutlined, EyeOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { cmsApi } from '../services/api';
import type { MappingRule, TableRegistry } from '../types';
import AddMappingModal from '../components/AddMappingModal';

const { Title, Text } = Typography;

const SYSTEM_DEFAULT_FIELDS = [
  { field: '_raw_data', type: 'JSONB', description: 'Full raw event data from source' },
  { field: '_source', type: 'VARCHAR', description: 'Data source identifier (airbyte/debezium)' },
  { field: '_synced_at', type: 'TIMESTAMP', description: 'When the record was last synced' },
  { field: '_version', type: 'BIGINT', description: 'Record version for conflict resolution' },
  { field: '_hash', type: 'VARCHAR', description: 'SHA256 hash for dedup detection' },
  { field: '_deleted', type: 'BOOLEAN', description: 'Soft delete flag' },
  { field: '_created_at', type: 'TIMESTAMP', description: 'Record creation time in DW' },
  { field: '_updated_at', type: 'TIMESTAMP', description: 'Last update time in DW' },
];

export default function MappingFieldsPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [registry, setRegistry] = useState<TableRegistry | null>(null);
  const [rules, setRules] = useState<MappingRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [newFields, setNewFields] = useState<string[]>([]);
  const [modalVisible, setModalVisible] = useState(false);
  const [modalInitial, setModalInitial] = useState({ source_table: '', source_field: '' });
  const [togglingId, setTogglingId] = useState<number | null>(null);
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const [batchLoading, setBatchLoading] = useState(false);
  const [previewRule, setPreviewRule] = useState<MappingRule | null>(null);
  const [previewPath, setPreviewPath] = useState('');
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewResult, setPreviewResult] = useState<Array<{ source_id: string; extracted?: unknown; violation?: string }>>([]);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const openPreview = (rule: MappingRule) => {
    setPreviewRule(rule);
    setPreviewPath(`after.${rule.source_field}`);
    setPreviewResult([]);
    setPreviewError(null);
  };

  const runPreview = async () => {
    if (!registry || !previewRule) return;
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const { data: res } = await cmsApi.post('/api/v1/mapping-rules/preview', {
        shadow_table: registry.target_table,
        jsonpath: previewPath,
        sample_limit: 3,
      });
      setPreviewResult(res.data || []);
    } catch (err: any) {
      setPreviewError(err.response?.data?.detail || err.response?.data?.error || 'Preview failed');
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleBatchUpdate = async (status: string) => {
    if (selectedRowKeys.length === 0) {
      message.warning('Chọn field trước');
      return;
    }
    setBatchLoading(true);
    try {
      await cmsApi.patch('/api/mapping-rules/batch', {
        ids: selectedRowKeys.map(Number),
        status,
      });
      message.success(`${selectedRowKeys.length} field đã ${status === 'approved' ? 'duyệt' : 'từ chối'}`);
      setSelectedRowKeys([]);
      fetchRules();
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Cập nhật thất bại');
    } finally {
      setBatchLoading(false);
    }
  };

  const [syncFieldsLoading, setSyncFieldsLoading] = useState(false);
  const handleSyncFields = async () => {
    if (!id) return;
    setSyncFieldsLoading(true);
    try {
      await cmsApi.post(`/api/registry/${id}/create-default-columns`);
      message.success('Đang cập nhật field vào table đích...');
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Cập nhật field thất bại');
    } finally {
      setSyncFieldsLoading(false);
    }
  };

  const fetchRegistry = useCallback(async () => {
    try {
      const { data: res } = await cmsApi.get('/api/registry');
      const all: TableRegistry[] = res.data || [];
      const entry = all.find((r: TableRegistry) => r.id === Number(id));
      if (entry) setRegistry(entry);
    } catch { /* ignore */ }
  }, [id]);

  const fetchRules = useCallback(async () => {
    if (!registry) return;
    setLoading(true);
    try {
      const { data: res } = await cmsApi.get('/api/mapping-rules', {
        params: { table: registry.source_table, page_size: 200 }
      });
      setRules(res.data || []);
    } catch {
      message.error('Failed to fetch mapping rules');
    } finally {
      setLoading(false);
    }
  }, [registry]);

  useEffect(() => { fetchRegistry(); }, [fetchRegistry]);
  useEffect(() => { if (registry) fetchRules(); }, [registry, fetchRules]);

  const handleToggleActive = async (rule: MappingRule) => {
    setTogglingId(rule.id);
    try {
      await cmsApi.patch(`/api/mapping-rules/${rule.id}`, {
        status: rule.is_active ? 'approved' : 'approved',
      });
      // Optimistic update
      setRules(prev => prev.map(r => r.id === rule.id ? { ...r, is_active: !r.is_active } : r));
      message.success(`Mapping ${rule.source_field} → ${rule.is_active ? 'deactivated' : 'activated'}`);
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Failed to toggle mapping');
    } finally {
      setTogglingId(null);
    }
  };

  const handleScan = async () => {
    if (!registry) return;
    setScanning(true);
    try {

      const { data: res } = await cmsApi.get(`/api/introspection/scan-raw/${registry.target_table}`);
      setNewFields(res.new_fields || []);
      if ((res.new_fields || []).length === 0) {
        message.info('All fields in _raw_data are already mapped.');
      } else {
        message.success(`Found ${res.new_fields.length} unmapped fields in _raw_data!`);
      }
    } catch (err: any) {

      try {
        const { data: res } = await cmsApi.get(`/api/introspection/scan/${registry.target_table}`);
        setNewFields(res.new_fields || []);
        if ((res.new_fields || []).length === 0) {
          message.info('All fields are mapped.');
        } else {
          message.success(`Found ${res.new_fields.length} unmapped fields via Airbyte!`);
        }
      } catch (err2: any) {
        message.error(err2.response?.data?.error || 'Scan failed — both _raw_data and Airbyte methods failed');
      }
    } finally {
      setScanning(false);
    }
  };

  const handleBackfill = async (ruleId: number) => {
    try {
      await cmsApi.post(`/api/mapping-rules/${ruleId}/backfill`);
      message.success('Backfill command submitted');
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Backfill failed');
    }
  };

  const handleReload = async () => {
    try {
      await cmsApi.post('/api/mapping-rules/reload', null, {
        params: { table: registry?.source_table }
      });
      message.success('Reload signal sent to workers');
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Reload failed');
    }
  };

  const handleAddNew = (field?: string) => {
    if (!registry) return;
    setModalInitial({ source_table: registry.source_table, source_field: field || '' });
    setModalVisible(true);
  };

  const columns: ColumnsType<MappingRule> = [
    {
      title: 'Source Field',
      dataIndex: 'source_field',
      key: 'source_field',
      render: (text) => <code>{text}</code>,
      sorter: (a, b) => a.source_field.localeCompare(b.source_field),
    },
    {
      title: 'Target Column',
      dataIndex: 'target_column',
      key: 'target_column',
      render: (text) => <b>{text}</b>,
    },
    {
      title: 'Data Type',
      dataIndex: 'data_type',
      key: 'data_type',
      render: (text) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: 'Rule Type',
      dataIndex: 'rule_type',
      key: 'rule_type',
      render: (type: string) => {
        const colors: Record<string, string> = { system: 'default', discovered: 'cyan', mapping: 'purple' };
        return <Tag color={colors[type] || 'default'}>{type}</Tag>;
      },
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => {
        const colors: Record<string, string> = { approved: 'green', pending: 'orange', rejected: 'red' };
        return <Tag color={colors[status] || 'default'}>{status}</Tag>;
      },
    },
    {
      title: 'Active',
      dataIndex: 'is_active',
      key: 'is_active',
      render: (active: boolean, record) => (
        <Switch
          checked={active}
          loading={togglingId === record.id}
          onChange={() => handleToggleActive(record)}
          size="small"
        />
      ),
    },
    {
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space size={4}>
          <Button size="small" type="link" icon={<EyeOutlined />} onClick={() => openPreview(record)}>
            Preview
          </Button>
          <Button size="small" type="link" onClick={() => handleBackfill(record.id)}>
            Backfill
          </Button>
        </Space>
      ),
    },
  ];

  if (!registry) {
    return <div style={{ textAlign: 'center', padding: 100 }}><Spin size="large" tip="Loading registry..." /></div>;
  }

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/registry')}>Back to Registry</Button>
      </Space>

      <Title level={4}>
        Mapping Fields: <code>{registry.source_table}</code> → <code>{registry.target_table}</code>
      </Title>

      {/* Table Info */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Descriptions size="small" column={4}>
          <Descriptions.Item label="Source DB">{registry.source_db}</Descriptions.Item>
          <Descriptions.Item label="Source Type"><Tag>{registry.source_type}</Tag></Descriptions.Item>
          <Descriptions.Item label="Sync Engine"><Tag color="blue">{registry.sync_engine}</Tag></Descriptions.Item>
          <Descriptions.Item label="Priority"><Tag color={registry.priority === 'critical' ? 'red' : 'default'}>{registry.priority}</Tag></Descriptions.Item>
          <Descriptions.Item label="PK Field"><code>{registry.primary_key_field}</code></Descriptions.Item>
          <Descriptions.Item label="Active">{registry.is_active ? <Tag color="green">Yes</Tag> : <Tag color="red">No</Tag>}</Descriptions.Item>
        </Descriptions>
      </Card>

      {/* System Default Fields */}
      <Card title="System Default Fields (auto-created)" size="small" style={{ marginBottom: 16 }}>
        <Row gutter={[8, 8]}>
          {SYSTEM_DEFAULT_FIELDS.map(f => (
            <Col key={f.field} span={6}>
              <Tooltip title={f.description}>
                <Tag style={{ width: '100%', textAlign: 'center', padding: '4px 8px' }}>
                  <code>{f.field}</code> <Text type="secondary" style={{ fontSize: 11 }}>({f.type})</Text>
                </Tag>
              </Tooltip>
            </Col>
          ))}
        </Row>
      </Card>

      {/* Actions */}
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Text strong>Custom Mapping Rules ({rules.length})</Text>
        <Space>
          <Button type="primary" icon={<PlusOutlined />} size="small" onClick={() => handleAddNew()}>Add Mapping</Button>
          <Button icon={<SearchOutlined />} size="small" loading={scanning} onClick={handleScan}>Scan Unmapped Fields</Button>
          <Button icon={<ReloadOutlined />} size="small" onClick={handleReload}>Reload Workers</Button>
        </Space>
      </Space>

      {/* Unmapped fields alert */}
      {newFields.length > 0 && (
        <Alert
          type="warning"
          showIcon
          message={`${newFields.length} unmapped fields found in _raw_data`}
          description={
            <Space wrap style={{ marginTop: 8 }}>
              {newFields.map(f => (
                <Tooltip title={`Add mapping for ${f}`} key={f}>
                  <Tag color="orange" style={{ cursor: 'pointer' }} onClick={() => handleAddNew(f)}>
                    {f} +
                  </Tag>
                </Tooltip>
              ))}
            </Space>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      {/* Action Bar */}
      <Space style={{ marginBottom: 12 }}>
        <Button type="primary" icon={<SyncOutlined />} loading={syncFieldsLoading}
          onClick={handleSyncFields}>Cập nhật Field vào Table</Button>
        {selectedRowKeys.length > 0 && (
          <>
            <Text strong>{selectedRowKeys.length} đã chọn</Text>
            <Button type="primary" icon={<CheckCircleOutlined />} loading={batchLoading}
              onClick={() => handleBatchUpdate('approved')}>Duyệt</Button>
            <Button danger icon={<CloseCircleOutlined />} loading={batchLoading}
              onClick={() => handleBatchUpdate('rejected')}>Từ chối</Button>
            <Button onClick={() => setSelectedRowKeys([])}>Bỏ chọn</Button>
          </>
        )}
      </Space>

      {/* Mapping Rules Table */}
      <Table
        columns={columns}
        dataSource={rules}
        rowKey="id"
        loading={loading}
        pagination={false}
        size="small"
        rowSelection={{
          selectedRowKeys,
          onChange: setSelectedRowKeys,
        }}
        locale={{ emptyText: 'No mapping rules. Data is stored in _raw_data column only.' }}
      />

      <AddMappingModal
        visible={modalVisible}
        onCancel={() => setModalVisible(false)}
        onSuccess={() => {
          setModalVisible(false);
          fetchRules();
        }}
        initialValues={modalInitial}
      />

      <Modal
        open={!!previewRule}
        title={previewRule ? `Preview JsonPath — ${previewRule.source_field} → ${previewRule.target_column}` : ''}
        onCancel={() => { setPreviewRule(null); setPreviewResult([]); setPreviewError(null); }}
        onOk={runPreview}
        okText="Run Preview"
        confirmLoading={previewLoading}
        cancelText="Close"
        width={720}
      >
        <Space direction="vertical" style={{ width: '100%' }} size={12}>
          <div>
            <Text type="secondary">Shadow table: </Text>
            <Text code>{registry.target_table}</Text>
          </div>
          <Input
            placeholder="JsonPath (gjson syntax) — e.g. after.amount, after.user._id"
            value={previewPath}
            onChange={(e) => setPreviewPath(e.target.value)}
            prefix={<EyeOutlined />}
          />
          {previewError && <Alert type="error" message={previewError} showIcon />}
          {previewResult.length > 0 && (
            <div>
              <Text strong>3 sample rows:</Text>
              <pre style={{ background: '#fafafa', padding: 12, marginTop: 8, maxHeight: 360, overflow: 'auto', fontSize: 12 }}>
                {JSON.stringify(previewResult, null, 2)}
              </pre>
            </div>
          )}
          {!previewLoading && previewResult.length === 0 && !previewError && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              Nhấn "Run Preview" để gọi <code>POST /api/v1/mapping-rules/preview</code> với gjson engine lấy 3 sample từ shadow table.
            </Text>
          )}
        </Space>
      </Modal>
    </div>
  );
}
