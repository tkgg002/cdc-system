import { useEffect, useState } from 'react';
import { Table, Tag, message, Spin, Button, Space, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { cmsApi } from '../services/api';
import type { MappingRule } from '../types';
import AddMappingModal from './AddMappingModal';

interface Props {
  sourceTable: string;
}

export default function MappingRuleList({ sourceTable }: Props) {
  const [data, setData] = useState<MappingRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [newFields, setNewFields] = useState<string[]>([]);
  
  const [modalVisible, setModalVisible] = useState(false);
  const [modalInitial, setModalInitial] = useState({ source_table: '', source_field: '' });

  const fetchData = async () => {
    setLoading(true);
    try {
      const { data: res } = await cmsApi.get('/api/mapping-rules', {
        params: { table: sourceTable }
      });
      setData(res.data || []);
    } catch (err) {
      message.error('Failed to fetch mapping rules');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [sourceTable]);

  const handleScan = async () => {
    setScanning(true);
    try {
      const { data: res } = await cmsApi.get(`/api/introspection/scan/${sourceTable}`);
      setNewFields(res.new_fields || []);
      if ((res.new_fields || []).length === 0) {
        message.info('All fields in _raw_data are already mapped.');
      } else {
        message.success(`Found ${res.new_fields.length} new fields!`);
      }
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Scan failed');
    } finally {
      setScanning(false);
    }
  };

  const handleBackfill = async (ruleId: number) => {
    try {
      await cmsApi.post(`/api/mapping-rules/${ruleId}/backfill`);
      message.success(`Backfill command submitted. Check worker logs for progress.`);
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Backfill failed');
    }
  };

  const columns: ColumnsType<MappingRule> = [
    { title: 'Source Field', dataIndex: 'source_field', key: 'source_field', render: (text) => <code>{text}</code> },
    { title: 'Target Column', dataIndex: 'target_column', key: 'target_column', render: (text) => <b>{text}</b> },
    { title: 'Data Type', dataIndex: 'data_type', key: 'data_type', render: (text) => <Tag color="blue">{text}</Tag> },
    { 
      title: 'Status', 
      dataIndex: 'is_active', 
      key: 'is_active',
      render: (active: boolean) => active ? <Tag color="green">Active</Tag> : <Tag color="red">Inactive</Tag>
    },
    { 
      title: 'Enriched', 
      dataIndex: 'is_enriched', 
      key: 'is_enriched',
      render: (v: boolean) => v ? <Tag color="purple">Yes</Tag> : null
    },
    {
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Button size="small" type="link" onClick={() => handleBackfill(record.id)}>
          Backfill
        </Button>
      ),
    },
  ];

  const handleAddNew = (field?: string) => {
    setModalInitial({ source_table: sourceTable, source_field: field || '' });
    setModalVisible(true);
  };

  const handleReload = async () => {
    try {
      await cmsApi.post('/api/mapping-rules/reload', null, { params: { table: sourceTable } });
      message.success('Reload signal sent to workers');
    } catch (err: any) {
      message.error(err.response?.data?.error || 'Reload failed');
    }
  };

  if (loading) return <div style={{ padding: 20, textAlign: 'center' }}><Spin tip="Loading mappings..." /></div>;

  return (
    <div style={{ padding: '10px 40px', background: '#fafafa' }}>
      <Space style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
        <h4>Field Mappings for <code>{sourceTable}</code></h4>
        <Space>
          <Button type="primary" size="small" onClick={() => handleAddNew()}>Add Mapping</Button>
          <Button size="small" onClick={handleReload}>Reload Workers</Button>
          <Button type="dashed" size="small" loading={scanning} onClick={handleScan}>Scan for unmapped fields</Button>
        </Space>
      </Space>

      <Table 
        columns={columns} 
        dataSource={data} 
        rowKey="id" 
        pagination={false} 
        size="small"
        locale={{ emptyText: 'No mapping rules found for this table. Data will be stored in _raw_data column.' }}
      />

      {newFields.length > 0 && (
        <div style={{ marginTop: 16, padding: 12, border: '1px dashed #ffa39e', borderRadius: 8, background: '#fff2f0' }}>
          <h5>New fields found in <code>_raw_data</code>:</h5>
          <p style={{ fontSize: '12px', color: '#666', marginBottom: 8 }}>Click a field to add a new mapping rule.</p>
          <Space wrap>
            {newFields.map(f => (
              <Tooltip title={`Add mapping for ${f}`} key={f}>
                <Tag color="red" style={{ cursor: 'pointer' }} onClick={() => handleAddNew(f)}>
                  {f} +
                </Tag>
              </Tooltip>
            ))}
          </Space>
        </div>
      )}

      <AddMappingModal
        visible={modalVisible}
        onCancel={() => setModalVisible(false)}
        onSuccess={() => {
          setModalVisible(false);
          fetchData();
        }}
        initialValues={modalInitial}
      />
    </div>
  );
}
