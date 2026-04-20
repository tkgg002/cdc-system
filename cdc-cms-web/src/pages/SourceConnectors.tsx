import { useState, useEffect } from 'react';
import { Table, Card, Typography, Space, Button, message, Tag } from 'antd';
import { ReloadOutlined, DatabaseOutlined } from '@ant-design/icons';
import axios from 'axios';

const { Title, Text } = Typography;

interface AirbyteSource {
  sourceId: string;
  name: string;
  sourceName: string;
  workspaceId: string;
  database: string;
}

export default function SourceConnectors() {
  const [loading, setLoading] = useState(false);
  const [sources, setSources] = useState<AirbyteSource[]>([]);

  const fetchSources = async () => {
    setLoading(true);
    try {
      const token = localStorage.getItem('access_token');
      const resp = await axios.get('http://localhost:8083/api/airbyte/sources', {
        headers: { Authorization: `Bearer ${token}` }
      });
      setSources(resp.data);
    } catch (err: any) {
      console.error(err);
      message.error('Failed to fetch Airbyte sources: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSources();
  }, []);

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      render: (text: string) => <Space><DatabaseOutlined /> <Text strong>{text}</Text></Space>
    },
    {
      title: 'Type',
      dataIndex: 'sourceName',
      key: 'sourceName',
      render: (text: string) => <Tag color="blue">{text}</Tag>
    },
    {
      title: 'Database',
      dataIndex: 'database',
      key: 'database',
      render: (text: string) => <Text>{text || 'N/A'}</Text>
    },
    {
      title: 'ID',
      dataIndex: 'sourceId',
      key: 'sourceId',
      render: (text: string) => <Text>{text}</Text>
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: AirbyteSource) => (
        <Button size="small" onClick={() => window.open(`http://localhost:18000/workspaces/${record.workspaceId}/source/${record.sourceId}`, '_blank')}>
          View in Airbyte
        </Button>
      )
    }
  ];

  return (
    <Card bordered={false}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <Title level={4}>Airbyte Source Connectors</Title>
        <Button icon={<ReloadOutlined />} onClick={fetchSources} loading={loading}>
          Refresh
        </Button>
      </div>

      <Table
        dataSource={sources}
        columns={columns}
        rowKey="sourceId"
        loading={loading}
        pagination={false}
      />
    </Card>
  );
}
