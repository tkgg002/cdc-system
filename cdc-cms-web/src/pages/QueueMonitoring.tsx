import { useEffect, useState } from 'react';
import { Row, Col, Card, Statistic, Progress, Spin, Typography, Space, Tag, List, Alert } from 'antd';
import { SyncOutlined, CheckCircleOutlined, ExclamationCircleOutlined, DashboardOutlined, DatabaseOutlined, TeamOutlined } from '@ant-design/icons';
import { cmsApi, workerApi } from '../services/api';

const { Title, Text } = Typography;

interface WorkerStats {
  queue: {
    processed: number;
    failed: number;
    active_workers: number;
    pool_size: number;
  };
  buffer: {
    buffer_size: number;
    max_size: number;
    last_flush: string;
  };
  config: {
    pool_size: number;
    batch_size: number;
  };
}

interface AirbyteJob {
  jobId: number;
  status: string;
  createdAt: string;
  updatedAt: string;
}

// Safe accessors to prevent crashes on unexpected data shapes
function safeNum(val: any): number {
  return typeof val === 'number' ? val : 0;
}

function safeStr(val: any): string {
  return typeof val === 'string' ? val : '';
}

export default function QueueMonitoring() {
  const [stats, setStats] = useState<WorkerStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [airbyteJobs, setAirbyteJobs] = useState<AirbyteJob[]>([]);

  const fetchStats = async () => {
    try {
      const { data } = await workerApi.get('/api/v1/internal/stats');
      if (data && typeof data === 'object') {
        setStats(data);
        setError(null);
      }
    } catch (err: any) {
      setError(err.message || 'Worker API disconnected');
    } finally {
      setLoading(false);
    }
  };

  const fetchAirbyteJobs = async () => {
    try {
      const { data } = await cmsApi.get('/api/airbyte/jobs');
      if (Array.isArray(data)) {
        setAirbyteJobs(data);
      }
    } catch { /* Silent fail for monitoring */ }
  };

  useEffect(() => {
    fetchStats();
    fetchAirbyteJobs();
    const timer = setInterval(() => {
      fetchStats();
      fetchAirbyteJobs();
    }, 5000);
    return () => clearInterval(timer);
  }, []);

  if (loading && !stats) {
    return <div style={{ textAlign: 'center', padding: '100px' }}><Spin size="large" tip="Connecting to Worker..." /></div>;
  }

  const processed = safeNum(stats?.queue?.processed);
  const failed = safeNum(stats?.queue?.failed);
  const activeWorkers = safeNum(stats?.queue?.active_workers);
  const poolSize = safeNum(stats?.queue?.pool_size || stats?.config?.pool_size);
  const bufferSize = safeNum(stats?.buffer?.buffer_size);
  const maxSize = safeNum(stats?.buffer?.max_size || stats?.config?.batch_size);
  const lastFlush = safeStr(stats?.buffer?.last_flush);

  const workerActivePercent = poolSize > 0 ? Math.round((activeWorkers / poolSize) * 100) : 0;
  const bufferFullPercent = maxSize > 0 ? Math.round((bufferSize / maxSize) * 100) : 0;

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>CDC Worker Monitoring <Tag color={error ? 'error' : 'success'}>{error ? 'Offline' : 'Healthy'}</Tag></Title>

      {error && (
        <Alert
          message="Worker Connection Error"
          description={error}
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        <Col span={8}>
          <Card hoverable>
            <Statistic
              title="Processed Events"
              value={processed}
              prefix={<CheckCircleOutlined style={{ color: '#52c41a' }} />}
              suffix="msgs"
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card hoverable>
            <Statistic
              title="Failed Events"
              value={failed}
              prefix={<ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />}
            />
          </Card>
        </Col>
        <Col span={8}>
          <Card hoverable>
            <Statistic
              title="Active Progress"
              value={workerActivePercent}
              suffix="%"
              prefix={<DashboardOutlined />}
            />
            <Progress percent={workerActivePercent} showInfo={false} strokeColor="#1890ff" />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col span={12}>
          <Card title={<Space><TeamOutlined /> Worker Pool Status</Space>}>
            <Row align="middle" gutter={24}>
              <Col span={12}>
                <Progress
                   type="dashboard"
                   percent={workerActivePercent}
                   format={() => `${activeWorkers}/${poolSize}`}
                />
              </Col>
              <Col span={12}>
                <List size="small">
                   <List.Item><Text strong>Workers Active:</Text> {activeWorkers}</List.Item>
                   <List.Item><Text strong>Total Capacity:</Text> {poolSize}</List.Item>
                   <List.Item><Text strong>Processing State:</Text> <Tag color="blue">JetStream Pull</Tag></List.Item>
                </List>
              </Col>
            </Row>
          </Card>
        </Col>
        <Col span={12}>
          <Card title={<Space><DatabaseOutlined /> Batch Buffer (DW Persistence)</Space>}>
             <Row align="middle" gutter={24}>
              <Col span={12}>
                <Progress
                   type="dashboard"
                   percent={bufferFullPercent}
                   format={() => `${bufferSize}`}
                   strokeColor={bufferFullPercent > 80 ? '#fb8c00' : '#52c41a'}
                />
              </Col>
              <Col span={12}>
                <List size="small">
                   <List.Item><Text strong>Buffer Records:</Text> {bufferSize}</List.Item>
                   <List.Item><Text strong>Max Batch Size:</Text> {maxSize}</List.Item>
                   <List.Item><Text strong>Last Flush:</Text> {lastFlush ? new Date(lastFlush).toLocaleTimeString() : 'N/A'}</List.Item>
                </List>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      <Row style={{ marginTop: 16 }}>
        <Col span={24}>
           <Card title={<Space><SyncOutlined /> Airbyte Raw Data Sync Status (Extraction Layer)</Space>}>
             {airbyteJobs.length === 0 ? (
               <Text type="secondary">No sync jobs found</Text>
             ) : (
               <List
                  grid={{ gutter: 16, column: 4 }}
                  dataSource={airbyteJobs}
                  renderItem={(job) => (
                    <List.Item>
                      <Card size="small">
                         <Space direction="vertical" style={{ width: '100%' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%' }}>
                               <Text strong>Job #{job.jobId}</Text>
                               <Tag color={
                                 job.status === 'succeeded' ? 'success' :
                                 (job.status === 'running' ? 'processing' : 'error')
                               }>
                                 {(job.status || 'unknown').toUpperCase()}
                               </Tag>
                            </div>
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                               Last Update: {job.updatedAt ? new Date(job.updatedAt).toLocaleTimeString() : 'N/A'}
                            </Text>
                         </Space>
                      </Card>
                    </List.Item>
                  )}
               />
             )}
           </Card>
        </Col>
      </Row>

      <div style={{ marginTop: 24, textAlign: 'center' }}>
        <Text type="secondary"><SyncOutlined spin /> Auto-refreshing every 5 seconds</Text>
      </div>
    </div>
  );
}
