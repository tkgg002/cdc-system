import { useEffect, useState } from 'react';
import { Row, Col, Card, Statistic, Spin, Tag, Space } from 'antd';
import {
  DatabaseOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  SyncOutlined,
  AlertOutlined,
} from '@ant-design/icons';
import { cmsApi } from '../services/api';
import type { RegistryStats } from '../types';

interface SyncHealth {
  total_streams: number;
  registered: number;
  unregistered: number;
  mismatched: number;
  pending_rules: number;
  transform_pending_rows: number;
  last_scan?: string;
}

export default function Dashboard() {
  const [stats, setStats] = useState<RegistryStats | null>(null);
  const [pendingCount, setPendingCount] = useState(0);
  const [syncHealth, setSyncHealth] = useState<SyncHealth | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [statsRes, pendingRes, healthRes] = await Promise.all([
          cmsApi.get('/api/registry/stats'),
          cmsApi.get('/api/schema-changes/pending?status=pending&page_size=1'),
          cmsApi.get('/api/sync/health').catch(() => ({ data: null })),
        ]);
        setStats(statsRes.data);
        setPendingCount(pendingRes.data.total || 0);
        if (healthRes.data) setSyncHealth(healthRes.data);
      } catch {
        // handled by interceptor
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return <Spin size="large" style={{ display: 'block', margin: '100px auto' }} />;

  return (
    <div>
      <Row gutter={[16, 16]}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Registered Tables" value={stats?.total || 0} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Tables Created" value={stats?.tables_created || 0} prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Pending Changes" value={pendingCount} prefix={<ClockCircleOutlined />} valueStyle={{ color: '#faad14' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Source DBs" value={stats ? Object.keys(stats.by_source_db).length : 0} prefix={<WarningOutlined />} />
          </Card>
        </Col>
      </Row>

      {syncHealth && (
        <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
          <Col xs={24}>
            <Card title={<Space><SyncOutlined spin={false} /> Sync Health</Space>} size="small">
              <Row gutter={16}>
                <Col span={4}>
                  <Statistic title="Registered" value={syncHealth.registered} valueStyle={{ color: '#3f8600' }} />
                </Col>
                <Col span={4}>
                  <Statistic title="Unregistered" value={syncHealth.unregistered}
                    valueStyle={{ color: syncHealth.unregistered > 0 ? '#cf1322' : '#3f8600' }} />
                </Col>
                <Col span={4}>
                  <Statistic title="Mismatched" value={syncHealth.mismatched} prefix={syncHealth.mismatched > 0 ? <AlertOutlined /> : undefined}
                    valueStyle={{ color: syncHealth.mismatched > 0 ? '#faad14' : '#3f8600' }} />
                </Col>
                <Col span={4}>
                  <Statistic title="Pending Rules" value={syncHealth.pending_rules}
                    valueStyle={{ color: syncHealth.pending_rules > 0 ? '#faad14' : '#3f8600' }} />
                </Col>
                <Col span={4}>
                  <Statistic title="Transform Pending" value={syncHealth.transform_pending_rows?.toLocaleString() || 0} />
                </Col>
                <Col span={4}>
                  {syncHealth.mismatched === 0 && syncHealth.unregistered === 0
                    ? <Tag color="success" style={{ marginTop: 24, fontSize: 14 }}>All Healthy</Tag>
                    : <Tag color="warning" style={{ marginTop: 24, fontSize: 14 }}>Needs Attention</Tag>
                  }
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>
      )}

      <Row gutter={[16, 16]} style={{ marginTop: 16 }}>
        <Col xs={24} sm={8}>
          <Card title="By Sync Engine" size="small">
            {stats && Object.entries(stats.by_sync_engine).map(([k, v]) => (
              <p key={k}><strong>{k}:</strong> {v}</p>
            ))}
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card title="By Priority" size="small">
            {stats && Object.entries(stats.by_priority).map(([k, v]) => (
              <p key={k}><strong>{k}:</strong> {v}</p>
            ))}
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card title="By Source DB" size="small">
            {stats && Object.entries(stats.by_source_db).map(([k, v]) => (
              <p key={k}><strong>{k}:</strong> {v}</p>
            ))}
          </Card>
        </Col>
      </Row>
    </div>
  );
}
