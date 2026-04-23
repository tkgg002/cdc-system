import { lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate, Link, useNavigate } from 'react-router-dom';
import { Layout, Menu, Button, Typography, Spin } from 'antd';
import {
  DashboardOutlined,
  DatabaseOutlined,
  BranchesOutlined,
  SettingOutlined,
  LogoutOutlined,
} from '@ant-design/icons';
import QueryErrorBoundary from './components/QueryErrorBoundary';

// Lazy-loaded pages → each route becomes its own chunk (code-split per route)
const Login = lazy(() => import('./pages/Login'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const SchemaChanges = lazy(() => import('./pages/SchemaChanges'));
const TableRegistry = lazy(() => import('./pages/TableRegistry'));
const CDCInternalRegistry = lazy(() => import('./pages/CDCInternalRegistry'));
const MasterRegistry = lazy(() => import('./pages/MasterRegistry'));
const SchemaProposals = lazy(() => import('./pages/SchemaProposals'));
const TransmuteSchedules = lazy(() => import('./pages/TransmuteSchedules'));
const MappingFieldsPage = lazy(() => import('./pages/MappingFieldsPage'));
const SourceConnectors = lazy(() => import('./pages/SourceConnectors'));
const QueueMonitoring = lazy(() => import('./pages/QueueMonitoring'));
const ActivityLog = lazy(() => import('./pages/ActivityLog'));
const ActivityManager = lazy(() => import('./pages/ActivityManager'));
const DataIntegrity = lazy(() => import('./pages/DataIntegrity'));
const SystemHealth = lazy(() => import('./pages/SystemHealth'));

const { Header, Sider, Content } = Layout;
const { Text } = Typography;

function LoadingSpinner() {
  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', padding: 50, minHeight: 200 }}>
      <Spin size="large" tip="Đang tải..." />
    </div>
  );
}

function isLoggedIn() {
  return !!localStorage.getItem('access_token');
}

function getUser() {
  const raw = localStorage.getItem('user');
  return raw ? JSON.parse(raw) : null;
}

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  if (!isLoggedIn()) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function AppLayout() {
  const navigate = useNavigate();
  const user = getUser();

  const logout = () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    localStorage.removeItem('user');
    navigate('/login');
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={220} theme="dark">
        <div style={{ padding: '16px', textAlign: 'center' }}>
          <Text strong style={{ color: '#fff', fontSize: 16 }}>CDC Management</Text>
        </div>
        <Menu theme="dark" mode="inline" defaultSelectedKeys={['dashboard']}>
          <Menu.Item key="dashboard" icon={<DashboardOutlined />}>
            <Link to="/">Dashboard</Link>
          </Menu.Item>
          <Menu.Item key="schema" icon={<BranchesOutlined />}>
            <Link to="/schema-changes">Mapping Approval</Link>
          </Menu.Item>
          <Menu.Item key="registry" icon={<DatabaseOutlined />}>
            <Link to="/registry">Table Registry</Link>
          </Menu.Item>
          <Menu.Item key="cdc-internal" icon={<DatabaseOutlined />}>
            <Link to="/cdc-internal">Shadow Registry (v1.25)</Link>
          </Menu.Item>
          <Menu.Item key="masters" icon={<DatabaseOutlined />}>
            <Link to="/masters">Master Registry</Link>
          </Menu.Item>
          <Menu.Item key="proposals" icon={<BranchesOutlined />}>
            <Link to="/schema-proposals">Schema Proposals</Link>
          </Menu.Item>
          <Menu.Item key="schedules" icon={<SettingOutlined />}>
            <Link to="/schedules">Transmute Schedules</Link>
          </Menu.Item>
          <Menu.Item key="sources" icon={<SettingOutlined />}>
            <Link to="/sources">Debezium Command Center</Link>
          </Menu.Item>
          <Menu.Item key="queue" icon={<DashboardOutlined />}>
            <Link to="/queue">Queue Monitor</Link>
          </Menu.Item>
          <Menu.Item key="activity" icon={<BranchesOutlined />}>
            <Link to="/activity-log">Activity Log</Link>
          </Menu.Item>
          <Menu.Item key="manager" icon={<SettingOutlined />}>
            <Link to="/activity-manager">Quản lý tác vụ</Link>
          </Menu.Item>
          <Menu.Item key="integrity" icon={<DatabaseOutlined />}>
            <Link to="/data-integrity">Toàn vẹn dữ liệu</Link>
          </Menu.Item>
          <Menu.Item key="health" icon={<DashboardOutlined />}>
            <Link to="/system-health">Sức khỏe hệ thống</Link>
          </Menu.Item>
        </Menu>
      </Sider>
      <Layout>
        <Header style={{ background: '#fff', padding: '0 24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Text>{user?.full_name || user?.username} ({user?.role})</Text>
          <Button icon={<LogoutOutlined />} onClick={logout}>Logout</Button>
        </Header>
        <Content style={{ margin: 16, padding: 24, background: '#fff', borderRadius: 8 }}>
          <QueryErrorBoundary>
            <Suspense fallback={<LoadingSpinner />}>
              <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/schema-changes" element={<SchemaChanges />} />
                <Route path="/registry" element={<TableRegistry />} />
                <Route path="/cdc-internal" element={<CDCInternalRegistry />} />
                <Route path="/masters" element={<MasterRegistry />} />
                <Route path="/schema-proposals" element={<SchemaProposals />} />
                <Route path="/schedules" element={<TransmuteSchedules />} />
                <Route path="/registry/:id/mappings" element={<MappingFieldsPage />} />
                <Route path="/sources" element={<SourceConnectors />} />
                <Route path="/queue" element={<QueueMonitoring />} />
                <Route path="/activity-log" element={<ActivityLog />} />
                <Route path="/activity-manager" element={<ActivityManager />} />
                <Route path="/data-integrity" element={<DataIntegrity />} />
                <Route path="/system-health" element={<SystemHealth />} />
              </Routes>
            </Suspense>
          </QueryErrorBoundary>
        </Content>
      </Layout>
    </Layout>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Suspense fallback={<LoadingSpinner />}>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/*" element={
            <ProtectedRoute>
              <AppLayout />
            </ProtectedRoute>
          } />
        </Routes>
      </Suspense>
    </BrowserRouter>
  );
}
