import { lazy, Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate, Link, useLocation, useNavigate } from 'react-router-dom';
import { Layout, Menu, Button, Typography, Spin } from 'antd';
import type { MenuProps } from 'antd';
import {
  DashboardOutlined,
  DatabaseOutlined,
  BranchesOutlined,
  SettingOutlined,
  LogoutOutlined,
  CompassOutlined,
  ThunderboltOutlined
} from '@ant-design/icons';
import QueryErrorBoundary from './components/QueryErrorBoundary';

// Lazy-loaded pages → each route becomes its own chunk (code-split per route)
const Login = lazy(() => import('./pages/Login'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const SchemaChanges = lazy(() => import('./pages/SchemaChanges'));
const TableRegistry = lazy(() => import('./pages/TableRegistry'));
const MasterRegistry = lazy(() => import('./pages/MasterRegistry'));
const SchemaProposals = lazy(() => import('./pages/SchemaProposals'));
const TransmuteSchedules = lazy(() => import('./pages/TransmuteSchedules'));
const MappingFieldsPage = lazy(() => import('./pages/MappingFieldsPage'));
const SourceConnectors = lazy(() => import('./pages/SourceConnectors'));
const ActivityLog = lazy(() => import('./pages/ActivityLog'));
const ActivityManager = lazy(() => import('./pages/ActivityManager'));
const DataIntegrity = lazy(() => import('./pages/DataIntegrity'));
const SystemHealth = lazy(() => import('./pages/SystemHealth'));
const SourceToMasterWizard = lazy(() => import('./pages/SourceToMasterWizard'));

const { Header, Sider, Content } = Layout;
const { Text } = Typography;

function LoadingSpinner() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', gap: 12, padding: 50, minHeight: 200 }}>
      <Spin size="large" />
      <Text type="secondary">Đang tải...</Text>
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
  const location = useLocation();
  const user = getUser();

  const logout = () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    localStorage.removeItem('user');
    navigate('/login');
  };

  const menuItems: MenuProps['items'] = [
    {
      key: '/',
      icon: <DashboardOutlined />,
      label: <Link to="/">Dashboard</Link>,
    },
    {
      key: 'setup',
      icon: <CompassOutlined />,
      label: 'Setup',
      children: [
        {
          key: '/source-to-master',
          icon: <CompassOutlined />,
          label: <Link to="/source-to-master">Source → Master Wizard</Link>,
        },
        {
          key: '/sources',
          icon: <SettingOutlined />,
          label: <Link to="/sources">Sources & Connectors</Link>,
        },
        {
          key: '/shadow',
          icon: <DatabaseOutlined />,
          label: <Link to="/shadow">Shadow</Link>,
        },
        {
          key: '/masters',
          icon: <DatabaseOutlined />,
          label: <Link to="/masters">Master Registry</Link>,
        },
      ],
    },
    {
      key: 'operate',
      icon: <ThunderboltOutlined />,
      label: 'Operate',
      children: [
        {
          key: '/schema-proposals',
          icon: <BranchesOutlined />,
          label: <Link to="/schema-proposals">Schema Proposals</Link>,
        },
        {
          key: '/schedules',
          icon: <SettingOutlined />,
          label: <Link to="/schedules">Transmute Schedules</Link>,
        },
        {
          key: '/activity-log',
          icon: <BranchesOutlined />,
          label: <Link to="/activity-log">Activity Log</Link>,
        },
        {
          key: '/data-integrity',
          icon: <DatabaseOutlined />,
          label: <Link to="/data-integrity">Data Integrity</Link>,
        },
        {
          key: '/system-health',
          icon: <DashboardOutlined />,
          label: <Link to="/system-health">System Health</Link>,
        },
      ],
    },
    {
      key: 'advanced',
      icon: <SettingOutlined />,
      label: 'Advanced',
      children: [
        {
          key: '/schema-changes',
          icon: <BranchesOutlined />,
          label: <Link to="/schema-changes">Schema Review</Link>,
        },
        {
          key: '/activity-manager',
          icon: <SettingOutlined />,
          label: <Link to="/activity-manager">Operations</Link>,
        },
      ],
    },
  ];

  const selectedMenuKey =
    location.pathname.startsWith('/registry/') || location.pathname.startsWith('/shadow/')
      ? '/shadow'
      : location.pathname;

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider width={220} theme="dark">
        <div style={{ padding: '16px', textAlign: 'center' }}>
          <Text strong style={{ color: '#fff', fontSize: 16 }}>CDC Management</Text>
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[selectedMenuKey]}
          defaultOpenKeys={['setup', 'operate', 'advanced']}
          items={menuItems}
        />
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
                <Route path="/shadow" element={<TableRegistry />} />
                <Route path="/shadow/:id/mappings" element={<MappingFieldsPage />} />
                <Route path="/registry" element={<Navigate to="/shadow" replace />} />
                <Route path="/registry/:id/mappings" element={<Navigate to="/shadow" replace />} />
                <Route path="/cdc-internal" element={<Navigate to="/registry" replace />} />
                <Route path="/masters" element={<MasterRegistry />} />
                <Route path="/schema-proposals" element={<SchemaProposals />} />
                <Route path="/schedules" element={<TransmuteSchedules />} />
                <Route path="/sources" element={<SourceConnectors />} />
                <Route path="/queue" element={<Navigate to="/system-health" replace />} />
                <Route path="/activity-log" element={<ActivityLog />} />
                <Route path="/activity-manager" element={<ActivityManager />} />
                <Route path="/data-integrity" element={<DataIntegrity />} />
                <Route path="/system-health" element={<SystemHealth />} />
                <Route path="/source-to-master" element={<SourceToMasterWizard />} />
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
