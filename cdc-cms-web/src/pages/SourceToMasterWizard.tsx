import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Card, Steps, Space, Button, Typography, Alert, Tag, Select, Input, message, Spin,
} from 'antd';
import {
  CheckCircleOutlined, DatabaseOutlined, ThunderboltOutlined,
  BranchesOutlined, SettingOutlined, RocketOutlined, EyeOutlined,
} from '@ant-design/icons';
import { Link, useSearchParams } from 'react-router-dom';
import { cmsApi } from '../services/api';

const { Title, Text, Paragraph } = Typography;

interface SourceRow {
  id: number;
  connector_name: string;
  source_type: string;
}

interface ProgressEntry {
  ts?: string;
  step?: number;
  event?: string;
  actor?: string;
  [k: string]: unknown;
}

interface WizardSession {
  id: string;
  source_name?: string;
  connector_id?: number | null;
  registry_id?: number | null;
  master_name?: string;
  current_step: number;
  status: 'draft' | 'running' | 'done' | 'failed';
  step_payload?: Record<string, unknown>;
  progress_log?: ProgressEntry[];
  created_by?: string;
  created_at?: string;
  updated_at?: string;
}

interface StepSpec {
  title: string;
  description: string;
  goto?: string;
  verify: string;
  icon: React.ReactNode;
}

const STEPS: StepSpec[] = [
  { title: '1. Debezium Connector', description: 'Tạo connector cho source mới.', goto: '/sources', verify: 'Connector state=RUNNING.', icon: <DatabaseOutlined /> },
  { title: '2. Register Shadow', description: 'Register source object và shadow binding theo metadata V2.', goto: '/shadow', verify: 'Row mới trong /shadow, is_table_created=true.', icon: <DatabaseOutlined /> },
  { title: '3. Shadow DDL', description: 'EnsureShadowTable synchronous trong Register. Trigger Sonyflake attached.', goto: '/shadow', verify: '\\d shadow_<source_db>.<target> có 8 cols + trigger.', icon: <SettingOutlined /> },
  { title: '4. Snapshot Now', description: 'Trigger Debezium incremental snapshot.', goto: '/shadow', verify: 'SinkWorker log "shadow upsert".', icon: <ThunderboltOutlined /> },
  { title: '5. Wait for Ingest', description: 'SinkWorker consume Kafka → upsert shadow.', goto: '/shadow', verify: 'COUNT(shadow) > 0.', icon: <ThunderboltOutlined /> },
  { title: '6. Review Proposals', description: 'SchemaManager emit proposals.', goto: '/schema-proposals', verify: 'Pending count > 0.', icon: <BranchesOutlined /> },
  { title: '7. Approve Proposals', description: 'Approve → ALTER shadow + mapping rule.', goto: '/schema-proposals', verify: 'Proposal status=approved.', icon: <CheckCircleOutlined /> },
  { title: '8. Mapping Rules', description: 'Thêm custom rule nếu cần.', goto: '/shadow', verify: 'Preview trả 3 sample.', icon: <EyeOutlined /> },
  { title: '9. Create Master', description: 'Declare master binding + transform spec.', goto: '/masters', verify: 'schema_status=pending_review.', icon: <RocketOutlined /> },
  { title: '10. Approve Master', description: 'Approve → worker CREATE TABLE.', goto: '/masters', verify: 'schema_status=approved.', icon: <CheckCircleOutlined /> },
  { title: '11. Activate + Swap', description: 'Atomic swap public.<master> khi v2 ready.', goto: '/schedules', verify: 'Worker "transmute complete". Rows in master.', icon: <SettingOutlined /> },
];

export default function SourceToMasterWizard() {
  const [params, setParams] = useSearchParams();
  const sessionId = params.get('session_id');
  const [session, setSession] = useState<WizardSession | null>(null);
  const [loading, setLoading] = useState(false);
  const [sources, setSources] = useState<SourceRow[]>([]);

  const current = session?.current_step ?? 0;
  const active = STEPS[Math.min(current, STEPS.length - 1)];

  const loadSession = useCallback(async (id: string) => {
    setLoading(true);
    try {
      const { data } = await cmsApi.get<WizardSession>(`/api/v1/wizard/sessions/${id}`);
      setSession(data);
    } catch {
      message.error('Session not found');
      setParams({});
    } finally {
      setLoading(false);
    }
  }, [setParams]);

  // Bootstrap: create draft on first mount when URL has no session_id.
  useEffect(() => {
    if (sessionId) {
      loadSession(sessionId);
      return;
    }
    (async () => {
      try {
        const { data } = await cmsApi.post<WizardSession>('/api/v1/wizard/sessions', {});
        setSession(data);
        setParams({ session_id: data.id });
      } catch {
        message.error('Cannot create wizard session');
      }
    })();
  }, [sessionId, loadSession, setParams]);

  // Poll progress every 2s while status='running'.
  useEffect(() => {
    if (!session || session.status !== 'running') return;
    const t = setInterval(() => {
      if (session.id) loadSession(session.id);
    }, 2000);
    return () => clearInterval(t);
  }, [session, loadSession]);

  // Load sources once for step-1 dropdown.
  useEffect(() => {
    cmsApi.get('/api/v1/sources')
      .then(({ data }) => setSources(data.data || []))
      .catch(() => setSources([]));
  }, []);

  const patch = async (updates: Partial<WizardSession> & { step_payload?: Record<string, unknown> }) => {
    if (!session) return;
    try {
      const { data } = await cmsApi.patch<WizardSession>(
        `/api/v1/wizard/sessions/${session.id}`,
        updates,
      );
      setSession(data);
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Save failed');
    }
  };

  const execute = async () => {
    if (!session) return;
    const src = session.source_name || `connector#${session.connector_id ?? '?'}`;
    const dst = session.master_name || 'unnamed-master';
    const reason = `automate source=${src} → master=${dst} via wizard`;
    try {
      await cmsApi.post(
        `/api/v1/wizard/sessions/${session.id}/execute`,
        { reason },
        {
          headers: {
            'Idempotency-Key': `wizard-exec-${session.id}-${Date.now()}`,
            'X-Action-Reason': reason,
          },
        },
      );
      message.success('Pipeline started — progress will stream here.');
      loadSession(session.id);
    } catch (err) {
      const e = err as { response?: { data?: { error?: string } } };
      message.error(e.response?.data?.error || 'Execute failed');
    }
  };

  const progressLog = useMemo<ProgressEntry[]>(
    () => (Array.isArray(session?.progress_log) ? session!.progress_log : []),
    [session],
  );

  if (loading || !session) {
    return <Card bordered={false}><Spin /> Loading wizard…</Card>;
  }

  return (
    <Card bordered={false}>
      <Space align="center" style={{ marginBottom: 8 }}>
        <Title level={4} style={{ margin: 0 }}>Source → Master Wizard</Title>
        <Tag color={session.status === 'running' ? 'blue' : session.status === 'done' ? 'green' : 'default'}>
          {session.status}
        </Tag>
        <Text type="secondary" code>session: {session.id.slice(0, 8)}…</Text>
      </Space>
      <Paragraph type="secondary">
        Stateful wizard — F5 resume được qua URL param. Status stream qua poll 2s khi running.
      </Paragraph>

      <Steps
        current={Math.min(current, STEPS.length - 1)}
        direction="vertical"
        size="small"
        onChange={(n) => patch({ current_step: n })}
        items={STEPS.map((s) => ({
          key: s.title,
          title: s.title,
          description: s.description,
          icon: s.icon,
        }))}
      />

      <Card style={{ marginTop: 24 }} type="inner" title={<Space>{active.icon}<Text strong>{active.title}</Text></Space>}>
        <Paragraph>{active.description}</Paragraph>

        {current === 0 && (
          <Space direction="vertical" style={{ width: '100%', marginBottom: 12 }}>
            <Text>Source</Text>
            <Select
              placeholder="Pick registered connector"
              value={session.connector_id ?? undefined}
              style={{ width: '100%' }}
              onChange={(id: number) => {
                const src = sources.find((s) => s.id === id);
                patch({
                  connector_id: id,
                  source_name: src?.connector_name,
                  step_payload: { ...(session.step_payload || {}), source_type: src?.source_type },
                });
              }}
              options={sources.map((s) => ({ label: `${s.connector_name} (${s.source_type})`, value: s.id }))}
            />
            <Text>Master table name (final)</Text>
            <Input
              placeholder="e.g. public_user"
              value={session.master_name || ''}
              onChange={(e) => patch({ master_name: e.target.value })}
            />
            <Button type="primary" size="large" icon={<RocketOutlined />} onClick={execute}
              disabled={!session.connector_id || session.status === 'running'}>
              🚀 Automate Everything
            </Button>
          </Space>
        )}

        <Alert type="info" showIcon style={{ marginBottom: 12 }}
          message="DoD verify"
          description={<Text code style={{ fontSize: 12 }}>{active.verify}</Text>} />

        <Space>
          {active.goto && (
            <Link to={active.goto}>
              <Button>Mở trang: {active.goto}</Button>
            </Link>
          )}
          <Button onClick={() => patch({ current_step: Math.max(0, current - 1) })} disabled={current === 0}>
            ← Previous
          </Button>
          <Button onClick={() => patch({ current_step: Math.min(STEPS.length - 1, current + 1) })}
            disabled={current === STEPS.length - 1}>
            Next →
          </Button>
          <Tag color="blue">Step {current + 1} / {STEPS.length}</Tag>
        </Space>

        {progressLog.length > 0 && (
          <Card size="small" style={{ marginTop: 16 }} title="Progress Log" type="inner">
            <div style={{ maxHeight: 200, overflowY: 'auto', fontFamily: 'monospace', fontSize: 12 }}>
              {progressLog.map((e, i) => (
                <div key={i}>
                  <Text type="secondary">{e.ts}</Text>{' '}
                  <Tag>{`step ${e.step ?? '-'}`}</Tag>{' '}
                  <Text>{e.event}</Text>{' '}
                  {e.actor && <Text type="secondary">by {e.actor}</Text>}
                </div>
              ))}
            </div>
          </Card>
        )}
      </Card>
    </Card>
  );
}
