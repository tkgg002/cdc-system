import { useState } from 'react';
import { Card, Steps, Space, Button, Typography, Alert, Tag } from 'antd';
import {
  CheckCircleOutlined, DatabaseOutlined, ThunderboltOutlined,
  BranchesOutlined, SettingOutlined, RocketOutlined, EyeOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';

const { Title, Text, Paragraph } = Typography;
const { Step } = Steps;

interface StepSpec {
  title: string;
  description: string;
  manual: boolean;
  goto?: string;
  verify: string;
  icon: React.ReactNode;
}

const STEPS: StepSpec[] = [
  {
    title: '1. Debezium Connector',
    description: 'Tạo connector cho Mongo collection mới.',
    manual: false,
    goto: '/sources',
    verify: 'Connector state=RUNNING trên /sources.',
    icon: <DatabaseOutlined />,
  },
  {
    title: '2. Register Shadow',
    description: 'Register shadow table vào cdc_table_registry (source_db + source_table).',
    manual: false,
    goto: '/registry',
    verify: 'Row mới xuất hiện trong /registry group theo source_db.',
    icon: <DatabaseOutlined />,
  },
  {
    title: '3. Tạo Shadow DDL',
    description: 'Click "Tạo Table" trên /registry → worker tạo cdc_internal.<target> + system cols.',
    manual: false,
    goto: '/registry',
    verify: 'is_table_created=true, \\d cdc_internal.<target> hiện system cols.',
    icon: <SettingOutlined />,
  },
  {
    title: '4. Snapshot Now',
    description: 'Trigger Debezium incremental snapshot để bơm rows hiện tại vào Kafka → Shadow.',
    manual: false,
    goto: '/registry',
    verify: 'SinkWorker log "shadow upsert" + SELECT COUNT(*) FROM cdc_internal.<target> > 0.',
    icon: <ThunderboltOutlined />,
  },
  {
    title: '5. Wait for Ingest',
    description: 'SinkWorker consume Kafka → upsert shadow. Auto-ALTER cho field mới.',
    manual: false,
    goto: '/registry',
    verify: 'SELECT COUNT(*) FROM cdc_internal.<target> match với Mongo.',
    icon: <ThunderboltOutlined />,
  },
  {
    title: '6. Review Proposals',
    description: 'SchemaManager flag field mới (financial ⇒ typed) vào cdc_internal.schema_proposal.',
    manual: false,
    goto: '/schema-proposals',
    verify: 'Badge pending count > 0 trên /schema-proposals.',
    icon: <BranchesOutlined />,
  },
  {
    title: '7. Approve Proposals',
    description: 'Approve (optional override data_type/jsonpath/transform_fn). Worker ALTER + insert mapping_rule.',
    manual: false,
    goto: '/schema-proposals',
    verify: 'Proposal status=approved, cdc_mapping_rules có row mới.',
    icon: <CheckCircleOutlined />,
  },
  {
    title: '8. Mapping Rules',
    description: 'Thêm custom mapping rule cho field phức tạp. Preview JsonPath trước khi save.',
    manual: false,
    goto: '/registry',
    verify: 'Click shadow row → mapping fields page → thêm rule + Preview trả 3 sample.',
    icon: <EyeOutlined />,
  },
  {
    title: '9. Create Master',
    description: 'Declare master table spec (master_name, source_shadow, transform_type, spec JSON).',
    manual: false,
    goto: '/masters',
    verify: 'Row master_table_registry schema_status=pending_review.',
    icon: <RocketOutlined />,
  },
  {
    title: '10. Approve Master',
    description: 'Approve → NATS cdc.cmd.master-create → worker CREATE TABLE public.<master> + RLS.',
    manual: false,
    goto: '/masters',
    verify: 'schema_status=approved, \\d public.<master> hiện cols + RLS policy.',
    icon: <CheckCircleOutlined />,
  },
  {
    title: '11. Activate + Schedule',
    description: 'Toggle is_active + tạo cron schedule (cron/immediate/post_ingest).',
    manual: false,
    goto: '/schedules',
    verify: 'Worker log "transmute complete scanned:N inserted:N". public.<master> có rows.',
    icon: <SettingOutlined />,
  },
];

export default function SourceToMasterWizard() {
  const [current, setCurrent] = useState(0);
  const active = STEPS[current];

  return (
    <Card bordered={false}>
      <Title level={4} style={{ marginBottom: 8 }}>Source → Master Wizard</Title>
      <Paragraph type="secondary">
        Hướng dẫn end-to-end: connect 1 Mongo collection mới → data chảy tới Master layer.
        Mỗi step có link sang trang thao tác + DoD verify. KHÔNG tự động thực thi, chỉ điều hướng.
      </Paragraph>

      <Steps current={current} direction="vertical" size="small" onChange={setCurrent}>
        {STEPS.map((s) => (
          <Step key={s.title} title={s.title} description={s.description} icon={s.icon} />
        ))}
      </Steps>

      <Card style={{ marginTop: 24 }} type="inner" title={<Space>{active.icon}<Text strong>{active.title}</Text></Space>}>
        <Paragraph>{active.description}</Paragraph>
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message="DoD verify"
          description={<Text code style={{ fontSize: 12 }}>{active.verify}</Text>}
        />
        <Space>
          {active.goto && (
            <Link to={active.goto}>
              <Button type="primary">Mở trang: {active.goto}</Button>
            </Link>
          )}
          <Button onClick={() => setCurrent((p) => Math.max(0, p - 1))} disabled={current === 0}>
            ← Previous
          </Button>
          <Button onClick={() => setCurrent((p) => Math.min(STEPS.length - 1, p + 1))}
            disabled={current === STEPS.length - 1}>
            Next →
          </Button>
          <Tag color="blue">Step {current + 1} / {STEPS.length}</Tag>
        </Space>
      </Card>
    </Card>
  );
}
