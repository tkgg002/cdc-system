import { useState } from 'react';
import { Modal, Form, Input, Select, Switch, message } from 'antd';
import { cmsApi } from '../services/api';

interface Props {
  visible: boolean;
  onCancel: () => void;
  onSuccess: () => void;
  initialValues?: {
    source_database?: string;
    source_table: string;
    shadow_schema?: string;
    shadow_table?: string;
    source_field: string;
  };
}

export default function AddMappingModal({ visible, onCancel, onSuccess, initialValues }: Props) {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);
      await cmsApi.post('/api/mapping-rules', values);
      message.success('Mapping rule created successfully');
      form.resetFields();
      onSuccess();
    } catch (err: any) {
      if (err.errorFields) return; // Validation error
      message.error(err.response?.data?.error || 'Failed to create mapping rule');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title="Add Mapping Rule"
      open={visible}
      onCancel={onCancel}
      onOk={handleSubmit}
      confirmLoading={loading}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          source_database: initialValues?.source_database,
          source_table: initialValues?.source_table,
          shadow_schema: initialValues?.shadow_schema,
          shadow_table: initialValues?.shadow_table,
          source_field: initialValues?.source_field,
          is_active: true,
          is_enriched: false
        }}
      >
        <Form.Item name="source_database" hidden>
          <Input />
        </Form.Item>
        <Form.Item
          name="source_table"
          label="Source Object Table"
          tooltip="Backend hiện ưu tiên resolve theo source/shadow context V2; source_table được giữ lại làm compatibility fallback."
          rules={[{ required: true }]}
        >
          <Input disabled />
        </Form.Item>
        <Form.Item name="shadow_schema" hidden>
          <Input />
        </Form.Item>
        <Form.Item name="shadow_table" hidden>
          <Input />
        </Form.Item>
        <Form.Item
          name="source_field"
          label="Source Field (from shadow raw payload)"
          rules={[{ required: true }]}
        >
          <Input placeholder="e.g. user_id, amount" />
        </Form.Item>
        <Form.Item name="target_column" label="Target Column" rules={[{ required: true }]}>
          <Input placeholder="e.g. user_id, tx_amount" />
        </Form.Item>
        <Form.Item name="data_type" label="Data Type" rules={[{ required: true }]}>
          <Select placeholder="Select target data type">
            <Select.Option value="VARCHAR">VARCHAR</Select.Option>
            <Select.Option value="TEXT">TEXT</Select.Option>
            <Select.Option value="INT">INT</Select.Option>
            <Select.Option value="BIGINT">BIGINT</Select.Option>
            <Select.Option value="NUMERIC">NUMERIC</Select.Option>
            <Select.Option value="BOOLEAN">BOOLEAN</Select.Option>
            <Select.Option value="TIMESTAMP">TIMESTAMP</Select.Option>
            <Select.Option value="JSONB">JSONB</Select.Option>
          </Select>
        </Form.Item>
        <Form.Item name="is_active" label="Active" valuePropName="checked">
          <Switch />
        </Form.Item>
        <Form.Item name="is_enriched" label="Is Enriched" valuePropName="checked">
          <Switch />
        </Form.Item>
      </Form>
    </Modal>
  );
}
