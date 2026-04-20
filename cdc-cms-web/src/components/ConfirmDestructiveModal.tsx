/**
 * ConfirmDestructiveModal
 *
 * Shared modal for destructive operations (restart connector, purge cache,
 * drop data, etc.). Enforces an audit reason (min 10 chars) before enabling
 * the confirm action.
 *
 * Usage example:
 *
 *   const [open, setOpen] = useState(false);
 *   const restart = useRestartConnector();
 *
 *   <ConfirmDestructiveModal
 *     open={open}
 *     title="Khởi động lại Debezium Connector"
 *     description="Hành động này sẽ restart connector và có thể gây gián đoạn pipeline trong 30-60s."
 *     targetName="goopay-mongodb-cdc"
 *     actionLabel="Restart Connector"
 *     danger
 *     loading={restart.isPending}
 *     onConfirm={async (reason) => {
 *       await restart.mutateAsync({ reason });
 *       setOpen(false);
 *     }}
 *     onCancel={() => setOpen(false)}
 *   />
 */
import { useEffect, useState } from 'react';
import { Alert, Input, Modal, Typography } from 'antd';
import { ExclamationCircleOutlined } from '@ant-design/icons';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

const MIN_REASON_LENGTH = 10;

export interface ConfirmDestructiveModalProps {
  open: boolean;
  title: string;
  description: string;
  targetName: string;
  actionLabel: string;
  danger?: boolean;
  onConfirm: (reason: string) => Promise<void> | void;
  onCancel: () => void;
  loading?: boolean;
}

export default function ConfirmDestructiveModal({
  open,
  title,
  description,
  targetName,
  actionLabel,
  danger = false,
  onConfirm,
  onCancel,
  loading = false,
}: ConfirmDestructiveModalProps) {
  const [reason, setReason] = useState('');
  const [submitting, setSubmitting] = useState(false);

  // Reset reason each time modal re-opens so stale text is not carried over.
  useEffect(() => {
    if (open) {
      setReason('');
      setSubmitting(false);
    }
  }, [open]);

  const trimmed = reason.trim();
  const isReasonValid = trimmed.length >= MIN_REASON_LENGTH;
  const isBusy = loading || submitting;

  const handleOk = async () => {
    if (!isReasonValid || isBusy) return;
    try {
      setSubmitting(true);
      await onConfirm(trimmed);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={
        <span>
          {danger && (
            <ExclamationCircleOutlined
              style={{ color: '#ff4d4f', marginRight: 8 }}
            />
          )}
          {title}
        </span>
      }
      onOk={handleOk}
      onCancel={() => {
        if (isBusy) return;
        onCancel();
      }}
      okText={actionLabel}
      okButtonProps={{
        danger,
        disabled: !isReasonValid || isBusy,
        loading: isBusy,
      }}
      cancelButtonProps={{ disabled: isBusy }}
      maskClosable={!isBusy}
      destroyOnHidden
    >
      <Paragraph>{description}</Paragraph>

      <Paragraph>
        <Text type="secondary">Đối tượng: </Text>
        <Text code>{targetName}</Text>
      </Paragraph>

      {danger && (
        <Alert
          type="warning"
          showIcon
          message="Hành động này có thể gây gián đoạn dịch vụ."
          description="Hãy chắc chắn bạn đã thông báo cho team và nắm rõ tác động trước khi tiếp tục."
          style={{ marginBottom: 12 }}
        />
      )}

      <div style={{ marginBottom: 8 }}>
        <Text strong>Lý do thực hiện </Text>
        <Text type="danger">*</Text>
        <Text type="secondary">
          {' '}
          (tối thiểu {MIN_REASON_LENGTH} ký tự, sẽ lưu vào audit log)
        </Text>
      </div>
      <TextArea
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        placeholder="Ví dụ: Kafka Connect stuck trong 10 phút, cần restart để khôi phục pipeline."
        rows={3}
        maxLength={500}
        showCount
        disabled={isBusy}
        status={reason.length > 0 && !isReasonValid ? 'error' : undefined}
      />
      {reason.length > 0 && !isReasonValid && (
        <Text type="danger" style={{ fontSize: 12 }}>
          Cần ít nhất {MIN_REASON_LENGTH} ký tự (hiện tại: {trimmed.length}).
        </Text>
      )}
    </Modal>
  );
}
