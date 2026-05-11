package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/natsconn"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	DLQSubject  = "cdc.dlq"
	MaxRetries  = 3
	BaseBackoff = 1 * time.Second
)

// DLQMessage represents a failed event sent to dead letter queue.
// Both OriginalData and Error must already be sanitized before publish
// or persistence.
type DLQMessage struct {
	OriginalSubject string `json:"original_subject"`
	OriginalData    []byte `json:"original_data"`
	Error           string `json:"error"`
	RetryCount      int    `json:"retry_count"`
	FailedAt        string `json:"failed_at"`
	SourceTable     string `json:"source_table"`
}

// DLQHandler handles retry logic and dead letter queue.
// Any payload or error text written through this handler is sanitized
// before it can reach failed_sync_logs or the replay subject.
type DLQHandler struct {
	nats    *natsconn.NatsClient
	db      *gorm.DB
	masking *service.MaskingService
	logger  *zap.Logger
}

func NewDLQHandler(nats *natsconn.NatsClient, logger *zap.Logger, dbs ...*gorm.DB) *DLQHandler {
	var db *gorm.DB
	if len(dbs) > 0 {
		db = dbs[0]
	}
	return &DLQHandler{
		nats:   nats,
		db:     db,
		logger: logger,
	}
}

func (d *DLQHandler) SetMaskingService(masking *service.MaskingService) {
	d.masking = masking
}

// HandleWithRetry is a compatibility wrapper for callers that do not
// provide a cancellable context yet.
func (d *DLQHandler) HandleWithRetry(subject string, data []byte, sourceTable string, handler func() error) bool {
	return d.HandleWithRetryContext(context.Background(), subject, data, sourceTable, func(context.Context) error {
		return handler()
	})
}

// HandleWithRetryContext retries with a non-blocking backoff that can be
// interrupted by context cancellation for graceful shutdown.
func (d *DLQHandler) HandleWithRetryContext(
	ctx context.Context,
	subject string,
	data []byte,
	sourceTable string,
	handler func(context.Context) error,
) bool {
	var lastErr error
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		if ctx.Err() != nil {
			d.logWarn("event retry aborted by context", zap.String("source_table", sourceTable), zap.Error(ctx.Err()))
			return false
		}

		lastErr = handler(ctx)
		if lastErr == nil {
			return true
		}

		d.logWarn("event processing failed, retrying",
			zap.Int("attempt", attempt),
			zap.Int("max", MaxRetries),
			zap.String("source_table", sourceTable),
			zap.Error(lastErr),
		)

		if attempt < MaxRetries {
			backoff := BaseBackoff * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				d.logWarn("event retry cancelled during backoff",
					zap.String("source_table", sourceTable),
					zap.Error(ctx.Err()),
				)
				return false
			case <-time.After(backoff):
			}
		}
	}

	if err := d.sendToDLQ(ctx, subject, data, sourceTable, lastErr); err != nil {
		d.logError("failed to send event to DLQ",
			zap.String("subject", subject),
			zap.String("source_table", sourceTable),
			zap.Error(err),
		)
	}
	return false
}

func (d *DLQHandler) sendToDLQ(ctx context.Context, subject string, data []byte, sourceTable string, err error) error {
	if err == nil {
		err = fmt.Errorf("unknown processing error")
	}
	if sourceTable == "" {
		sourceTable = extractSourceTableFromSubject(subject)
	}
	sanitizedData := d.normalizeDLQRawJSON(sourceTable, data)

	dlqMsg := DLQMessage{
		OriginalSubject: subject,
		OriginalData:    []byte(sanitizedData),
		Error:           sanitizeDLQError(err.Error()),
		RetryCount:      MaxRetries,
		FailedAt:        time.Now().UTC().Format(time.RFC3339),
		SourceTable:     sourceTable,
	}

	payload, marshalErr := json.Marshal(dlqMsg)
	if marshalErr != nil {
		return fmt.Errorf("marshal dlq payload: %w", marshalErr)
	}

	var failedLogID uint64
	if d.db != nil {
		row, buildErr := d.buildFailedSyncLog(subject, []byte(sanitizedData), sourceTable, err)
		if buildErr != nil {
			return buildErr
		}
		if txErr := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(row).Error; err != nil {
				return err
			}
			failedLogID = row.ID
			return nil
		}); txErr != nil {
			return fmt.Errorf("insert failed_sync_logs before DLQ publish: %w", txErr)
		}
	}

	if d.nats == nil || d.nats.Conn == nil {
		return fmt.Errorf("nats connection not configured")
	}
	if pubErr := d.nats.Conn.Publish(DLQSubject, payload); pubErr != nil {
		d.markPublishFailure(ctx, failedLogID, pubErr)
		return fmt.Errorf("publish to DLQ: %w", pubErr)
	}

	d.logError("event sent to DLQ after max retries",
		zap.String("subject", subject),
		zap.String("source_table", sourceTable),
		zap.String("error", err.Error()),
		zap.Int("retries", MaxRetries),
		zap.Uint64("failed_log_id", failedLogID),
	)
	return nil
}

func (d *DLQHandler) buildFailedSyncLog(subject string, data []byte, sourceTable string, err error) (*model.FailedSyncLog, error) {
	if sourceTable == "" {
		sourceTable = extractSourceTableFromSubject(subject)
	}
	recordID := extractDLQRecordID(data)
	rawJSON := d.normalizeDLQRawJSON(sourceTable, data)
	row := &model.FailedSyncLog{
		TargetTable:  sourceTable,
		SourceTable:  sourceTable,
		RecordID:     recordID,
		Operation:    "dlq",
		RawJSON:      rawJSON,
		ErrorMessage: sanitizeDLQError(err.Error()),
		ErrorType:    "dlq_pending",
		KafkaTopic:   subject,
		RetryCount:   0,
		MaxRetries:   MaxRetries,
		Status:       "pending",
	}
	return row, nil
}

func (d *DLQHandler) markPublishFailure(ctx context.Context, failedLogID uint64, publishErr error) {
	if d.db == nil || failedLogID == 0 {
		return
	}
	now := time.Now().UTC()
	errMsg := truncateDLQError(sanitizeDLQError(publishErr.Error()), 2000)
	updates := map[string]interface{}{
		"status":        "failed",
		"last_error":    errMsg,
		"last_retry_at": &now,
	}
	if err := d.db.WithContext(ctx).Model(&model.FailedSyncLog{}).Where("id = ?", failedLogID).Updates(updates).Error; err != nil {
		d.logWarn("failed to update DLQ publish failure state",
			zap.Uint64("failed_log_id", failedLogID),
			zap.Error(err),
		)
	}
}

// ReplayDLQ replays a DLQ message back to its original subject.
func (d *DLQHandler) ReplayDLQ(dlqData []byte) error {
	var msg DLQMessage
	if err := json.Unmarshal(dlqData, &msg); err != nil {
		return fmt.Errorf("parse DLQ message: %w", err)
	}
	if d.nats == nil || d.nats.Conn == nil {
		return fmt.Errorf("nats connection not configured")
	}
	if err := d.nats.Conn.Publish(msg.OriginalSubject, msg.OriginalData); err != nil {
		return fmt.Errorf("replay to %s: %w", msg.OriginalSubject, err)
	}

	d.logInfo("DLQ message replayed",
		zap.String("subject", msg.OriginalSubject),
		zap.String("source_table", msg.SourceTable),
	)
	return nil
}

func extractDLQRecordID(data []byte) string {
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil {
		return ""
	}
	for _, key := range []string{"_id", "id", "record_id"} {
		if value, ok := payload[key]; ok {
			return fmt.Sprintf("%v", value)
		}
	}
	if after, ok := payload["after"].(map[string]interface{}); ok {
		for _, key := range []string{"_id", "id"} {
			if value, ok := after[key]; ok {
				return fmt.Sprintf("%v", value)
			}
		}
	}
	return ""
}

func normalizeDLQRawJSON(data []byte) json.RawMessage {
	if json.Valid(data) {
		return json.RawMessage(data)
	}
	wrapped, _ := json.Marshal(map[string]string{"raw": string(data)})
	return json.RawMessage(wrapped)
}

func (d *DLQHandler) normalizeDLQRawJSON(table string, data []byte) json.RawMessage {
	if d.masking != nil {
		return d.masking.MaskJSONPayload(table, data)
	}
	return normalizeDLQRawJSON(data)
}

func truncateDLQError(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}

func sanitizeDLQError(errMsg string) string {
	return service.SanitizeFreeformText(errMsg, 2000)
}

func (d *DLQHandler) logInfo(msg string, fields ...zap.Field) {
	if d.logger != nil {
		d.logger.Info(msg, fields...)
	}
}

func (d *DLQHandler) logWarn(msg string, fields ...zap.Field) {
	if d.logger != nil {
		d.logger.Warn(msg, fields...)
	}
}

func (d *DLQHandler) logError(msg string, fields ...zap.Field) {
	if d.logger != nil {
		d.logger.Error(msg, fields...)
	}
}

func extractSourceTableFromSubject(subject string) string {
	if subject == "" {
		return ""
	}
	parts := strings.Split(subject, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
