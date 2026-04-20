package handler

import (
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/pkgs/natsconn"

	"go.uber.org/zap"
)

const (
	DLQSubject  = "cdc.dlq"
	MaxRetries  = 3
	BaseBackoff = 1 * time.Second
)

// DLQMessage represents a failed event sent to dead letter queue
type DLQMessage struct {
	OriginalSubject string `json:"original_subject"`
	OriginalData    []byte `json:"original_data"`
	Error           string `json:"error"`
	RetryCount      int    `json:"retry_count"`
	FailedAt        string `json:"failed_at"`
	SourceTable     string `json:"source_table"`
}

// DLQHandler handles retry logic and dead letter queue
type DLQHandler struct {
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewDLQHandler(nats *natsconn.NatsClient, logger *zap.Logger) *DLQHandler {
	return &DLQHandler{nats: nats, logger: logger}
}

// HandleWithRetry wraps an event handler with retry logic.
// Returns true if processed successfully, false if sent to DLQ.
func (d *DLQHandler) HandleWithRetry(subject string, data []byte, sourceTable string, handler func() error) bool {
	var lastErr error
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		lastErr = handler()
		if lastErr == nil {
			return true
		}

		d.logger.Warn("event processing failed, retrying",
			zap.Int("attempt", attempt),
			zap.Int("max", MaxRetries),
			zap.String("source_table", sourceTable),
			zap.Error(lastErr),
		)

		if attempt < MaxRetries {
			backoff := BaseBackoff * time.Duration(1<<(attempt-1)) // exponential
			time.Sleep(backoff)
		}
	}

	// All retries failed → send to DLQ
	d.sendToDLQ(subject, data, sourceTable, lastErr)
	return false
}

func (d *DLQHandler) sendToDLQ(subject string, data []byte, sourceTable string, err error) {
	dlqMsg := DLQMessage{
		OriginalSubject: subject,
		OriginalData:    data,
		Error:           err.Error(),
		RetryCount:      MaxRetries,
		FailedAt:        time.Now().Format(time.RFC3339),
		SourceTable:     sourceTable,
	}

	payload, _ := json.Marshal(dlqMsg)
	if pubErr := d.nats.Conn.Publish(DLQSubject, payload); pubErr != nil {
		d.logger.Error("failed to publish to DLQ",
			zap.String("subject", subject),
			zap.Error(pubErr),
		)
		return
	}

	d.logger.Error("event sent to DLQ after max retries",
		zap.String("subject", subject),
		zap.String("source_table", sourceTable),
		zap.String("error", err.Error()),
		zap.Int("retries", MaxRetries),
	)
}

// ReplayDLQ replays a DLQ message back to its original subject
func (d *DLQHandler) ReplayDLQ(dlqData []byte) error {
	var msg DLQMessage
	if err := json.Unmarshal(dlqData, &msg); err != nil {
		return fmt.Errorf("parse DLQ message: %w", err)
	}

	if err := d.nats.Conn.Publish(msg.OriginalSubject, msg.OriginalData); err != nil {
		return fmt.Errorf("replay to %s: %w", msg.OriginalSubject, err)
	}

	d.logger.Info("DLQ message replayed",
		zap.String("subject", msg.OriginalSubject),
		zap.String("source_table", msg.SourceTable),
	)
	return nil
}
