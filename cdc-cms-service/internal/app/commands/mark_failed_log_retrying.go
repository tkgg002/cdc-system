package commands

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// MarkFailedLogRetryingCommand stamps a failed_sync_logs row so the FE
// shows "retrying" the moment the operator hits Retry. The actual retry
// work is dispatched separately via RetryFailedCommand (async, on the
// worker); this command only flips the UI projection.
type MarkFailedLogRetryingCommand struct {
	ports.SyncCommandMixin
	FailedLogID uint64 `json:"failed_log_id"`
	UpdatedBy   string `json:"updated_by,omitempty"`
}

func (MarkFailedLogRetryingCommand) Type() string { return "recon.failed-log-mark-retrying" }

var ErrFailedLogNotFound = errors.New("failed_log_not_found")

func (c MarkFailedLogRetryingCommand) Validate() error {
	if c.FailedLogID == 0 {
		return errors.New("failed_log_id required")
	}
	return nil
}

type MarkFailedLogRetryingHandler struct {
	db *gorm.DB
}

func NewMarkFailedLogRetryingHandler(db *gorm.DB) *MarkFailedLogRetryingHandler {
	return &MarkFailedLogRetryingHandler{db: db}
}

func (h *MarkFailedLogRetryingHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(MarkFailedLogRetryingCommand)
	if !ok {
		return nil, errors.New("recon.failed-log-mark-retrying: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("failed log store not ready")
	}
	res := h.db.WithContext(ctx).
		Table("failed_sync_logs").
		Where("id = ?", cmd.FailedLogID).
		Updates(map[string]interface{}{
			"status":        "retrying",
			"retry_count":   gorm.Expr("retry_count + 1"),
			"last_retry_at": time.Now(),
		})
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrFailedLogNotFound
	}
	return json.RawMessage(`{"ok":true}`), nil
}
