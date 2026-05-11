package service

import (
	"encoding/json"
	"time"

	"centralized-data-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ActivityLogger records all auto operations to cdc_activity_log for monitoring
type ActivityLogger struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewActivityLogger(db *gorm.DB, logger *zap.Logger) *ActivityLogger {
	return &ActivityLogger{db: db, logger: logger}
}

// Start begins a new activity log entry. Returns the entry to be completed later.
func (a *ActivityLogger) Start(operation, targetTable, triggeredBy string) *model.ActivityLog {
	entry := &model.ActivityLog{
		Operation:   operation,
		TargetTable: targetTable,
		Status:      "running",
		TriggeredBy: triggeredBy,
		StartedAt:   time.Now(),
	}
	if err := a.db.Create(entry).Error; err != nil {
		a.logger.Warn("failed to create activity log", zap.Error(err))
	}
	return entry
}

// Complete marks an activity as successful
func (a *ActivityLogger) Complete(entry *model.ActivityLog, rowsAffected int64, details map[string]interface{}) {
	now := time.Now()
	duration := int(now.Sub(entry.StartedAt).Milliseconds())
	sanitizedDetails, _ := SanitizeNestedStrings(details, 2000).(map[string]interface{})
	detailsJSON, _ := json.Marshal(sanitizedDetails)

	a.db.Model(entry).Updates(map[string]interface{}{
		"status":        "success",
		"rows_affected": rowsAffected,
		"duration_ms":   duration,
		"details":       detailsJSON,
		"completed_at":  now,
	})

	a.logger.Info("activity completed",
		zap.String("operation", entry.Operation),
		zap.String("table", entry.TargetTable),
		zap.Int64("rows", rowsAffected),
		zap.Int("duration_ms", duration),
	)
}

// Fail marks an activity as failed
func (a *ActivityLogger) Fail(entry *model.ActivityLog, errMsg string) {
	now := time.Now()
	duration := int(now.Sub(entry.StartedAt).Milliseconds())
	errMsg = SanitizeFreeformText(errMsg, 2000)

	a.db.Model(entry).Updates(map[string]interface{}{
		"status":        "error",
		"error_message": errMsg,
		"duration_ms":   duration,
		"completed_at":  now,
	})

	a.logger.Error("activity failed",
		zap.String("operation", entry.Operation),
		zap.String("table", entry.TargetTable),
		zap.String("error", errMsg),
		zap.Int("duration_ms", duration),
	)
}

// Skip marks an activity as skipped (no work needed)
func (a *ActivityLogger) Skip(entry *model.ActivityLog, reason string) {
	now := time.Now()
	duration := int(now.Sub(entry.StartedAt).Milliseconds())
	reason = SanitizeFreeformText(reason, 2000)

	a.db.Model(entry).Updates(map[string]interface{}{
		"status":        "skipped",
		"error_message": reason,
		"duration_ms":   duration,
		"completed_at":  now,
	})
}

// Quick logs a complete activity in one call (for simple operations)
func (a *ActivityLogger) Quick(operation, targetTable, triggeredBy, status string, rowsAffected int64, details map[string]interface{}, errMsg string) {
	sanitizedDetails, _ := SanitizeNestedStrings(details, 2000).(map[string]interface{})
	detailsJSON, _ := json.Marshal(sanitizedDetails)
	now := time.Now()
	var errPtr *string
	if errMsg != "" {
		errMsg = SanitizeFreeformText(errMsg, 2000)
		errPtr = &errMsg
	}

	entry := &model.ActivityLog{
		Operation:    operation,
		TargetTable:  targetTable,
		Status:       status,
		RowsAffected: rowsAffected,
		Details:      detailsJSON,
		ErrorMessage: errPtr,
		TriggeredBy:  triggeredBy,
		StartedAt:    now,
		CompletedAt:  &now,
	}
	a.db.Create(entry)
}
