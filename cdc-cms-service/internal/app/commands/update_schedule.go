package commands

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
)

// UpdateScheduleCommand updates allow-listed fields on a worker schedule
// (interval_minutes, is_enabled, notes). Sync — single-row UPDATE plus an
// inline ActivityLog so legacy operator dashboards keep working.
type UpdateScheduleCommand struct {
	ports.SyncCommandMixin
	ID              int64   `json:"id"`
	IntervalMinutes *int    `json:"interval_minutes,omitempty"`
	IsEnabled       *bool   `json:"is_enabled,omitempty"`
	Notes           *string `json:"notes,omitempty"`
	UpdatedBy       string  `json:"updated_by,omitempty"`
}

func (UpdateScheduleCommand) Type() string { return "schedule.update" }

var (
	ErrScheduleNotFound = errors.New("schedule_not_found")
	ErrScheduleNoFields = errors.New("nothing to update")
)

func (c UpdateScheduleCommand) Validate() error {
	if c.ID <= 0 {
		return errors.New("invalid_schedule_id")
	}
	if c.IntervalMinutes == nil && c.IsEnabled == nil && c.Notes == nil {
		return ErrScheduleNoFields
	}
	return nil
}

type UpdateScheduleHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewUpdateScheduleHandler(db *gorm.DB, logger *zap.Logger) *UpdateScheduleHandler {
	return &UpdateScheduleHandler{db: db, logger: logger}
}

func (h *UpdateScheduleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(UpdateScheduleCommand)
	if !ok {
		return nil, errors.New("schedule.update: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("schedule store not ready")
	}

	var existing model.WorkerSchedule
	if err := h.db.WithContext(ctx).First(&existing, cmd.ID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrScheduleNotFound
		}
		return nil, err
	}

	updates := map[string]interface{}{}
	if cmd.IntervalMinutes != nil {
		updates["interval_minutes"] = *cmd.IntervalMinutes
	}
	if cmd.IsEnabled != nil {
		updates["is_enabled"] = *cmd.IsEnabled
	}
	if cmd.Notes != nil {
		updates["notes"] = *cmd.Notes
	}
	updates["updated_at"] = time.Now()

	if err := h.db.WithContext(ctx).
		Model(&model.WorkerSchedule{}).
		Where("id = ?", cmd.ID).
		Updates(updates).Error; err != nil {
		return nil, err
	}

	detailsJSON, _ := json.Marshal(updates)
	logTarget := "all_schedules"
	if existing.TargetTable != nil && *existing.TargetTable != "" {
		logTarget = *existing.TargetTable
	}
	now := time.Now()
	if err := h.db.WithContext(ctx).Create(&model.ActivityLog{
		Operation:   "schedule-update",
		TargetTable: logTarget,
		Status:      "success",
		Details:     detailsJSON,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	}).Error; err != nil {
		h.logger.Warn("schedule-update activity log write failed",
			zap.Int64("schedule_id", cmd.ID), zap.Error(err))
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message":        "schedule updated",
		"id":             cmd.ID,
		"updated_fields": updates,
	})
	return body, nil
}
