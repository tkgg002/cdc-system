package commands

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
)

// CreateWorkerScheduleCommand inserts a row into cdc_worker_schedule.
// Distinct from schedule.create (Đợt 4) which targets the V2
// cdc_system.transmute_schedule table — this one keeps legacy
// recon/sync schedules. Type tag namespaced with "worker-" so the
// CommandBus registry doesn't collide.
type CreateWorkerScheduleCommand struct {
	ports.SyncCommandMixin
	Operation       string  `json:"operation"`
	TargetTable     *string `json:"target_table,omitempty"`
	IntervalMinutes int     `json:"interval_minutes"`
	IsEnabled       bool    `json:"is_enabled"`
	Notes           *string `json:"notes,omitempty"`
	CreatedBy       string  `json:"created_by,omitempty"`
}

func (CreateWorkerScheduleCommand) Type() string { return "worker-schedule.create" }

func (c CreateWorkerScheduleCommand) Validate() error {
	if c.Operation == "" {
		return errors.New("operation required")
	}
	return nil
}

type CreateWorkerScheduleHandler struct {
	db *gorm.DB
}

func NewCreateWorkerScheduleHandler(db *gorm.DB) *CreateWorkerScheduleHandler {
	return &CreateWorkerScheduleHandler{db: db}
}

func (h *CreateWorkerScheduleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateWorkerScheduleCommand)
	if !ok {
		return nil, errors.New("worker-schedule.create: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("worker schedule store not ready")
	}
	row := model.WorkerSchedule{
		Operation:       cmd.Operation,
		TargetTable:     cmd.TargetTable,
		IntervalMinutes: cmd.IntervalMinutes,
		IsEnabled:       cmd.IsEnabled,
		Notes:           cmd.Notes,
	}
	if err := h.db.WithContext(ctx).Create(&row).Error; err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]interface{}{
		"id":      row.ID,
		"created": row,
	})
	return body, nil
}
