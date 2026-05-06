package commands

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// CreateTransmuteScheduleCommand upserts a row in
// cdc_system.transmute_schedule keyed on (master_table, mode). API layer
// validates cron_expr / mode / master_table; handler just executes the
// SQL UPSERT atomically with the cdc_jobs audit row.
type CreateTransmuteScheduleCommand struct {
	ports.SyncCommandMixin
	MasterTable string     `json:"master_table"`
	Mode        string     `json:"mode"`
	CronExpr    string     `json:"cron_expr,omitempty"`
	NextRunAt   *time.Time `json:"next_run_at,omitempty"`
	IsEnabled   bool       `json:"is_enabled"`
	CreatedBy   string     `json:"created_by,omitempty"`
}

func (CreateTransmuteScheduleCommand) Type() string { return "schedule.create" }

func (c CreateTransmuteScheduleCommand) Validate() error {
	if c.MasterTable == "" {
		return errors.New("master_table required")
	}
	if c.Mode == "" {
		return errors.New("mode required")
	}
	return nil
}

type CreateTransmuteScheduleHandler struct {
	db *gorm.DB
}

func NewCreateTransmuteScheduleHandler(db *gorm.DB) *CreateTransmuteScheduleHandler {
	return &CreateTransmuteScheduleHandler{db: db}
}

func (h *CreateTransmuteScheduleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateTransmuteScheduleCommand)
	if !ok {
		return nil, errors.New("schedule.create: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("transmute schedule store not ready")
	}
	err := h.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.transmute_schedule
		   (master_table, mode, cron_expr, next_run_at, is_enabled, created_by, created_at, updated_at)
		 VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, NOW(), NOW())
		 ON CONFLICT (master_table, mode) DO UPDATE
		   SET cron_expr = EXCLUDED.cron_expr,
		       next_run_at = EXCLUDED.next_run_at,
		       is_enabled = EXCLUDED.is_enabled,
		       updated_at = NOW()`,
		cmd.MasterTable, cmd.Mode, cmd.CronExpr, cmd.NextRunAt, cmd.IsEnabled, cmd.CreatedBy,
	).Error
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status":       "created",
		"master_table": cmd.MasterTable,
		"mode":         cmd.Mode,
	})
	return body, nil
}
