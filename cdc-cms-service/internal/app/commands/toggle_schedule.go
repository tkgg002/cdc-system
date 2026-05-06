package commands

import (
	"context"
	"encoding/json"
	"errors"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// ToggleTransmuteScheduleCommand flips is_enabled on a single
// cdc_system.transmute_schedule row identified by primary-key id.
type ToggleTransmuteScheduleCommand struct {
	ports.SyncCommandMixin
	ID        string `json:"id"`
	IsEnabled bool   `json:"is_enabled"`
	UpdatedBy string `json:"updated_by,omitempty"`
}

func (ToggleTransmuteScheduleCommand) Type() string { return "schedule.toggle" }

var ErrTransmuteScheduleNotFound = errors.New("transmute_schedule_not_found")

func (c ToggleTransmuteScheduleCommand) Validate() error {
	if c.ID == "" {
		return errors.New("id required")
	}
	return nil
}

type ToggleTransmuteScheduleHandler struct {
	db *gorm.DB
}

func NewToggleTransmuteScheduleHandler(db *gorm.DB) *ToggleTransmuteScheduleHandler {
	return &ToggleTransmuteScheduleHandler{db: db}
}

func (h *ToggleTransmuteScheduleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(ToggleTransmuteScheduleCommand)
	if !ok {
		return nil, errors.New("schedule.toggle: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("transmute schedule store not ready")
	}
	res := h.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.transmute_schedule
		    SET is_enabled = ?, updated_at = NOW()
		  WHERE id = ?`,
		cmd.IsEnabled, cmd.ID,
	)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrTransmuteScheduleNotFound
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status":     "toggled",
		"id":         cmd.ID,
		"is_enabled": cmd.IsEnabled,
	})
	return body, nil
}
