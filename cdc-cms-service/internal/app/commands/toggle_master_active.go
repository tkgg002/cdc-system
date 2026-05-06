package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// ToggleMasterActiveCommand flips is_active on a master_binding row.
// API resolves master_name → ID and passes the resolved ID; handler
// owns the atomic UPDATE so the cdc_jobs audit row is the single
// source of truth for the toggle outcome.
type ToggleMasterActiveCommand struct {
	ports.SyncCommandMixin
	MasterBindingID uint64 `json:"master_binding_id"`
	UpdatedBy       string `json:"updated_by,omitempty"`
}

func (ToggleMasterActiveCommand) Type() string { return "master.toggle-active" }

var (
	ErrMasterBindingNotFound  = errors.New("master_binding_not_found")
	ErrMasterRequiresApproved = errors.New("master_requires_approved")
)

func (c ToggleMasterActiveCommand) Validate() error {
	if c.MasterBindingID == 0 {
		return errors.New("master_binding_id required")
	}
	return nil
}

type ToggleMasterActiveHandler struct {
	db *gorm.DB
}

func NewToggleMasterActiveHandler(db *gorm.DB) *ToggleMasterActiveHandler {
	return &ToggleMasterActiveHandler{db: db}
}

func (h *ToggleMasterActiveHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(ToggleMasterActiveCommand)
	if !ok {
		return nil, errors.New("master.toggle-active: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("master binding store not ready")
	}
	res := h.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.master_binding
		    SET is_active = NOT is_active, updated_at = NOW()
		  WHERE id = ?`,
		cmd.MasterBindingID,
	)
	if res.Error != nil {
		if strings.Contains(res.Error.Error(), "v2_master_active_requires_approved") {
			return nil, ErrMasterRequiresApproved
		}
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrMasterBindingNotFound
	}
	body, _ := json.Marshal(map[string]interface{}{
		"status":            "toggled",
		"master_binding_id": cmd.MasterBindingID,
	})
	return body, nil
}
