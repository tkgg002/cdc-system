package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// RejectMasterCommand is POST /api/v1/masters/:name/reject expressed as
// a sync command. Lookup + UPDATE in-process — no NATS RPC.
type RejectMasterCommand struct {
	ports.SyncCommandMixin
	Name      string `json:"name"`
	Reason    string `json:"reason"`
	UpdatedBy string `json:"updated_by"`
}

func (RejectMasterCommand) Type() string { return "master.reject" }

func (c RejectMasterCommand) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("invalid_master_name")
	}
	if len(strings.TrimSpace(c.Reason)) < 10 {
		return errors.New("reason_required_min_10_chars")
	}
	return nil
}

var (
	ErrMasterNotFound        = errors.New("not_found")
	ErrMasterNameAmbiguous   = errors.New("ambiguous_master_name")
)

type RejectMasterHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewRejectMasterHandler(db *gorm.DB, logger *zap.Logger) *RejectMasterHandler {
	return &RejectMasterHandler{db: db, logger: logger}
}

func (h *RejectMasterHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(RejectMasterCommand)
	if !ok {
		return nil, errors.New("master.reject: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("master store not ready")
	}

	type bindingRow struct {
		ID int64 `gorm:"column:id"`
	}
	var rows []bindingRow
	err := h.db.WithContext(ctx).Raw(
		`SELECT id FROM cdc_system.master_binding
		  WHERE master_table = ?
		  ORDER BY updated_at DESC, id DESC
		  LIMIT 2`,
		cmd.Name,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrMasterNotFound
	}
	if len(rows) > 1 {
		return nil, ErrMasterNameAmbiguous
	}

	res := h.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.master_binding
		    SET schema_status = 'rejected',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = ?,
		        is_active = false,
		        updated_at = NOW()
		  WHERE id = ?`,
		cmd.UpdatedBy, cmd.Reason, rows[0].ID,
	)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrMasterNotFound
	}

	body, _ := json.Marshal(map[string]interface{}{
		"status":      "rejected",
		"master_name": cmd.Name,
	})
	return body, nil
}
