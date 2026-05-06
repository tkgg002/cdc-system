package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/pkgs/natsconn"
)

// ApproveMasterCommand is POST /api/v1/masters/:name/approve. Sync —
// resolves the binding, flips schema_status='approved', and publishes
// cdc.cmd.master-create so the worker materialises the master table.
//
// Single command on purpose: caller sees one audit row in cdc_jobs and
// one round-trip. The downstream master-create still has its own job
// row when bus.Dispatch is later used elsewhere; here the publish is a
// fire-and-forget continuation captured in this row's result.
type ApproveMasterCommand struct {
	ports.SyncCommandMixin
	Name      string `json:"name"`
	Reason    string `json:"reason"`
	UpdatedBy string `json:"updated_by"`
}

func (ApproveMasterCommand) Type() string { return "master.approve" }

var ErrMasterNotApprovable = errors.New("not_approvable")

func (c ApproveMasterCommand) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("invalid_master_name")
	}
	if len(strings.TrimSpace(c.Reason)) < 10 {
		return errors.New("reason_required_min_10_chars")
	}
	return nil
}

type ApproveMasterHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewApproveMasterHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *ApproveMasterHandler {
	return &ApproveMasterHandler{db: db, nats: nats, logger: logger}
}

func (h *ApproveMasterHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(ApproveMasterCommand)
	if !ok {
		return nil, errors.New("master.approve: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("master store not ready")
	}

	type bindingRow struct {
		ID int64 `gorm:"column:id"`
	}
	var rows []bindingRow
	if err := h.db.WithContext(ctx).Raw(
		`SELECT id FROM cdc_system.master_binding
		  WHERE master_table = ?
		  ORDER BY updated_at DESC, id DESC
		  LIMIT 2`,
		cmd.Name,
	).Scan(&rows).Error; err != nil {
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
		    SET schema_status = 'approved',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = NULL,
		        updated_at = NOW()
		  WHERE id = ?
		    AND schema_status IN ('pending_review','rejected','failed')`,
		cmd.UpdatedBy, rows[0].ID,
	)
	if res.Error != nil {
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrMasterNotApprovable
	}

	dispatched := "cdc.cmd.master-create"
	dispatchErr := ""
	payload, _ := json.Marshal(map[string]string{
		"master_table":   cmd.Name,
		"triggered_by":   cmd.UpdatedBy,
		"correlation_id": "approve-" + cmd.Name + "-" + time.Now().UTC().Format(time.RFC3339Nano),
	})
	if h.nats == nil || h.nats.Conn == nil {
		dispatchErr = "nats not ready"
	} else if err := h.nats.Conn.Publish(dispatched, payload); err != nil {
		dispatchErr = err.Error()
		h.logger.Warn("master-create publish failed", zap.String("master", cmd.Name), zap.Error(err))
	}

	body := map[string]interface{}{
		"status":      "approved",
		"master_name": cmd.Name,
		"dispatched":  dispatched,
	}
	if dispatchErr != "" {
		body["status"] = "approved_but_dispatch_failed"
		body["dispatch_err"] = dispatchErr
	}
	out, _ := json.Marshal(body)
	return out, nil
}
