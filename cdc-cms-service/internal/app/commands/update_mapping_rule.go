package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/pkgs/natsconn"
)

// UpdateMappingRuleCommand is PUT /api/mapping-rules/:id (status only)
// expressed as a sync command. The handler runs in-process: lookup +
// UPDATE + reload publish — no NATS RPC.
type UpdateMappingRuleCommand struct {
	ports.SyncCommandMixin
	ID        int64  `json:"id"`
	Status    string `json:"status"`
	UpdatedBy string `json:"updated_by"`
}

func (UpdateMappingRuleCommand) Type() string { return "mapping.update-status" }

func (c UpdateMappingRuleCommand) Validate() error {
	if c.ID <= 0 {
		return errors.New("id required")
	}
	if strings.TrimSpace(c.Status) == "" {
		return errors.New("status required")
	}
	return nil
}

// UpdateMappingRuleHandler updates `cdc_system.mapping_rule_v2` and
// fires a worker reload for the affected shadow table (if any).
type UpdateMappingRuleHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewUpdateMappingRuleHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *UpdateMappingRuleHandler {
	return &UpdateMappingRuleHandler{db: db, nats: nats, logger: logger}
}

// ErrMappingRuleNotFound surfaces from Handle so the API layer can
// translate to 404 without string-matching.
var ErrMappingRuleNotFound = errors.New("mapping rule not found")

func (h *UpdateMappingRuleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(UpdateMappingRuleCommand)
	if !ok {
		return nil, errors.New("mapping.update-status: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("mapping rule store not ready")
	}

	// shadow_table lives in cdc_system.shadow_binding (JOINed via
	// source_object_id). Selecting it directly off mapping_rule_v2
	// raises 42703 — the column was relocated when the binding became a
	// first-class entity.
	var rule struct {
		ShadowTable *string `gorm:"column:shadow_table"`
	}
	err := h.db.WithContext(ctx).
		Raw(`SELECT sb.shadow_table
		     FROM cdc_system.mapping_rule_v2 mr
		     LEFT JOIN cdc_system.shadow_binding sb
		       ON sb.source_object_id = mr.source_object_id
		      AND sb.is_active = TRUE
		     WHERE mr.id = ?
		     LIMIT 1`, cmd.ID).
		Scan(&rule).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrMappingRuleNotFound
		}
		return nil, err
	}

	updates := map[string]interface{}{
		"status":     cmd.Status,
		"updated_by": cmd.UpdatedBy,
	}
	switch cmd.Status {
	case "rejected":
		updates["is_active"] = false
	case "approved":
		updates["is_active"] = true
	}
	if err := h.db.WithContext(ctx).
		Table("cdc_system.mapping_rule_v2").
		Where("id = ?", cmd.ID).
		Updates(updates).Error; err != nil {
		return nil, err
	}

	if h.nats != nil && rule.ShadowTable != nil && *rule.ShadowTable != "" {
		h.nats.PublishReload(*rule.ShadowTable, cmd.UpdatedBy, "mapping_status_update", "")
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message": "mapping rule updated",
		"id":      cmd.ID,
		"status":  cmd.Status,
	})
	return body, nil
}
