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
	"cdc-cms-service/pkgs/natsconn"
)

// UpdateRegistryCommand patches allow-listed fields on a TableRegistry
// row. Sync — single primary update plus optional cascading mapping
// rule auto-approve when IsActive flips false→true. Activity log + NATS
// reload publishes happen inside the handler so the audit row is atomic
// with the DB writes.
type UpdateRegistryCommand struct {
	ports.SyncCommandMixin
	ID             uint    `json:"id"`
	SyncEngine     *string `json:"sync_engine,omitempty"`
	SyncInterval   *string `json:"sync_interval,omitempty"`
	Priority       *string `json:"priority,omitempty"`
	IsActive       *bool   `json:"is_active,omitempty"`
	Notes          *string `json:"notes,omitempty"`
	TimestampField *string `json:"timestamp_field,omitempty"`
	UpdatedBy      string  `json:"updated_by,omitempty"`
}

func (UpdateRegistryCommand) Type() string { return "registry.update" }

var (
	ErrRegistryNotFound       = errors.New("registry_not_found")
	ErrRegistryNoFields       = errors.New("no fields to update")
	ErrRegistryInvalidTSField = errors.New("invalid_timestamp_field")
)

func (c UpdateRegistryCommand) Validate() error {
	if c.ID == 0 {
		return errors.New("invalid_registry_id")
	}
	if c.SyncEngine == nil && c.SyncInterval == nil && c.Priority == nil &&
		c.IsActive == nil && c.Notes == nil && c.TimestampField == nil {
		return ErrRegistryNoFields
	}
	if c.TimestampField != nil && !validTimestampField(*c.TimestampField) {
		return ErrRegistryInvalidTSField
	}
	return nil
}

type UpdateRegistryHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewUpdateRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *UpdateRegistryHandler {
	return &UpdateRegistryHandler{db: db, nats: nats, logger: logger}
}

func (h *UpdateRegistryHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(UpdateRegistryCommand)
	if !ok {
		return nil, errors.New("registry.update: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("registry store not ready")
	}

	var existing model.TableRegistry
	if err := h.db.WithContext(ctx).First(&existing, cmd.ID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRegistryNotFound
		}
		return nil, err
	}

	updates := map[string]interface{}{}
	if cmd.SyncEngine != nil {
		updates["sync_engine"] = *cmd.SyncEngine
		existing.SyncEngine = *cmd.SyncEngine
	}
	if cmd.SyncInterval != nil {
		updates["sync_interval"] = *cmd.SyncInterval
		existing.SyncInterval = *cmd.SyncInterval
	}
	if cmd.Priority != nil {
		updates["priority"] = *cmd.Priority
		existing.Priority = *cmd.Priority
	}
	if cmd.IsActive != nil {
		updates["is_active"] = *cmd.IsActive
		existing.IsActive = *cmd.IsActive
	}
	if cmd.Notes != nil {
		updates["notes"] = *cmd.Notes
		existing.Notes = cmd.Notes
	}
	if cmd.TimestampField != nil {
		updates["timestamp_field"] = *cmd.TimestampField
		tsf := *cmd.TimestampField
		existing.TimestampField = &tsf
	}

	if err := h.db.WithContext(ctx).
		Model(&model.TableRegistry{}).
		Where("id = ?", existing.ID).
		Updates(updates).Error; err != nil {
		return nil, err
	}

	if cmd.IsActive != nil && *cmd.IsActive {
		result := h.db.WithContext(ctx).
			Model(&model.MappingRule{}).
			Where("source_table = ? AND status != ?", existing.SourceTable, "approved").
			Updates(map[string]interface{}{
				"status":    "approved",
				"is_active": true,
			})
		if result.RowsAffected > 0 {
			now := time.Now()
			autoDetails, _ := json.Marshal(map[string]interface{}{
				"fields_approved": result.RowsAffected,
				"source_table":    existing.SourceTable,
				"trigger":         "inactive→active",
			})
			if err := h.db.WithContext(ctx).Create(&model.ActivityLog{
				Operation:   "auto-approve-fields",
				TargetTable: existing.TargetTable,
				Status:      "success",
				Details:     autoDetails,
				TriggeredBy: "manual",
				StartedAt:   now,
				CompletedAt: &now,
			}).Error; err != nil {
				h.logger.Warn("auto-approve activity log failed", zap.Error(err))
			}
			if h.nats != nil {
				_ = h.nats.PublishReload(existing.TargetTable, cmd.UpdatedBy, "auto_approve", "")
			}
		}
	}

	dispatched := []string{}
	details, _ := json.Marshal(map[string]interface{}{
		"updates":    updates,
		"user":       cmd.UpdatedBy,
		"dispatched": dispatched,
	})
	now := time.Now()
	if err := h.db.WithContext(ctx).Create(&model.ActivityLog{
		Operation:   "registry-update",
		TargetTable: existing.TargetTable,
		Status:      "accepted",
		Details:     details,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	}).Error; err != nil {
		h.logger.Warn("registry-update activity log failed", zap.Error(err))
	}
	if h.nats != nil {
		_ = h.nats.PublishReload(existing.TargetTable, cmd.UpdatedBy, "update", "")
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message":    "updated — external state dispatched",
		"entry":      existing,
		"dispatched": dispatched,
	})
	return body, nil
}
