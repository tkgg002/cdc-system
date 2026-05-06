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

// BulkRegisterRegistryCommand bulk-inserts TableRegistry rows + emits
// one global PublishReload + one activity log row, atomically per the
// cdc_jobs audit envelope. CreateDefaultColumns dispatches per row stay
// at the API layer (post-bus) since they're independent fire-and-forget.
type BulkRegisterRegistryCommand struct {
	ports.SyncCommandMixin
	Entries   []model.TableRegistry `json:"entries"`
	CreatedBy string                `json:"created_by,omitempty"`
}

func (BulkRegisterRegistryCommand) Type() string { return "registry.bulk-register" }

func (c BulkRegisterRegistryCommand) Validate() error {
	if len(c.Entries) == 0 {
		return errors.New("entries required")
	}
	for i, e := range c.Entries {
		if e.SourceDB == "" || e.SourceTable == "" || e.TargetTable == "" {
			return errors.New("entry missing source_db/source_table/target_table at index")
		}
		_ = i
	}
	return nil
}

type BulkRegisterRegistryHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewBulkRegisterRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *BulkRegisterRegistryHandler {
	return &BulkRegisterRegistryHandler{db: db, nats: nats, logger: logger}
}

func (h *BulkRegisterRegistryHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(BulkRegisterRegistryCommand)
	if !ok {
		return nil, errors.New("registry.bulk-register: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("registry store not ready")
	}

	entries := cmd.Entries
	if err := h.db.WithContext(ctx).Create(&entries).Error; err != nil {
		return nil, err
	}

	tables := make([]string, 0, len(entries))
	for _, e := range entries {
		tables = append(tables, e.TargetTable)
	}

	var refreshed []model.TableRegistry
	if err := h.db.WithContext(ctx).
		Where("target_table IN ?", tables).
		Find(&refreshed).Error; err != nil {
		h.logger.Warn("bulk-register re-fetch failed", zap.Error(err))
	}

	if h.nats != nil {
		_ = h.nats.PublishReload("*", cmd.CreatedBy, "bulk_register", "")
	}

	now := time.Now()
	details, _ := json.Marshal(map[string]interface{}{
		"user":    cmd.CreatedBy,
		"created": len(entries),
	})
	if err := h.db.WithContext(ctx).Create(&model.ActivityLog{
		Operation:   "bulk-register",
		TargetTable: "*",
		Status:      "accepted",
		Details:     details,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	}).Error; err != nil {
		h.logger.Warn("bulk-register activity log failed", zap.Error(err))
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message": "tables registered",
		"created": len(entries),
		"entries": refreshed,
	})
	return body, nil
}
