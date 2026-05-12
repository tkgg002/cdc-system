package commands

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// UpdateSourceObjectV2Command writes allow-listed metadata fields on a
// V2 source object (is_active, notes, timestamp_field). When IsActive
// flips, the matching shadow_binding row is mirrored. Sync — two-table
// write inside one request.
type UpdateSourceObjectV2Command struct {
	ports.SyncCommandMixin
	ID             int64   `json:"id"`
	IsActive       *bool   `json:"is_active,omitempty"`
	Notes          *string `json:"notes,omitempty"`
	TimestampField *string `json:"timestamp_field,omitempty"`
	PrimaryKeyField *string `json:"primary_key_field,omitempty"`
	PrimaryKeyType  *string `json:"primary_key_type,omitempty"`
	UpdatedBy      string  `json:"updated_by,omitempty"`
}

func (UpdateSourceObjectV2Command) Type() string { return "source.update-v2" }

var (
	ErrSourceObjectNotFound        = errors.New("source_object_not_found")
	ErrSourceObjectNoFields        = errors.New("no_supported_fields_to_update")
	ErrSourceObjectInvalidTSField  = errors.New("invalid_timestamp_field")
)

func (c UpdateSourceObjectV2Command) Validate() error {
	if c.ID <= 0 {
		return errors.New("invalid_source_object_id")
	}
	if c.IsActive == nil && c.Notes == nil && c.TimestampField == nil && c.PrimaryKeyField == nil && c.PrimaryKeyType == nil {
		return ErrSourceObjectNoFields
	}
	if c.TimestampField != nil && !validTimestampField(*c.TimestampField) {
		return ErrSourceObjectInvalidTSField
	}
	return nil
}

// validTimestampField mirrors api.isValidTimestampField: ^[A-Za-z_][A-Za-z0-9_]{0,63}$.
// Inlined here to keep the commands package free of api imports.
func validTimestampField(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

type UpdateSourceObjectV2Handler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewUpdateSourceObjectV2Handler(db *gorm.DB, logger *zap.Logger) *UpdateSourceObjectV2Handler {
	return &UpdateSourceObjectV2Handler{db: db, logger: logger}
}

func (h *UpdateSourceObjectV2Handler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(UpdateSourceObjectV2Command)
	if !ok {
		return nil, errors.New("source.update-v2: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("source store not ready")
	}

	updates := map[string]interface{}{}
	if cmd.IsActive != nil {
		updates["is_active"] = *cmd.IsActive
		if *cmd.IsActive {
			updates["profile_status"] = "active"
		} else {
			updates["profile_status"] = "paused"
		}
	}
	if cmd.Notes != nil {
		updates["notes"] = *cmd.Notes
	}
	if cmd.TimestampField != nil {
		updates["timestamp_field"] = *cmd.TimestampField
	}
	if cmd.PrimaryKeyField != nil {
		updates["primary_key_field"] = *cmd.PrimaryKeyField
	}
	if cmd.PrimaryKeyType != nil {
		updates["primary_key_type"] = *cmd.PrimaryKeyType
	}
	updates["updated_at"] = gorm.Expr("NOW()")

	res := h.db.WithContext(ctx).
		Table("cdc_system.source_object_registry").
		Where("id = ?", cmd.ID).
		Updates(updates)
	if res.Error != nil {
		h.logger.Error("update v2 source object failed",
			zap.Int64("source_object_id", cmd.ID), zap.Error(res.Error))
		return nil, res.Error
	}
	if res.RowsAffected == 0 {
		return nil, ErrSourceObjectNotFound
	}

	if cmd.IsActive != nil {
		shadowUpdates := map[string]interface{}{
			"is_active":  *cmd.IsActive,
			"updated_at": gorm.Expr("NOW()"),
		}
		if err := h.db.WithContext(ctx).
			Table("cdc_system.shadow_binding").
			Where("source_object_id = ?", cmd.ID).
			Updates(shadowUpdates).Error; err != nil {
			h.logger.Error("update v2 shadow binding active flag failed",
				zap.Int64("source_object_id", cmd.ID), zap.Error(err))
			return nil, err
		}
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message":          "source object updated in v2 metadata",
		"source_object_id": cmd.ID,
		"updated_fields":   updates,
	})
	return body, nil
}
