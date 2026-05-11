package model

import (
	"encoding/json"
	"time"
)

type SyncRuntimeState struct {
	ID               int64           `gorm:"primaryKey" json:"id"`
	SourceObjectID   *int64          `gorm:"column:source_object_id" json:"source_object_id"`
	ShadowBindingID  *int64          `gorm:"column:shadow_binding_id" json:"shadow_binding_id"`
	MasterBindingID  *int64          `gorm:"column:master_binding_id" json:"master_binding_id"`
	RuntimeScope     string          `gorm:"column:runtime_scope;not null" json:"runtime_scope"`
	LastSuccessAt    *time.Time      `gorm:"column:last_success_at" json:"last_success_at"`
	LastErrorAt      *time.Time      `gorm:"column:last_error_at" json:"last_error_at"`
	LastErrorMessage *string         `gorm:"column:last_error_message" json:"last_error_message"`
	LastCursorJSON   json.RawMessage `gorm:"column:last_cursor_json;type:jsonb" json:"last_cursor_json"`
	LastSourceTs     *int64          `gorm:"column:last_source_ts" json:"last_source_ts"`
	LastReconAt      *time.Time      `gorm:"column:last_recon_at" json:"last_recon_at"`
	ReconDriftCount  int64           `gorm:"column:recon_drift_count;default:0" json:"recon_drift_count"`
	DDLStatus        *string         `gorm:"column:ddl_status" json:"ddl_status"`
	StatsJSON        json.RawMessage `gorm:"column:stats_json;type:jsonb" json:"stats_json"`
	UpdatedAt        time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (SyncRuntimeState) TableName() string { return "cdc_system.sync_runtime_state" }
