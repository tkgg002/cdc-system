package model

import (
	"encoding/json"
	"time"
)

type SourceObjectRegistry struct {
	ID                      int64           `gorm:"primaryKey" json:"id"`
	ObjectCode              string          `gorm:"column:object_code;not null" json:"object_code"`
	SourceConnectionID      int64           `gorm:"column:source_connection_id;not null" json:"source_connection_id"`
	SourceEngineType        string          `gorm:"column:source_engine_type;not null" json:"source_engine_type"`
	SourceDatabase          *string         `gorm:"column:source_database" json:"source_database"`
	SourceSchema            *string         `gorm:"column:source_schema" json:"source_schema"`
	SourceNamespace         *string         `gorm:"column:source_namespace" json:"source_namespace"`
	SourceObjectName        string          `gorm:"column:source_object_name;not null" json:"source_object_name"`
	SourceObjectType        string          `gorm:"column:source_object_type;not null" json:"source_object_type"`
	SourceLocatorJSON       json.RawMessage `gorm:"column:source_locator_json;type:jsonb" json:"source_locator_json"`
	NormalizedSourceKey     string          `gorm:"column:normalized_source_key;not null" json:"normalized_source_key"`
	PrimaryKeyField         string          `gorm:"column:primary_key_field;not null" json:"primary_key_field"`
	PrimaryKeyType          *string         `gorm:"column:primary_key_type" json:"primary_key_type"`
	TimestampField          *string         `gorm:"column:timestamp_field" json:"timestamp_field"`
	TimestampCandidatesJSON json.RawMessage `gorm:"column:timestamp_candidates_json;type:jsonb" json:"timestamp_candidates_json"`
	CDCMode                 string          `gorm:"column:cdc_mode;not null" json:"cdc_mode"`
	SyncEngine              string          `gorm:"column:sync_engine;not null" json:"sync_engine"`
	IsActive                bool            `gorm:"column:is_active;default:true" json:"is_active"`
	ProfileStatus           string          `gorm:"column:profile_status;not null" json:"profile_status"`
	Notes                   *string         `gorm:"column:notes" json:"notes"`
	// Migration 047 — provisioning state machine (auto/manual mode).
	// Decisions in workspace feature-cdc-integration/04_decisions_provisioning_mode.md.
	ProvisioningMode    string          `gorm:"column:provisioning_mode;default:manual" json:"provisioning_mode"`
	ProvisioningState   string          `gorm:"column:provisioning_state;default:draft" json:"provisioning_state"`
	ProvisioningStepLog json.RawMessage `gorm:"column:provisioning_step_log;type:jsonb" json:"provisioning_step_log"`
	LastStepError       *string         `gorm:"column:last_step_error" json:"last_step_error"`
	CreatedAt           time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt           time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (SourceObjectRegistry) TableName() string { return "cdc_system.source_object_registry" }
