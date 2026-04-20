package model

import (
	"encoding/json"
	"time"
)

type TableRegistry struct {
	ID                  uint      `gorm:"primaryKey" json:"id"`
	SourceDB            string    `gorm:"column:source_db;not null" json:"source_db"`
	SourceType          string    `gorm:"column:source_type;not null" json:"source_type"`
	SourceTable         string    `gorm:"column:source_table;not null" json:"source_table"`
	SourceURL           string    `gorm:"column:source_url" json:"source_url"`
	TargetTable         string    `gorm:"column:target_table;not null" json:"target_table"`
	SyncEngine          string    `gorm:"column:sync_engine;default:airbyte" json:"sync_engine"`
	SyncInterval        string    `gorm:"column:sync_interval;default:1h" json:"sync_interval"`
	Priority            string    `gorm:"column:priority;default:normal" json:"priority"`
	PrimaryKeyField     string    `gorm:"column:primary_key_field;default:id" json:"primary_key_field"`
	PrimaryKeyType      string    `gorm:"column:primary_key_type" json:"primary_key_type"`
	IsActive            bool      `gorm:"column:is_active;default:true" json:"is_active"`
	IsTableCreated      bool      `gorm:"column:is_table_created;default:false" json:"is_table_created"`
	AirbyteConnectionID      *string    `gorm:"column:airbyte_connection_id" json:"airbyte_connection_id"`
	AirbyteSourceID          *string    `gorm:"column:airbyte_source_id" json:"airbyte_source_id"`
	AirbyteDestinationID     *string    `gorm:"column:airbyte_destination_id" json:"airbyte_destination_id"`
	AirbyteDestinationName   *string    `gorm:"column:airbyte_destination_name" json:"airbyte_destination_name"`
	AirbyteRawTable          *string    `gorm:"column:airbyte_raw_table" json:"airbyte_raw_table"`
	AirbyteSyncMode          *string    `gorm:"column:airbyte_sync_mode" json:"airbyte_sync_mode"`
	AirbyteDestinationSync   *string    `gorm:"column:airbyte_destination_sync_mode" json:"airbyte_destination_sync_mode"`
	AirbyteCursorField       *string    `gorm:"column:airbyte_cursor_field" json:"airbyte_cursor_field"`
	AirbyteNamespace         *string    `gorm:"column:airbyte_namespace" json:"airbyte_namespace"`
	SyncStatus               string     `gorm:"column:sync_status;default:unknown" json:"sync_status"`
	LastReconAt              *time.Time `gorm:"column:last_recon_at" json:"last_recon_at"`
	ReconDrift               int64      `gorm:"column:recon_drift;default:0" json:"recon_drift"`
	LastBridgeAt             *time.Time `gorm:"column:last_bridge_at" json:"last_bridge_at"`
	IsPartitioned            *bool      `gorm:"column:is_partitioned;default:false" json:"is_partitioned"`
	PartitionKey             *string    `gorm:"column:partition_key;default:_synced_at" json:"partition_key"`
	// TimestampField — Mongo field used for recon window filter ($gte/$lt).
	// Migration 016. Default "updated_at"; override to "lastUpdatedAt",
	// "createdAt", "updatedAt", "ts" for collections that use a different
	// naming convention. Empty string + "_id" means fallback to ObjectID
	// timestamp extraction (recon_source_agent handles the auto-detect).
	TimestampField      *string    `gorm:"column:timestamp_field;default:updated_at" json:"timestamp_field"`
	// Migration 017 — systematic auto-detect. `TimestampFieldCandidates`
	// is the ordered candidate chain the detector probes against a
	// sample of the source collection. Empty/NULL = fall back to the
	// JSON default (`updated_at,updatedAt,created_at,createdAt`).
	// `TimestampFieldSource='admin_override'` pins the value — detector
	// must NOT overwrite. `TimestampFieldConfidence` is one of
	// {high, medium, low} computed from sample coverage.
	TimestampFieldCandidates json.RawMessage `gorm:"column:timestamp_field_candidates;type:jsonb" json:"timestamp_field_candidates"`
	TimestampFieldDetectedAt *time.Time      `gorm:"column:timestamp_field_detected_at" json:"timestamp_field_detected_at"`
	TimestampFieldSource     *string         `gorm:"column:timestamp_field_source;default:auto" json:"timestamp_field_source"`
	TimestampFieldConfidence *string         `gorm:"column:timestamp_field_confidence" json:"timestamp_field_confidence"`
	// Migration 017 §2.7 — daily full-count aggregator output. Used by
	// FE Total Source / Total Dest columns and drift-truth decisions.
	FullSourceCount *int64     `gorm:"column:full_source_count" json:"full_source_count"`
	FullDestCount   *int64     `gorm:"column:full_dest_count" json:"full_dest_count"`
	FullCountAt     *time.Time `gorm:"column:full_count_at" json:"full_count_at"`
	CreatedAt           time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt           time.Time  `gorm:"column:updated_at" json:"updated_at"`
	Notes               *string    `gorm:"column:notes" json:"notes"`
	// v3 Phase 2/3 — Schema validator Phase A (migration 013)
	ExpectedFields json.RawMessage `gorm:"column:expected_fields;type:jsonb" json:"expected_fields"`
	// Migration 014 — per-table sensitive field list consumed by DLQ
	// / recon-heal paths when masking raw_json. JSON array of field
	// names. Empty = no masking for this table.
	SensitiveFields json.RawMessage `gorm:"column:sensitive_fields;type:jsonb" json:"sensitive_fields"`
}

// GetCandidates returns the candidate chain for TimestampDetector. Falls
// back to the standard default chain when the JSON column is unset or
// empty — keeps detector code free of null-handling sprawl.
func (r *TableRegistry) GetCandidates() []string {
	def := []string{"updated_at", "updatedAt", "created_at", "createdAt"}
	if len(r.TimestampFieldCandidates) == 0 {
		return def
	}
	var out []string
	if err := json.Unmarshal(r.TimestampFieldCandidates, &out); err != nil || len(out) == 0 {
		return def
	}
	return out
}

func (TableRegistry) TableName() string { return "cdc_table_registry" }
