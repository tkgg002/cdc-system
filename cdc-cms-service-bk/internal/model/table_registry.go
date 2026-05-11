package model

import "time"

type TableRegistry struct {
	ID                  uint      `gorm:"primaryKey" json:"id"`
	SourceDB            string    `gorm:"column:source_db;not null" json:"source_db"`
	SourceType          string    `gorm:"column:source_type;not null" json:"source_type"`
	SourceTable         string    `gorm:"column:source_table;not null" json:"source_table"`
	SourceURL           string    `gorm:"column:source_url" json:"source_url"`
	TargetTable         string    `gorm:"column:target_table;not null" json:"target_table"`
	SyncEngine      string `gorm:"column:sync_engine;default:debezium" json:"sync_engine"`
	SyncInterval    string `gorm:"column:sync_interval;default:1h" json:"sync_interval"`
	Priority        string `gorm:"column:priority;default:normal" json:"priority"`
	PrimaryKeyField string `gorm:"column:primary_key_field;default:id" json:"primary_key_field"`
	PrimaryKeyType  string `gorm:"column:primary_key_type" json:"primary_key_type"`
	IsActive        bool   `gorm:"column:is_active;default:false" json:"is_active"`
	IsTableCreated  bool   `gorm:"column:is_table_created;default:false" json:"is_table_created"`
	// directive "KHÔNG DROP cột DB"). Go struct fields DROPPED to force
	// callers to migrate to Debezium path. Reads from the DB no longer
	// populate these values — queries bypass them via "-" gorm tag if
	// needed (none present now).
	SyncStatus      string `gorm:"column:sync_status;default:unknown" json:"sync_status"`
	LastReconAt              *time.Time `gorm:"column:last_recon_at" json:"last_recon_at"`
	ReconDrift               int64      `gorm:"column:recon_drift;default:0" json:"recon_drift"`
	LastBridgeAt             *time.Time `gorm:"column:last_bridge_at" json:"last_bridge_at"`
	IsPartitioned            *bool      `gorm:"column:is_partitioned;default:false" json:"is_partitioned"`
	PartitionKey             *string    `gorm:"column:partition_key;default:_synced_at" json:"partition_key"`
	// TimestampField — Mongo field used for recon window filter ($gte/$lt).
	// Migration 016 (worker). Default "updated_at"; common overrides:
	// "lastUpdatedAt", "createdAt", "updatedAt", "ts". Edited via
	// TableRegistry CMS form (Bug B fix 2026-04-20).
	TimestampField      *string    `gorm:"column:timestamp_field;default:updated_at" json:"timestamp_field"`
	CreatedAt           time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt           time.Time  `gorm:"column:updated_at" json:"updated_at"`
	Notes               *string    `gorm:"column:notes" json:"notes"`
}

func (TableRegistry) TableName() string { return "cdc_table_registry" }
