// Package queries — read-side use cases (CQRS Q-side).
//
// This file defines projection-level read models for the source-object
// surface. They are NOT domain entities; the SQL projection in
// `infra/persistence/source_object_read_repo_gorm.go` enriches V2
// `cdc_system.source_object_registry` rows with the active
// `shadow_binding`, the legacy `cdc_table_registry` bridge, and the
// latest `cdc_reconciliation_report` row.
//
// Field layout matches the existing wire DTOs in
// `internal/api/source_objects_handler.go` byte-for-byte so the API
// layer can pass results straight through `c.JSON` without copying.
package queries

import "time"

// SourceObjectListItem is one row of the V2 source-object list view.
type SourceObjectListItem struct {
	ID                int64     `json:"id"`
	RegistryID        *uint     `json:"registry_id,omitempty"`
	ShadowBindingID   *int64    `json:"shadow_binding_id,omitempty"`
	ObjectCode        string    `json:"object_code"`
	SourceDB          string    `json:"source_db"`
	SourceType        string    `json:"source_type"`
	SourceTable       string    `json:"source_table"`
	TargetTable       string    `json:"target_table"`
	ShadowSchema      *string   `json:"shadow_schema,omitempty"`
	PhysicalTableFQN  *string   `json:"physical_table_fqn,omitempty"`
	SyncEngine        string    `json:"sync_engine"`
	SyncInterval      string    `json:"sync_interval"`
	Priority          string    `json:"priority"`
	PrimaryKeyField   string    `json:"primary_key_field"`
	PrimaryKeyType    string    `json:"primary_key_type"`
	TimestampField    *string   `json:"timestamp_field,omitempty"`
	IsActive          bool      `json:"is_active"`
	IsTableCreated    bool      `json:"is_table_created"`
	ProfileStatus     string    `json:"profile_status"`
	DDLStatus         *string   `json:"ddl_status,omitempty"`
	SyncStatus        string    `json:"sync_status"`
	BridgeStatus      string    `json:"bridge_status"`
	MetadataStatus    string    `json:"metadata_status"`
	ReconDrift        int64     `json:"recon_drift"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	Notes             *string   `json:"notes,omitempty"`
	ProvisioningMode  *string   `json:"provisioning_mode,omitempty"`
	ProvisioningState *string   `json:"provisioning_state,omitempty"`
	SourceEngineType  string    `json:"source_engine_type"`
}

// SourceObjectMappingContextReadModel is the single-row mapping context
// view served by GET /api/v1/source-objects/registry/{registry_id}.
type SourceObjectMappingContextReadModel struct {
	ID               int64     `json:"id"`
	RegistryID       uint      `json:"registry_id"`
	ShadowBindingID  *int64    `json:"shadow_binding_id,omitempty"`
	ObjectCode       string    `json:"object_code"`
	SourceDB         string    `json:"source_db"`
	SourceType       string    `json:"source_type"`
	SourceTable      string    `json:"source_table"`
	TargetTable      string    `json:"target_table"`
	ShadowSchema     *string   `json:"shadow_schema,omitempty"`
	PhysicalTableFQN *string   `json:"physical_table_fqn,omitempty"`
	SyncEngine       string    `json:"sync_engine"`
	SyncInterval     string    `json:"sync_interval"`
	Priority         string    `json:"priority"`
	PrimaryKeyField  string    `json:"primary_key_field"`
	PrimaryKeyType   string    `json:"primary_key_type"`
	TimestampField   *string   `json:"timestamp_field,omitempty"`
	IsActive         bool      `json:"is_active"`
	IsTableCreated   bool      `json:"is_table_created"`
	ProfileStatus    string    `json:"profile_status"`
	DDLStatus        *string   `json:"ddl_status,omitempty"`
	SyncStatus       string    `json:"sync_status"`
	BridgeStatus     string    `json:"bridge_status"`
	MetadataStatus   string    `json:"metadata_status"`
	ReconDrift       int64     `json:"recon_drift"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	Notes            *string   `json:"notes,omitempty"`
}
