package model

import (
	"encoding/json"
	"time"
)

type MasterBinding struct {
	ID                 int64           `gorm:"primaryKey" json:"id"`
	BindingCode        string          `gorm:"column:binding_code;not null" json:"binding_code"`
	SourceObjectID     int64           `gorm:"column:source_object_id;not null" json:"source_object_id"`
	ShadowBindingID    *int64          `gorm:"column:shadow_binding_id" json:"shadow_binding_id"`
	MasterConnectionID int64           `gorm:"column:master_connection_id;not null" json:"master_connection_id"`
	MasterDatabase     *string         `gorm:"column:master_database" json:"master_database"`
	MasterSchema       string          `gorm:"column:master_schema;not null" json:"master_schema"`
	MasterTable        string          `gorm:"column:master_table;not null" json:"master_table"`
	PhysicalTableFQN   string          `gorm:"column:physical_table_fqn;not null" json:"physical_table_fqn"`
	TransformType      string          `gorm:"column:transform_type;not null" json:"transform_type"`
	TransformSpec      json.RawMessage `gorm:"column:transform_spec;type:jsonb" json:"transform_spec"`
	SchemaStatus       string          `gorm:"column:schema_status;not null" json:"schema_status"`
	IsActive           bool            `gorm:"column:is_active;default:false" json:"is_active"`
	SchemaReviewedBy   *string         `gorm:"column:schema_reviewed_by" json:"schema_reviewed_by"`
	SchemaReviewedAt   *time.Time      `gorm:"column:schema_reviewed_at" json:"schema_reviewed_at"`
	RejectionReason    *string         `gorm:"column:rejection_reason" json:"rejection_reason"`
	CreatedBy          *string         `gorm:"column:created_by" json:"created_by"`
	CreatedAt          time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt          time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (MasterBinding) TableName() string { return "cdc_system.master_binding" }
