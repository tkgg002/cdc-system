package model

import "time"

type ShadowBinding struct {
	ID                 int64     `gorm:"primaryKey" json:"id"`
	BindingCode        string    `gorm:"column:binding_code;not null" json:"binding_code"`
	SourceObjectID     int64     `gorm:"column:source_object_id;not null" json:"source_object_id"`
	ShadowConnectionID int64     `gorm:"column:shadow_connection_id;not null" json:"shadow_connection_id"`
	ShadowDatabase     *string   `gorm:"column:shadow_database" json:"shadow_database"`
	ShadowSchema       string    `gorm:"column:shadow_schema;not null" json:"shadow_schema"`
	ShadowTable        string    `gorm:"column:shadow_table;not null" json:"shadow_table"`
	PhysicalTableFQN   string    `gorm:"column:physical_table_fqn;not null" json:"physical_table_fqn"`
	NamespaceStrategy  string    `gorm:"column:namespace_strategy;not null" json:"namespace_strategy"`
	WriteMode          string    `gorm:"column:write_mode;not null" json:"write_mode"`
	DDLStatus          string    `gorm:"column:ddl_status;not null" json:"ddl_status"`
	IsActive           bool      `gorm:"column:is_active;default:true" json:"is_active"`
	CreatedAt          time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt          time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (ShadowBinding) TableName() string { return "cdc_system.shadow_binding" }
