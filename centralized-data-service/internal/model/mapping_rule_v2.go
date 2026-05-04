package model

import "time"

type MappingRuleV2 struct {
	ID              int64     `gorm:"primaryKey" json:"id"`
	SourceObjectID  int64     `gorm:"column:source_object_id;not null" json:"source_object_id"`
	MasterBindingID *int64    `gorm:"column:master_binding_id" json:"master_binding_id"`
	SourceField     string    `gorm:"column:source_field;not null" json:"source_field"`
	SourcePath      *string   `gorm:"column:source_path" json:"source_path"`
	TargetColumn    string    `gorm:"column:target_column;not null" json:"target_column"`
	DataType        string    `gorm:"column:data_type;not null" json:"data_type"`
	SourceFormat    string    `gorm:"column:source_format;not null" json:"source_format"`
	TransformFn     *string   `gorm:"column:transform_fn" json:"transform_fn"`
	IsNullable      bool      `gorm:"column:is_nullable;default:true" json:"is_nullable"`
	DefaultValue    *string   `gorm:"column:default_value" json:"default_value"`
	IsActive        bool      `gorm:"column:is_active;default:true" json:"is_active"`
	Status          string    `gorm:"column:status;not null" json:"status"`
	Notes           *string   `gorm:"column:notes" json:"notes"`
	CreatedBy       *string   `gorm:"column:created_by" json:"created_by"`
	UpdatedBy       *string   `gorm:"column:updated_by" json:"updated_by"`
	CreatedAt       time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (MappingRuleV2) TableName() string { return "cdc_system.mapping_rule_v2" }
