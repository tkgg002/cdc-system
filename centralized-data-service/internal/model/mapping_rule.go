package model

import "time"

type MappingRule struct {
	ID                  uint      `gorm:"primaryKey" json:"id"`
	SourceTable         string    `gorm:"column:source_table;not null" json:"source_table"`
	SourceField         string    `gorm:"column:source_field;not null" json:"source_field"`
	TargetColumn        string    `gorm:"column:target_column;not null" json:"target_column"`
	DataType            string    `gorm:"column:data_type;not null" json:"data_type"`
	IsActive            bool      `gorm:"column:is_active;default:true" json:"is_active"`
	IsEnriched          bool      `gorm:"column:is_enriched;default:false" json:"is_enriched"`
	Status              string    `gorm:"column:status;default:pending" json:"status"`
	RuleType            string    `gorm:"column:rule_type;default:mapping" json:"rule_type"`
	IsNullable          bool      `gorm:"column:is_nullable;default:true" json:"is_nullable"`
	DefaultValue        *string   `gorm:"column:default_value" json:"default_value"`
	EnrichmentFunction  *string   `gorm:"column:enrichment_function" json:"enrichment_function"`
	CreatedAt           time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt           time.Time `gorm:"column:updated_at" json:"updated_at"`
	CreatedBy           *string   `gorm:"column:created_by" json:"created_by"`
	UpdatedBy           *string   `gorm:"column:updated_by" json:"updated_by"`
	Notes               *string   `gorm:"column:notes" json:"notes"`
}

func (MappingRule) TableName() string { return "cdc_mapping_rules" }
