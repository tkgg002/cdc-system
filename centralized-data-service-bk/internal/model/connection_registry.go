package model

import (
	"encoding/json"
	"time"
)

type ConnectionRegistry struct {
	ID               int64           `gorm:"primaryKey" json:"id"`
	ConnectionCode   string          `gorm:"column:connection_code;not null" json:"connection_code"`
	DisplayName      string          `gorm:"column:display_name;not null" json:"display_name"`
	RoleType         string          `gorm:"column:role_type;not null" json:"role_type"`
	EngineType       string          `gorm:"column:engine_type;not null" json:"engine_type"`
	Host             *string         `gorm:"column:host" json:"host"`
	Port             *int            `gorm:"column:port" json:"port"`
	DefaultDatabase  *string         `gorm:"column:default_database" json:"default_database"`
	DefaultSchema    *string         `gorm:"column:default_schema" json:"default_schema"`
	SecretRef        string          `gorm:"column:secret_ref;not null" json:"secret_ref"`
	OptionsJSON      json.RawMessage `gorm:"column:options_json;type:jsonb" json:"options_json"`
	CapabilitiesJSON json.RawMessage `gorm:"column:capabilities_json;type:jsonb" json:"capabilities_json"`
	Status           string          `gorm:"column:status;not null" json:"status"`
	CreatedBy        *string         `gorm:"column:created_by" json:"created_by"`
	CreatedAt        time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt        time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (ConnectionRegistry) TableName() string { return "cdc_system.connection_registry" }
