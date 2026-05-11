package model

import "time"

// Source persists Connection Fingerprint after a Kafka Connect connector
// is created via POST /api/v1/system/connectors. Backs the Registry
// dropdown and the Systematic Flow wizard's step-1 selector.
// Row lifecycle: created -> running -> paused/failed, soft-delete=deleted.
type Source struct {
	ID                    int64     `gorm:"column:id;primaryKey" json:"id"`
	ConnectorName         string    `gorm:"column:connector_name;uniqueIndex;not null" json:"connector_name"`
	SourceType            string    `gorm:"column:source_type;not null" json:"source_type"`
	ConnectorClass        string    `gorm:"column:connector_class;not null" json:"connector_class"`
	TopicPrefix           string    `gorm:"column:topic_prefix" json:"topic_prefix"`
	ServerAddress         string    `gorm:"column:server_address" json:"server_address"`
	DatabaseIncludeList   string    `gorm:"column:database_include_list" json:"database_include_list"`
	CollectionIncludeList string    `gorm:"column:collection_include_list" json:"collection_include_list"`
	RawConfigSanitized    []byte    `gorm:"column:raw_config_sanitized;type:jsonb" json:"raw_config_sanitized,omitempty"`
	Status                string    `gorm:"column:status;not null;default:created" json:"status"`
	CreatedBy             string    `gorm:"column:created_by" json:"created_by"`
	CreatedAt             time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt             time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (Source) TableName() string { return "cdc_system.sources" }
