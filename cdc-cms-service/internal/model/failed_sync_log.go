package model

import (
	"encoding/json"
	"time"
)

type FailedSyncLog struct {
	ID             uint64          `gorm:"primaryKey" json:"id"`
	TargetTable    string          `gorm:"column:target_table;not null" json:"target_table"`
	SourceTable    string          `gorm:"column:source_table" json:"source_table"`
	SourceDB       string          `gorm:"column:source_db" json:"source_db"`
	RecordID       string          `gorm:"column:record_id" json:"record_id"`
	Operation      string          `gorm:"column:operation" json:"operation"`
	RawJSON        json.RawMessage `gorm:"column:raw_json;type:jsonb" json:"raw_json"`
	ErrorMessage   string          `gorm:"column:error_message;not null" json:"error_message"`
	ErrorType      string          `gorm:"column:error_type" json:"error_type"`
	KafkaTopic     string          `gorm:"column:kafka_topic" json:"kafka_topic"`
	KafkaPartition *int            `gorm:"column:kafka_partition" json:"kafka_partition"`
	KafkaOffset    *int64          `gorm:"column:kafka_offset" json:"kafka_offset"`
	RetryCount     int             `gorm:"column:retry_count;default:0" json:"retry_count"`
	MaxRetries     int             `gorm:"column:max_retries;default:3" json:"max_retries"`
	Status         string          `gorm:"column:status;default:failed" json:"status"`
	CreatedAt      time.Time       `gorm:"column:created_at" json:"created_at"`
	LastRetryAt    *time.Time      `gorm:"column:last_retry_at" json:"last_retry_at"`
	ResolvedAt     *time.Time      `gorm:"column:resolved_at" json:"resolved_at"`
	ResolvedBy     *string         `gorm:"column:resolved_by" json:"resolved_by"`
}

func (FailedSyncLog) TableName() string { return "failed_sync_logs" }
