package model

import (
	"encoding/json"
	"time"
)

type ActivityLog struct {
	ID           uint64          `gorm:"primaryKey" json:"id"`
	Operation    string          `gorm:"column:operation;not null" json:"operation"`
	TargetTable  string          `gorm:"column:target_table" json:"target_table"`
	Status       string          `gorm:"column:status;default:running" json:"status"`
	RowsAffected int64           `gorm:"column:rows_affected;default:0" json:"rows_affected"`
	DurationMs   *int            `gorm:"column:duration_ms" json:"duration_ms"`
	Details      json.RawMessage `gorm:"column:details;type:jsonb" json:"details"`
	ErrorMessage *string         `gorm:"column:error_message" json:"error_message"`
	TriggeredBy  string          `gorm:"column:triggered_by;default:scheduler" json:"triggered_by"`
	StartedAt    time.Time       `gorm:"column:started_at;not null" json:"started_at"`
	CompletedAt  *time.Time      `gorm:"column:completed_at" json:"completed_at"`
	CreatedAt    time.Time       `gorm:"column:created_at" json:"created_at"`
}

func (ActivityLog) TableName() string { return "cdc_activity_log" }
