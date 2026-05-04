package model

import (
	"encoding/json"
	"time"
)

type TransmuteSchedule struct {
	ID              int64           `gorm:"primaryKey" json:"id"`
	MasterBindingID int64           `gorm:"column:master_binding_id;not null" json:"master_binding_id"`
	Mode            string          `gorm:"column:mode;not null" json:"mode"`
	CronExpr        *string         `gorm:"column:cron_expr" json:"cron_expr"`
	LastRunAt       *time.Time      `gorm:"column:last_run_at" json:"last_run_at"`
	NextRunAt       *time.Time      `gorm:"column:next_run_at" json:"next_run_at"`
	LastStatus      *string         `gorm:"column:last_status" json:"last_status"`
	LastError       *string         `gorm:"column:last_error" json:"last_error"`
	LastStats       json.RawMessage `gorm:"column:last_stats;type:jsonb" json:"last_stats"`
	IsEnabled       bool            `gorm:"column:is_enabled;default:false" json:"is_enabled"`
	CreatedBy       *string         `gorm:"column:created_by" json:"created_by"`
	CreatedAt       time.Time       `gorm:"column:created_at" json:"created_at"`
	UpdatedAt       time.Time       `gorm:"column:updated_at" json:"updated_at"`
}

func (TransmuteSchedule) TableName() string { return "cdc_system.transmute_schedule" }
