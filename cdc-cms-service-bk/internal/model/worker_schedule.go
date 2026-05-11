package model

import "time"

type WorkerSchedule struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	Operation       string     `gorm:"column:operation;not null" json:"operation"`
	TargetTable     *string    `gorm:"column:target_table" json:"target_table"`
	IntervalMinutes int        `gorm:"column:interval_minutes;not null;default:5" json:"interval_minutes"`
	IsEnabled       bool       `gorm:"column:is_enabled;not null;default:true" json:"is_enabled"`
	LastRunAt       *time.Time `gorm:"column:last_run_at" json:"last_run_at"`
	NextRunAt       *time.Time `gorm:"column:next_run_at" json:"next_run_at"`
	RunCount        int64      `gorm:"column:run_count;default:0" json:"run_count"`
	LastError       *string    `gorm:"column:last_error" json:"last_error"`
	Notes           *string    `gorm:"column:notes" json:"notes"`
	CreatedAt       time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at" json:"updated_at"`
}

func (WorkerSchedule) TableName() string { return "cdc_worker_schedule" }
