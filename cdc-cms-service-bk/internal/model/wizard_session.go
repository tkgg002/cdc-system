package model

import "time"

// WizardSession persists draft + runtime state for the Source->Master
// automation wizard. One row == one user's F5-safe journey.
type WizardSession struct {
	ID          string    `gorm:"column:id;primaryKey;type:uuid" json:"id"`
	SourceName  string    `gorm:"column:source_name" json:"source_name"`
	ConnectorID *int64    `gorm:"column:connector_id" json:"connector_id,omitempty"`
	RegistryID  *int64    `gorm:"column:registry_id" json:"registry_id,omitempty"`
	MasterName  string    `gorm:"column:master_name" json:"master_name"`
	CurrentStep int       `gorm:"column:current_step;default:0" json:"current_step"`
	Status      string    `gorm:"column:status;default:draft" json:"status"`
	StepPayload []byte    `gorm:"column:step_payload;type:jsonb" json:"step_payload,omitempty"`
	ProgressLog []byte    `gorm:"column:progress_log;type:jsonb" json:"progress_log,omitempty"`
	CreatedBy   string    `gorm:"column:created_by" json:"created_by"`
	CreatedAt   time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (WizardSession) TableName() string { return "cdc_system.cdc_wizard_sessions" }
