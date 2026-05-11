package model

import "time"

type PendingField struct {
	ID               uint       `gorm:"primaryKey" json:"id"`
	TblName          string     `gorm:"column:table_name;not null" json:"table_name"`
	SourceDB         *string    `gorm:"column:source_db" json:"source_db"`
	FieldName        string     `gorm:"column:field_name;not null" json:"field_name"`
	SampleValue      *string    `gorm:"column:sample_value" json:"sample_value"`
	SuggestedType    string     `gorm:"column:suggested_type;not null" json:"suggested_type"`
	FinalType        *string    `gorm:"column:final_type" json:"final_type"`
	Status           string     `gorm:"column:status;default:pending" json:"status"`
	DetectedAt       time.Time  `gorm:"column:detected_at" json:"detected_at"`
	ReviewedAt       *time.Time `gorm:"column:reviewed_at" json:"reviewed_at"`
	ApprovedAt       *time.Time `gorm:"column:approved_at" json:"approved_at"`
	ReviewedBy       *string    `gorm:"column:reviewed_by" json:"reviewed_by"`
	ApprovalNotes    *string    `gorm:"column:approval_notes" json:"approval_notes"`
	RejectionReason  *string    `gorm:"column:rejection_reason" json:"rejection_reason"`
	TargetColumnName *string    `gorm:"column:target_column_name" json:"target_column_name"`
	DetectionCount   int        `gorm:"column:detection_count;default:1" json:"detection_count"`
}

func (PendingField) TableName() string { return "pending_fields" }
