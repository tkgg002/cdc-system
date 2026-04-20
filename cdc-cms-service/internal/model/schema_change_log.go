package model

import "time"

type SchemaChangeLog struct {
	ID                      uint      `gorm:"primaryKey" json:"id"`
	TblName                 string    `gorm:"column:table_name;not null" json:"table_name"`
	SourceDB                *string   `gorm:"column:source_db" json:"source_db"`
	ChangeType              string    `gorm:"column:change_type;not null" json:"change_type"`
	FieldName               *string   `gorm:"column:field_name" json:"field_name"`
	OldDefinition           *string   `gorm:"column:old_definition" json:"old_definition"`
	NewDefinition           *string   `gorm:"column:new_definition" json:"new_definition"`
	SQLExecuted             string    `gorm:"column:sql_executed;not null" json:"sql_executed"`
	ExecutionDurationMS     *int      `gorm:"column:execution_duration_ms" json:"execution_duration_ms"`
	Status                  string    `gorm:"column:status;default:pending" json:"status"`
	ErrorMessage            *string   `gorm:"column:error_message" json:"error_message"`
	PendingFieldID          *uint     `gorm:"column:pending_field_id" json:"pending_field_id"`
	ExecutedBy              string    `gorm:"column:executed_by;not null" json:"executed_by"`
	ExecutedAt              time.Time `gorm:"column:executed_at" json:"executed_at"`
	RollbackSQL             *string   `gorm:"column:rollback_sql" json:"rollback_sql"`
	AirbyteSourceID         *string   `gorm:"column:airbyte_source_id" json:"airbyte_source_id"`
	AirbyteRefreshTriggered bool      `gorm:"column:airbyte_refresh_triggered;default:false" json:"airbyte_refresh_triggered"`
	AirbyteRefreshStatus    *string   `gorm:"column:airbyte_refresh_status" json:"airbyte_refresh_status"`
}

func (SchemaChangeLog) TableName() string { return "schema_changes_log" }
