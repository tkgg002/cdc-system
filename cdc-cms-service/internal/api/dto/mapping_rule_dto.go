package dto

import (
	"time"

	"cdc-cms-service/internal/domain/mapping"
)

// MappingRuleRow represents the JSON format used in the API response.
type MappingRuleRow struct {
	ID              int64   `json:"id"`
	SourceObjectID  int64   `json:"source_object_id"`
	MasterBindingID *int64  `json:"master_binding_id,omitempty"`
	SourceDatabase  *string `json:"source_database,omitempty"`
	SourceSchema    *string `json:"source_schema,omitempty"`
	SourceNamespace *string `json:"source_namespace,omitempty"`
	SourceTable     string  `json:"source_table"`
	ShadowSchema    *string `json:"shadow_schema,omitempty"`
	ShadowTable     *string `json:"shadow_table,omitempty"`
	SourceField     string  `json:"source_field"`
	SourcePath      *string `json:"source_path,omitempty"`
	TargetColumn    string  `json:"target_column"`
	DataType        string  `json:"data_type"`
	SourceFormat    string  `json:"source_format"`
	TransformFn     *string `json:"transform_fn,omitempty"`
	IsNullable      bool    `json:"is_nullable"`
	IsActive        bool    `json:"is_active"`
	Status          string  `json:"status"`
	Notes           *string `json:"notes,omitempty"`
	CreatedBy       *string `json:"created_by,omitempty"`
	UpdatedBy       *string `json:"updated_by,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
	RuleType        string  `json:"rule_type"`
	IsEnriched      bool    `json:"is_enriched"`
}

// MappingRuleCreateRequest represents the request payload to create a mapping rule.
type MappingRuleCreateRequest struct {
	SourceObjectID  *int64  `json:"source_object_id"`
	MasterBindingID *int64  `json:"master_binding_id"`
	SourceDatabase  *string `json:"source_database"`
	SourceSchema    *string `json:"source_schema"`
	SourceNamespace *string `json:"source_namespace"`
	SourceTable     string  `json:"source_table"`
	ShadowSchema    *string `json:"shadow_schema"`
	ShadowTable     *string `json:"shadow_table"`
	SourceField     string  `json:"source_field"`
	SourcePath      *string `json:"source_path"`
	TargetColumn    string  `json:"target_column"`
	DataType        string  `json:"data_type"`
	SourceFormat    string  `json:"source_format"`
	TransformFn     *string `json:"transform_fn"`
	IsNullable      *bool   `json:"is_nullable"`
	IsActive        *bool   `json:"is_active"`
	Status          string  `json:"status"`
	Notes           *string `json:"notes"`
}

type MappingRuleBatchUpdateRequest struct {
	IDs          []uint `json:"ids"`
	Status       string `json:"status"`
	AutoBackfill bool   `json:"auto_backfill"`
}

// FormatPgOF formats a Go time object to match Postgres TO_CHAR YYYY-MM-DD"T"HH24:MI:SSOF
func FormatPgOF(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05") + "+00"
}

// RuleToRow converts a mapping domain Rule into the legacy row JSON representation.
func RuleToRow(r mapping.Rule) MappingRuleRow {
	return MappingRuleRow{
		ID:              r.ID,
		SourceObjectID:  r.SourceObjectID,
		MasterBindingID: r.MasterBindingID,
		SourceDatabase:  r.SourceDatabase,
		SourceSchema:    r.SourceSchema,
		SourceNamespace: r.SourceNamespace,
		SourceTable:     r.SourceTable,
		ShadowSchema:    r.ShadowSchema,
		ShadowTable:     r.ShadowTable,
		SourceField:     r.SourceField,
		SourcePath:      r.SourcePath,
		TargetColumn:    r.TargetColumn,
		DataType:        r.DataType,
		SourceFormat:    r.SourceFormat,
		TransformFn:     r.TransformFn,
		IsNullable:      r.IsNullable,
		IsActive:        r.IsActive,
		Status:          string(r.Status),
		Notes:           r.Notes,
		CreatedBy:       r.CreatedBy,
		UpdatedBy:       r.UpdatedBy,
		CreatedAt:       FormatPgOF(r.CreatedAt),
		UpdatedAt:       FormatPgOF(r.UpdatedAt),
		RuleType:        "mapping",
		IsEnriched:      r.IsEnriched,
	}
}
