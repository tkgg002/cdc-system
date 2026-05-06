// Package queries — read-side use cases (CQRS Q-side).
//
// Reconciliation reads cross 3 tables:
//   - cdc_reconciliation_report (legacy public schema)
//   - cdc_system.shadow_binding + source_object_registry (V2 enrichment)
//   - cdc_table_registry (V1 sync engine metadata)
//
// The two HTTP endpoints (LatestReport list + ListFailedLogs) embed the
// canonical model + a band of optional enrichment columns produced by
// the join. We keep the row types here (not in domain/) because they
// are projection-level shapes — not domain entities — and because the
// FE wire contract pins the JSON tags (must be byte-identical with the
// pre-CQRS payload from internal/api/reconciliation_handler.go).
package queries

import (
	"time"

	"cdc-cms-service/internal/model"
)

// LatestReportRow is one row of GET /api/reconciliation/report. It
// embeds the canonical model.ReconciliationReport plus enrichment
// columns surfaced via LATERAL joins on shadow_binding /
// source_object_registry / cdc_table_registry. The four `gorm:"-"`
// fields are zero on SQL exit and filled by the enrichment loop in
// internal/api/reconciliation_handler.go (ComputeDriftStatus +
// ErrorMessagesVI + deriveSourceQueryMethod). They are part of the
// JSON wire contract — moving them later would shift the surface.
type LatestReportRow struct {
	model.ReconciliationReport
	SourceObjectID           *int64     `gorm:"column:source_object_id" json:"source_object_id,omitempty"`
	SourceTable              *string    `gorm:"column:source_table" json:"source_table,omitempty"`
	ShadowSchema             *string    `gorm:"column:shadow_schema" json:"shadow_schema,omitempty"`
	ShadowTable              *string    `gorm:"column:shadow_table" json:"shadow_table,omitempty"`
	ScopeAmbiguous           bool       `gorm:"column:scope_ambiguous" json:"scope_ambiguous"`
	SyncEngine               *string    `gorm:"column:sync_engine" json:"sync_engine"`
	SourceType               *string    `gorm:"column:source_type" json:"source_type"`
	TimestampField           *string    `gorm:"column:timestamp_field" json:"timestamp_field"`
	TimestampFieldSource     *string    `gorm:"column:timestamp_field_source" json:"timestamp_field_source,omitempty"`
	TimestampFieldConfidence *string    `gorm:"column:timestamp_field_confidence" json:"timestamp_field_confidence,omitempty"`
	FullSourceCount          *int64     `gorm:"column:full_source_count" json:"full_source_count,omitempty"`
	FullDestCount            *int64     `gorm:"column:full_dest_count" json:"full_dest_count,omitempty"`
	FullCountAt              *time.Time `gorm:"column:full_count_at" json:"full_count_at,omitempty"`
	NullableSourceCount      *int64     `gorm:"column:nullable_source_count" json:"nullable_source_count,omitempty"`
	ErrorCode                *string    `gorm:"column:error_code" json:"error_code,omitempty"`
	ErrorMessageVI           string     `gorm:"-" json:"error_message_vi,omitempty"`
	DriftPct                 float64    `gorm:"-" json:"drift_pct"`
	ComputedStatus           string     `gorm:"-" json:"computed_status"`
	SourceQueryMethod        string     `gorm:"-" json:"source_query_method"`
}

// FailedLogRow is one row of GET /api/failed-sync-logs. Embeds the
// canonical model.FailedSyncLog plus shadow-binding-resolved scope.
type FailedLogRow struct {
	model.FailedSyncLog
	ResolvedSourceTable *string `gorm:"column:resolved_source_table" json:"resolved_source_table,omitempty"`
	ShadowSchema        *string `gorm:"column:shadow_schema" json:"shadow_schema,omitempty"`
	ShadowTable         *string `gorm:"column:shadow_table" json:"shadow_table,omitempty"`
	ScopeAmbiguous      bool    `gorm:"column:scope_ambiguous" json:"scope_ambiguous"`
}
