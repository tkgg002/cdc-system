// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"
	"time"

	"cdc-cms-service/internal/model"
)

// FailedLogFilter narrows GET /api/failed-sync-logs.
//
// Empty fields are treated as "no filter" — preserves the existing
// behaviour of the pre-CQRS handler that issued conditional WHERE
// clauses based on c.Query() emptiness.
type FailedLogFilter struct {
	TargetTable string
	Status      string
	ErrorType   string
}

// ReconScopeFilter narrows resolveTargetTable on the recon HTTP handler.
// Empty fields are treated as "no filter" so the legacy resolution
// behaviour (any combination of source/shadow scoping) is preserved.
type ReconScopeFilter struct {
	SourceDatabase  string
	SourceSchema    string
	SourceNamespace string
	SourceTable     string
	ShadowSchema    string
	ShadowTable     string
}

// FailedLogRetryScope is the metadata enrichment payload echoed back
// in the RetryFailedLog HTTP response and used to seed
// commands.RetryFailedCommand fields.
type FailedLogRetryScope struct {
	SourceDatabase      *string `gorm:"column:source_database"`
	ResolvedSourceTable *string `gorm:"column:resolved_source_table"`
	ShadowSchema        *string `gorm:"column:shadow_schema"`
	ShadowTable         *string `gorm:"column:shadow_table"`
	ScopeAmbiguous      bool    `gorm:"column:scope_ambiguous"`
}

// BackfillRunRow projects one tier=4 row from `recon_runs` for the
// _source_ts backfill status page. Times stay time.Time so the JSON
// wire shape (RFC3339) is byte-identical to the legacy handler.
type BackfillRunRow struct {
	ID           string     `gorm:"column:id" json:"id"`
	TableName    string     `gorm:"column:table_name" json:"table_name"`
	Tier         int        `gorm:"column:tier" json:"tier"`
	Status       string     `gorm:"column:status" json:"status"`
	StartedAt    time.Time  `gorm:"column:started_at" json:"started_at"`
	FinishedAt   *time.Time `gorm:"column:finished_at" json:"finished_at"`
	DocsScanned  int64      `gorm:"column:docs_scanned" json:"docs_scanned"`
	HealActions  int64      `gorm:"column:heal_actions" json:"heal_actions"`
	ErrorMessage *string    `gorm:"column:error_message" json:"error_message"`
	InstanceID   *string    `gorm:"column:instance_id" json:"instance_id"`
}

// ReconReader is the read-side port for the reconciliation surface.
// Single-caller interface (only the recon HTTP handler uses it), so
// it lives next to its consumer rather than in `ports/`.
type ReconReader interface {
	// ListLatest returns the latest reconciliation report per target_table,
	// enriched via cdc_table_registry + cdc_system.shadow_binding /
	// source_object_registry. The adapter encapsulates the
	// migration-017 fallback (legacy SELECT when the new columns are
	// missing).
	ListLatest(ctx context.Context) ([]LatestReportRow, error)

	// GetTableHistory returns the paginated history of one target_table
	// ordered by checked_at DESC. Returns rows + total count.
	GetTableHistory(ctx context.Context, table string, page, pageSize int) ([]model.ReconciliationReport, int64, error)

	// ListFailedLogs returns paginated failed_sync_logs joined with
	// shadow_binding. The filter shape matches the legacy query string.
	ListFailedLogs(ctx context.Context, f FailedLogFilter, page, pageSize int) ([]FailedLogRow, int64, error)

	// ResolveTargetTableByScope resolves an active shadow_table from a
	// (source_database, source_table, …) tuple. Returns
	// gorm.ErrRecordNotFound when no row matches and a sentinel
	// ErrAmbiguousScope when more than one shadow binding qualifies.
	ResolveTargetTableByScope(ctx context.Context, f ReconScopeFilter) (string, error)

	// GetFailedLogByID loads one row from failed_sync_logs by primary
	// key. Returns gorm.ErrRecordNotFound when absent.
	GetFailedLogByID(ctx context.Context, id int64) (*model.FailedSyncLog, error)

	// GetRetryScopeByLogID returns the source/shadow enrichment for the
	// retry response and the dispatched command. Errors are silently
	// swallowed by the legacy handler — adapter mirrors that behaviour
	// by returning an empty struct rather than failing the retry.
	GetRetryScopeByLogID(ctx context.Context, id int64) (FailedLogRetryScope, error)

	// ListBackfillRuns returns the most recent tier=4 recon_runs rows,
	// optionally filtered by table and run_id. Limit is bounded server-
	// side to keep the page render cheap.
	ListBackfillRuns(ctx context.Context, table, runID string, limit int) ([]BackfillRunRow, error)

	// CountTableRows returns (total, nullSourceTs) for a single table.
	// Identifier safety is the caller's responsibility — pass tables
	// produced by the recon worker, never user-supplied names.
	CountTableRows(ctx context.Context, table string) (int64, int64, error)
}

// ErrAmbiguousScope signals that ResolveTargetTableByScope matched
// more than one active shadow_binding row. The HTTP layer maps this to
// 409 Conflict.
var ErrAmbiguousScope = errBackfill("ambiguous_reconciliation_scope")

// errBackfill keeps the error sentinel a value type so it stays
// comparable with errors.Is.
type errBackfill string

func (e errBackfill) Error() string { return string(e) }
