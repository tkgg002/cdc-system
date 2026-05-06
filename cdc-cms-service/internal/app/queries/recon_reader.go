// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"

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
}
