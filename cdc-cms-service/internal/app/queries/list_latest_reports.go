// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// ListLatestReportsQuery is the input for GET /api/reconciliation/report.
// The endpoint is currently unfiltered — the SQL DISTINCT-ONs by
// target_table and ORDERs the result alphabetically.
type ListLatestReportsQuery struct{}

func (q ListLatestReportsQuery) Type() string { return "recon.list_latest" }

// ListLatestReportsResult returns the raw rows. The HTTP handler
// fills the four `gorm:"-"` enrichment fields (DriftPct,
// ComputedStatus, ErrorMessageVI, SourceQueryMethod) from
// ComputeDriftStatus + ErrorMessagesVI + deriveSourceQueryMethod —
// those helpers stay in `internal/api/` because they own a 16-case
// unit test suite (ComputeDriftStatus_test.go) we don't want to
// move yet.
type ListLatestReportsResult struct {
	Data  []LatestReportRow
	Count int
}

// ListLatestReportsHandler resolves the query against an injected
// ReconReader.
type ListLatestReportsHandler struct {
	reader ReconReader
}

func NewListLatestReportsHandler(r ReconReader) *ListLatestReportsHandler {
	return &ListLatestReportsHandler{reader: r}
}

// Handle resolves the query.
func (h *ListLatestReportsHandler) Handle(ctx context.Context, _ ListLatestReportsQuery) (ListLatestReportsResult, error) {
	rows, err := h.reader.ListLatest(ctx)
	if err != nil {
		return ListLatestReportsResult{}, err
	}
	return ListLatestReportsResult{Data: rows, Count: len(rows)}, nil
}
