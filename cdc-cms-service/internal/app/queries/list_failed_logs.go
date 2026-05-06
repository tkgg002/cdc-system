// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// ListFailedLogsQuery is the input for GET /api/failed-sync-logs.
// Page defaults to 1, PageSize to 30, max 200 (matches the legacy
// handler in internal/api/reconciliation_handler.go::ListFailedLogs).
type ListFailedLogsQuery struct {
	Filter   FailedLogFilter
	Page     int
	PageSize int
}

func (q ListFailedLogsQuery) Type() string { return "recon.failed_logs" }

// ListFailedLogsResult returns the page of failed-log rows + total count.
type ListFailedLogsResult struct {
	Data  []FailedLogRow
	Total int64
	Page  int
}

// ListFailedLogsHandler resolves the query against an injected
// ReconReader.
type ListFailedLogsHandler struct {
	reader ReconReader
}

func NewListFailedLogsHandler(r ReconReader) *ListFailedLogsHandler {
	return &ListFailedLogsHandler{reader: r}
}

// Handle clamps page/pageSize to the legacy bounds (page>=1,
// pageSize 1..200 default 30).
func (h *ListFailedLogsHandler) Handle(ctx context.Context, q ListFailedLogsQuery) (ListFailedLogsResult, error) {
	page, size := q.Page, q.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 30
	}
	rows, total, err := h.reader.ListFailedLogs(ctx, q.Filter, page, size)
	if err != nil {
		return ListFailedLogsResult{}, err
	}
	return ListFailedLogsResult{Data: rows, Total: total, Page: page}, nil
}
