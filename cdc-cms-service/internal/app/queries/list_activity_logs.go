// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// ActivityLogFilter pins the supported query-string filters of GET
// /api/activity-log. All fields are optional; empty string means
// "no filter". The reader implementation is responsible for safe
// parameter binding.
type ActivityLogFilter struct {
	Operation      string
	Status         string
	TriggeredBy    string
	TargetTable    string
	SourceDatabase string
	SourceTable    string
	ShadowSchema   string
	ShadowTable    string
}

// ActivityLogReader is the read-side port. Single caller (the CMS
// activity-log handler), so it lives next to the query handlers.
type ActivityLogReader interface {
	ListActivity(ctx context.Context, f ActivityLogFilter, page, pageSize int) ([]ActivityLogRow, int64, error)
	Stats24h(ctx context.Context) (ops []OpStat, recentErrors []ActivityLogRow, err error)
}

// ----- ListActivityLogs --------------------------------------------

// ListActivityLogsQuery is GET /api/activity-log. Page/PageSize bounds
// match the legacy handler exactly: `page>=1, pageSize in [1,200]
// default 50` (anything out of range falls back to 50).
type ListActivityLogsQuery struct {
	Filter   ActivityLogFilter
	Page     int
	PageSize int
}

func (q ListActivityLogsQuery) Type() string { return "activitylog.list" }

// ListActivityLogsResult mirrors the legacy fiber.Map shape. `Total`
// is the unfiltered count (for pagination); `Page`/`PageSize` echo
// the request so the FE does not have to track them locally.
type ListActivityLogsResult struct {
	Data     []ActivityLogRow
	Total    int64
	Page     int
	PageSize int
}

// ListActivityLogsHandler resolves the query against the reader.
type ListActivityLogsHandler struct {
	reader ActivityLogReader
}

func NewListActivityLogsHandler(r ActivityLogReader) *ListActivityLogsHandler {
	return &ListActivityLogsHandler{reader: r}
}

func (h *ListActivityLogsHandler) Handle(ctx context.Context, q ListActivityLogsQuery) (ListActivityLogsResult, error) {
	page := q.Page
	if page < 1 {
		page = 1
	}
	size := q.PageSize
	if size < 1 || size > 200 {
		size = 50
	}
	rows, total, err := h.reader.ListActivity(ctx, q.Filter, page, size)
	if err != nil {
		return ListActivityLogsResult{}, err
	}
	return ListActivityLogsResult{
		Data:     rows,
		Total:    total,
		Page:     page,
		PageSize: size,
	}, nil
}
