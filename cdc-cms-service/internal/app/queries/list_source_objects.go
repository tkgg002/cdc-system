// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// ListSourceObjectsQuery is the input for GET /api/v1/source-objects.
// Page defaults to 1, PageSize to 20 (matching the V2 surface in
// `internal/api/source_objects_handler.go`); the reader clamps both.
type ListSourceObjectsQuery struct {
	Filter   SourceObjectListFilter
	Page     int
	PageSize int
}

func (q ListSourceObjectsQuery) Type() string { return "source.list" }

// ListSourceObjectsResult is what the handler returns.
type ListSourceObjectsResult struct {
	Data  []SourceObjectListItem
	Total int64
	Page  int
}

// ListSourceObjectsHandler resolves the query against an injected
// SourceObjectReader. It is stateless and constructor-injected.
type ListSourceObjectsHandler struct {
	reader SourceObjectReader
}

func NewListSourceObjectsHandler(r SourceObjectReader) *ListSourceObjectsHandler {
	return &ListSourceObjectsHandler{reader: r}
}

// Handle resolves the query. Page/pageSize defaults are normalized
// here so callers (HTTP handler, future RPC) get the same semantics.
func (h *ListSourceObjectsHandler) Handle(ctx context.Context, q ListSourceObjectsQuery) (ListSourceObjectsResult, error) {
	page, size := q.Page, q.PageSize
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	if size > 500 {
		size = 500
	}
	rows, total, err := h.reader.ListEnriched(ctx, q.Filter, page, size)
	if err != nil {
		return ListSourceObjectsResult{}, err
	}
	return ListSourceObjectsResult{
		Data:  rows,
		Total: total,
		Page:  page,
	}, nil
}
