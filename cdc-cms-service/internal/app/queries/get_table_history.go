// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"

	"cdc-cms-service/internal/model"
)

// GetTableHistoryQuery is the input for GET /api/reconciliation/report/:table.
// Page defaults to 1, PageSize to 20, max 100 (matches the legacy
// handler in internal/api/reconciliation_handler.go::TableHistory).
type GetTableHistoryQuery struct {
	Table    string
	Page     int
	PageSize int
}

func (q GetTableHistoryQuery) Type() string { return "recon.history" }

// GetTableHistoryResult returns the page of model rows + total count.
type GetTableHistoryResult struct {
	Data  []model.ReconciliationReport
	Total int64
	Page  int
}

// GetTableHistoryHandler resolves the query against an injected
// ReconReader.
type GetTableHistoryHandler struct {
	reader ReconReader
}

func NewGetTableHistoryHandler(r ReconReader) *GetTableHistoryHandler {
	return &GetTableHistoryHandler{reader: r}
}

// Handle clamps page/pageSize to the same bounds the legacy handler
// applied (page>=1, pageSize 1..100 default 20).
func (h *GetTableHistoryHandler) Handle(ctx context.Context, q GetTableHistoryQuery) (GetTableHistoryResult, error) {
	page, size := q.Page, q.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	rows, total, err := h.reader.GetTableHistory(ctx, q.Table, page, size)
	if err != nil {
		return GetTableHistoryResult{}, err
	}
	return GetTableHistoryResult{Data: rows, Total: total, Page: page}, nil
}
