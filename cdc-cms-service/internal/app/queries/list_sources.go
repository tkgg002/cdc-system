// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"

	"cdc-cms-service/internal/model"
)

// SourceReader is the read-side port for the Connection Fingerprint
// registry (Systematic Flow F-1.2/1.3). Single caller (the CMS
// sources handler).
type SourceReader interface {
	List(ctx context.Context) ([]model.Source, error)
	GetByID(ctx context.Context, id int64) (*model.Source, error)
}

// ----- ListSources -------------------------------------------------

// ListSourcesQuery is GET /api/v1/sources. Unfiltered.
type ListSourcesQuery struct{}

func (q ListSourcesQuery) Type() string { return "sources.list" }

// ListSourcesResult mirrors the legacy fiber.Map shape `{data, count}`.
type ListSourcesResult struct {
	Data  []model.Source
	Count int
}

type ListSourcesHandler struct {
	reader SourceReader
}

func NewListSourcesHandler(r SourceReader) *ListSourcesHandler {
	return &ListSourcesHandler{reader: r}
}

func (h *ListSourcesHandler) Handle(ctx context.Context, _ ListSourcesQuery) (ListSourcesResult, error) {
	rows, err := h.reader.List(ctx)
	if err != nil {
		return ListSourcesResult{}, err
	}
	return ListSourcesResult{Data: rows, Count: len(rows)}, nil
}

// ----- GetSource ---------------------------------------------------

// GetSourceQuery is GET /api/v1/sources/:id.
type GetSourceQuery struct {
	ID int64
}

func (q GetSourceQuery) Type() string { return "sources.get" }

// GetSourceResult is the projected single-source view. The handler
// returns the model verbatim — no transformation needed for byte-
// identical wire output.
type GetSourceResult struct {
	Source *model.Source
}

type GetSourceHandler struct {
	reader SourceReader
}

func NewGetSourceHandler(r SourceReader) *GetSourceHandler {
	return &GetSourceHandler{reader: r}
}

func (h *GetSourceHandler) Handle(ctx context.Context, q GetSourceQuery) (GetSourceResult, error) {
	s, err := h.reader.GetByID(ctx, q.ID)
	if err != nil {
		return GetSourceResult{}, err
	}
	return GetSourceResult{Source: s}, nil
}
