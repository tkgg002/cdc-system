// Package queries — read-side use cases (CQRS Q-side).
//
// Each query is a small struct with input validation; the matching
// handler resolves it via injected ports.* repos. Handlers are stateless
// and constructor-injected. They MUST NOT reach for *gorm.DB directly —
// repository ports are the only contact with the persistence layer.
package queries

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/mapping"
)

// ListMappingRulesQuery is the input for /api/mapping-rules list.
// Page defaults to 1, PageSize to 50 if zero/negative — the repo
// enforces the same defaults so the handler does not have to pre-clamp.
type ListMappingRulesQuery struct {
	Filter   mapping.Filter
	Page     int
	PageSize int
}

func (q ListMappingRulesQuery) Type() string { return "mapping.list" }

// ListMappingRulesResult is what the handler returns.
type ListMappingRulesResult struct {
	Data     []mapping.Rule
	Total    int64
	Page     int
	PageSize int
}

// ListMappingRulesHandler is the use-case handler. Constructor-injected
// with the MappingRuleRepo port.
type ListMappingRulesHandler struct {
	repo ports.MappingRuleRepo
}

func NewListMappingRulesHandler(r ports.MappingRuleRepo) *ListMappingRulesHandler {
	return &ListMappingRulesHandler{repo: r}
}

// Handle resolves the query against the repo. The repo enforces the
// page/pageSize defaults so callers get consistent paging semantics.
func (h *ListMappingRulesHandler) Handle(ctx context.Context, q ListMappingRulesQuery) (ListMappingRulesResult, error) {
	rules, total, err := h.repo.ListPaginated(ctx, q.Filter, q.Page, q.PageSize)
	if err != nil {
		return ListMappingRulesResult{}, err
	}
	page, size := q.Page, q.PageSize
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 200 {
		size = 50
	}
	return ListMappingRulesResult{
		Data:     rules,
		Total:    total,
		Page:     page,
		PageSize: size,
	}, nil
}
