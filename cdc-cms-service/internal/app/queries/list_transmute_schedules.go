// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"
	"encoding/json"
	"time"
)

// TransmuteScheduleRow is the projection for GET /api/v1/schedules.
// JSON tags pin the wire contract; the legacy handler in
// `internal/api/transmute_schedule_handler.go` re-exports this via
// type alias for Swagger compatibility.
type TransmuteScheduleRow struct {
	ID          int64           `json:"id"`
	MasterTable string          `json:"master_table"`
	Mode        string          `json:"mode"`
	CronExpr    *string         `json:"cron_expr,omitempty"`
	LastRunAt   *time.Time      `json:"last_run_at,omitempty"`
	NextRunAt   *time.Time      `json:"next_run_at,omitempty"`
	LastStatus  *string         `json:"last_status,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	LastStats   json.RawMessage `json:"last_stats,omitempty"`
	IsEnabled   bool            `json:"is_enabled"`
	CreatedBy   *string         `json:"created_by,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// TransmuteScheduleReader is the read-side port. Single caller (the
// CMS schedules handler), so it lives next to the query handler.
type TransmuteScheduleReader interface {
	ListSchedules(ctx context.Context) ([]TransmuteScheduleRow, error)
}

// ----- ListTransmuteSchedules --------------------------------------

// ListTransmuteSchedulesQuery is GET /api/v1/schedules. Unfiltered.
type ListTransmuteSchedulesQuery struct{}

func (q ListTransmuteSchedulesQuery) Type() string { return "transmute_schedules.list" }

// ListTransmuteSchedulesResult mirrors the legacy fiber.Map shape.
type ListTransmuteSchedulesResult struct {
	Data  []TransmuteScheduleRow
	Count int
}

// ListTransmuteSchedulesHandler resolves the query.
type ListTransmuteSchedulesHandler struct {
	reader TransmuteScheduleReader
}

func NewListTransmuteSchedulesHandler(r TransmuteScheduleReader) *ListTransmuteSchedulesHandler {
	return &ListTransmuteSchedulesHandler{reader: r}
}

func (h *ListTransmuteSchedulesHandler) Handle(ctx context.Context, _ ListTransmuteSchedulesQuery) (ListTransmuteSchedulesResult, error) {
	rows, err := h.reader.ListSchedules(ctx)
	if err != nil {
		return ListTransmuteSchedulesResult{}, err
	}
	return ListTransmuteSchedulesResult{Data: rows, Count: len(rows)}, nil
}
