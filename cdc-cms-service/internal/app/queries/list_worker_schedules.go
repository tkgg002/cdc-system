// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"
	"time"
)

// WorkerScheduleScope is the V2 scope projection (LEFT JOIN LATERAL on
// shadow_binding + source_object_registry). `ScopeAmbiguous=true` when
// more than one shadow_binding row matches the same target_table.
type WorkerScheduleScope struct {
	SourceObjectID   *int64  `json:"source_object_id,omitempty"`
	SourceDatabase   *string `json:"source_database,omitempty"`
	SourceSchema     *string `json:"source_schema,omitempty"`
	SourceNamespace  *string `json:"source_namespace,omitempty"`
	SourceTable      *string `json:"source_table,omitempty"`
	ShadowBindingID  *int64  `json:"shadow_binding_id,omitempty"`
	ShadowSchema     *string `json:"shadow_schema,omitempty"`
	ShadowTable      *string `json:"shadow_table,omitempty"`
	PhysicalTableFQN *string `json:"physical_table_fqn,omitempty"`
	ScopeAmbiguous   bool    `json:"scope_ambiguous"`
}

// WorkerScheduleResponse is the projection for GET /api/worker-schedule.
// JSON tags pin the wire contract; the legacy handler in
// `internal/api/schedule_handler.go` re-exports this via type alias.
type WorkerScheduleResponse struct {
	ID              uint                `json:"id"`
	Operation       string              `json:"operation"`
	TargetTable     *string             `json:"target_table"`
	IntervalMinutes int                 `json:"interval_minutes"`
	IsEnabled       bool                `json:"is_enabled"`
	LastRunAt       *time.Time          `json:"last_run_at"`
	NextRunAt       *time.Time          `json:"next_run_at"`
	RunCount        int64               `json:"run_count"`
	LastError       *string             `json:"last_error"`
	Notes           *string             `json:"notes"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	Scope           WorkerScheduleScope `json:"scope"`
}

// WorkerScheduleReader is the read-side port. It is shared by the
// list endpoint AND the write endpoint (Update returns the same shape
// after mutating), so the port lives at the queries-package level
// rather than being collapsed into a single handler.
type WorkerScheduleReader interface {
	ListResponses(ctx context.Context) ([]WorkerScheduleResponse, error)
	GetResponseByID(ctx context.Context, id uint) (*WorkerScheduleResponse, error)
}

// ----- ListWorkerSchedules -----------------------------------------

// ListWorkerSchedulesQuery is GET /api/worker-schedule. Unfiltered.
type ListWorkerSchedulesQuery struct{}

func (q ListWorkerSchedulesQuery) Type() string { return "worker_schedules.list" }

// ListWorkerSchedulesResult mirrors the legacy fiber.Map shape `{data}`
// (the legacy handler did NOT include `count`, so we don't either —
// byte-identical contract).
type ListWorkerSchedulesResult struct {
	Data []WorkerScheduleResponse
}

// ListWorkerSchedulesHandler resolves the query.
type ListWorkerSchedulesHandler struct {
	reader WorkerScheduleReader
}

func NewListWorkerSchedulesHandler(r WorkerScheduleReader) *ListWorkerSchedulesHandler {
	return &ListWorkerSchedulesHandler{reader: r}
}

func (h *ListWorkerSchedulesHandler) Handle(ctx context.Context, _ ListWorkerSchedulesQuery) (ListWorkerSchedulesResult, error) {
	rows, err := h.reader.ListResponses(ctx)
	if err != nil {
		return ListWorkerSchedulesResult{}, err
	}
	return ListWorkerSchedulesResult{Data: rows}, nil
}
