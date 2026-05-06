// Package queries — read-side use cases (CQRS Q-side).
//
// get_job.go powers GET /api/jobs/:id, the FE-facing surface for the
// CommandBus job tracker (Phase 2 v2 / P3). The reader port is satisfied
// by the same `*jobRepoGorm` that the CommandBus writes through — no
// separate adapter, no SQL duplication.
package queries

import (
	"context"
	"encoding/json"
	"time"

	"cdc-cms-service/internal/domain/job"
)

// JobReader is the read-side port for `cdc_system.cdc_jobs`. Single
// caller (GET /api/jobs/:id). Defined here so the read concern lives
// next to its consumer.
type JobReader interface {
	GetByID(ctx context.Context, id string) (*job.Job, error)
}

// GetJobQuery is GET /api/jobs/:id.
type GetJobQuery struct {
	ID string
}

func (q GetJobQuery) Type() string { return "job.get" }

// JobView is the projected wire shape. Field order alphabetical-by-
// JSON-tag to match `fiber.Map` legacy output (lesson #1294 — Go maps
// serialize in alphabetical key order; structs serialize in declaration
// order, so we declare alphabetically to keep parity).
type JobView struct {
	CorrelationID  string          `json:"correlation_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	CreatedBy      string          `json:"created_by"`
	ErrorMessage   string          `json:"error_message,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
	Result         json.RawMessage `json:"result,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	Status         string          `json:"status"`
	Type           string          `json:"type"`
}

// GetJobResult wraps the projection.
type GetJobResult struct {
	Job JobView
}

type GetJobHandler struct {
	reader JobReader
}

func NewGetJobHandler(r JobReader) *GetJobHandler {
	return &GetJobHandler{reader: r}
}

// Handle returns gorm.ErrRecordNotFound verbatim so the HTTP layer can
// translate it to 404.
func (h *GetJobHandler) Handle(ctx context.Context, q GetJobQuery) (GetJobResult, error) {
	j, err := h.reader.GetByID(ctx, q.ID)
	if err != nil {
		return GetJobResult{}, err
	}
	return GetJobResult{
		Job: JobView{
			ID:             j.ID,
			Type:           j.Type,
			Status:         string(j.Status),
			Payload:        j.Payload,
			Result:         j.Result,
			ErrorMessage:   j.ErrorMessage,
			IdempotencyKey: j.IdempotencyKey,
			CreatedBy:      j.CreatedBy,
			CorrelationID:  j.CorrelationID,
			CreatedAt:      j.CreatedAt,
			StartedAt:      j.StartedAt,
			FinishedAt:     j.FinishedAt,
		},
	}, nil
}
