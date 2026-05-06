// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"
	"encoding/json"
	"time"

	"cdc-cms-service/internal/model"
)

// WizardReader is the read-side port for the Source->Master automation
// state machine. Single caller (the CMS wizard handler).
type WizardReader interface {
	Get(ctx context.Context, id string) (*model.WizardSession, error)
}

// ----- GetWizardSession --------------------------------------------

// GetWizardSessionQuery is GET /api/v1/wizard/sessions/:id.
type GetWizardSessionQuery struct {
	ID string
}

func (q GetWizardSessionQuery) Type() string { return "wizard.session.get" }

// GetWizardSessionResult is the full session row. The handler returns
// it verbatim (legacy parity).
type GetWizardSessionResult struct {
	Session *model.WizardSession
}

type GetWizardSessionHandler struct {
	reader WizardReader
}

func NewGetWizardSessionHandler(r WizardReader) *GetWizardSessionHandler {
	return &GetWizardSessionHandler{reader: r}
}

func (h *GetWizardSessionHandler) Handle(ctx context.Context, q GetWizardSessionQuery) (GetWizardSessionResult, error) {
	s, err := h.reader.Get(ctx, q.ID)
	if err != nil {
		return GetWizardSessionResult{}, err
	}
	return GetWizardSessionResult{Session: s}, nil
}

// ----- GetWizardProgress -------------------------------------------

// GetWizardProgressQuery is GET /api/v1/wizard/sessions/:id/progress.
// Compact snapshot for the FE progress bar.
type GetWizardProgressQuery struct {
	ID string
}

func (q GetWizardProgressQuery) Type() string { return "wizard.progress.get" }

// WizardProgressView is the projected compact snapshot. Field order
// alphabetical-by-JSON-tag to match the legacy `fiber.Map` output
// (lesson #1294 — Go maps serialize in alphabetical key order).
type WizardProgressView struct {
	CurrentStep int             `json:"current_step"`
	ProgressLog json.RawMessage `json:"progress_log"`
	SessionID   string          `json:"session_id"`
	Status      string          `json:"status"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

// GetWizardProgressResult wraps the projection.
type GetWizardProgressResult struct {
	Progress WizardProgressView
}

type GetWizardProgressHandler struct {
	reader WizardReader
}

func NewGetWizardProgressHandler(r WizardReader) *GetWizardProgressHandler {
	return &GetWizardProgressHandler{reader: r}
}

func (h *GetWizardProgressHandler) Handle(ctx context.Context, q GetWizardProgressQuery) (GetWizardProgressResult, error) {
	s, err := h.reader.Get(ctx, q.ID)
	if err != nil {
		return GetWizardProgressResult{}, err
	}
	return GetWizardProgressResult{
		Progress: WizardProgressView{
			SessionID:   s.ID,
			CurrentStep: s.CurrentStep,
			Status:      s.Status,
			ProgressLog: json.RawMessage(s.ProgressLog),
			UpdatedAt:   s.UpdatedAt,
		},
	}, nil
}
