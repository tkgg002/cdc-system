package commands

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"cdc-cms-service/internal/app/ports"
)

// WizardExecuteCommand flips a draft session to running and stamps the
// first progress entry. Sync — single in-process write through WizardRepo.
type WizardExecuteCommand struct {
	ports.SyncCommandMixin
	SessionID string `json:"session_id"`
	Actor     string `json:"actor,omitempty"`
}

func (WizardExecuteCommand) Type() string { return "wizard.execute" }

var (
	ErrWizardNotFound       = errors.New("wizard session not found")
	ErrWizardAlreadyRunning = errors.New("wizard session already running")
)

func (c WizardExecuteCommand) Validate() error {
	if c.SessionID == "" {
		return errors.New("session_id required")
	}
	return nil
}

type WizardExecuteHandler struct {
	repo   ports.WizardRepo
	logger *zap.Logger
}

func NewWizardExecuteHandler(repo ports.WizardRepo, logger *zap.Logger) *WizardExecuteHandler {
	return &WizardExecuteHandler{repo: repo, logger: logger}
}

func (h *WizardExecuteHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(WizardExecuteCommand)
	if !ok {
		return nil, errors.New("wizard.execute: command type mismatch")
	}
	if h.repo == nil {
		return nil, errors.New("wizard repo not ready")
	}

	s, err := h.repo.Get(ctx, cmd.SessionID)
	if err != nil {
		return nil, ErrWizardNotFound
	}
	if s.Status == "running" {
		return nil, ErrWizardAlreadyRunning
	}
	if err := h.repo.Update(ctx, cmd.SessionID, map[string]interface{}{
		"status":       "running",
		"current_step": 1,
	}); err != nil {
		return nil, err
	}
	_ = h.repo.AppendProgress(ctx, cmd.SessionID, map[string]interface{}{
		"step":  1,
		"event": "execute_started",
		"actor": cmd.Actor,
	})

	body, _ := json.Marshal(map[string]interface{}{
		"status":     "running",
		"session_id": cmd.SessionID,
	})
	return body, nil
}
