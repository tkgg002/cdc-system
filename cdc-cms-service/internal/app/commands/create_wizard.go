package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
)

// CreateWizardCommand starts a new wizard draft session. Body may be
// empty; FE typically passes {source_name} so the first step comes
// pre-filled. Sync — runs in-process via WizardRepo.
type CreateWizardCommand struct {
	ports.SyncCommandMixin
	SourceName string          `json:"source_name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	CreatedBy  string          `json:"created_by"`
}

func (CreateWizardCommand) Type() string { return "wizard.create" }

func (c CreateWizardCommand) Validate() error { return nil }

type CreateWizardHandler struct {
	repo   *repository.WizardRepo
	logger *zap.Logger
}

func NewCreateWizardHandler(repo *repository.WizardRepo, logger *zap.Logger) *CreateWizardHandler {
	return &CreateWizardHandler{repo: repo, logger: logger}
}

func (h *CreateWizardHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateWizardCommand)
	if !ok {
		return nil, errors.New("wizard.create: command type mismatch")
	}
	if h.repo == nil {
		return nil, errors.New("wizard repo not ready")
	}

	var payload []byte
	if len(cmd.Payload) > 0 {
		payload = []byte(cmd.Payload)
	}
	s := &model.WizardSession{
		ID:          uuid.NewString(),
		SourceName:  strings.TrimSpace(cmd.SourceName),
		Status:      "draft",
		CurrentStep: 0,
		StepPayload: payload,
		CreatedBy:   cmd.CreatedBy,
	}
	if err := h.repo.Create(ctx, s); err != nil {
		return nil, err
	}
	body, _ := json.Marshal(s)
	return body, nil
}
