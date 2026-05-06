package commands

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/repository"
)

// PatchWizardCommand updates allow-listed fields on a draft wizard
// session. Sync — runs in-process via WizardRepo.
type PatchWizardCommand struct {
	ports.SyncCommandMixin
	ID          string          `json:"id"`
	CurrentStep *int            `json:"current_step,omitempty"`
	Status      *string         `json:"status,omitempty"`
	MasterName  *string         `json:"master_name,omitempty"`
	ConnectorID *int64          `json:"connector_id,omitempty"`
	RegistryID  *int64          `json:"registry_id,omitempty"`
	StepPayload json.RawMessage `json:"step_payload,omitempty"`
	UpdatedBy   string          `json:"updated_by,omitempty"`
}

func (PatchWizardCommand) Type() string { return "wizard.patch" }

var (
	ErrWizardInvalidStatus  = errors.New("invalid status")
	ErrWizardNothingToPatch = errors.New("nothing to update")
)

func (c PatchWizardCommand) Validate() error {
	if c.ID == "" {
		return errors.New("id required")
	}
	if c.Status != nil {
		switch *c.Status {
		case "draft", "running", "done", "failed":
		default:
			return ErrWizardInvalidStatus
		}
	}
	if c.CurrentStep == nil && c.Status == nil && c.MasterName == nil &&
		c.ConnectorID == nil && c.RegistryID == nil && len(c.StepPayload) == 0 {
		return ErrWizardNothingToPatch
	}
	return nil
}

type PatchWizardHandler struct {
	repo   *repository.WizardRepo
	logger *zap.Logger
}

func NewPatchWizardHandler(repo *repository.WizardRepo, logger *zap.Logger) *PatchWizardHandler {
	return &PatchWizardHandler{repo: repo, logger: logger}
}

func (h *PatchWizardHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(PatchWizardCommand)
	if !ok {
		return nil, errors.New("wizard.patch: command type mismatch")
	}
	if h.repo == nil {
		return nil, errors.New("wizard repo not ready")
	}

	updates := map[string]interface{}{}
	if cmd.CurrentStep != nil {
		updates["current_step"] = *cmd.CurrentStep
	}
	if cmd.Status != nil {
		updates["status"] = *cmd.Status
	}
	if cmd.MasterName != nil {
		updates["master_name"] = *cmd.MasterName
	}
	if cmd.ConnectorID != nil {
		updates["connector_id"] = *cmd.ConnectorID
	}
	if cmd.RegistryID != nil {
		updates["registry_id"] = *cmd.RegistryID
	}
	if len(cmd.StepPayload) > 0 {
		updates["step_payload"] = []byte(cmd.StepPayload)
	}

	if err := h.repo.Update(ctx, cmd.ID, updates); err != nil {
		return nil, err
	}
	s, err := h.repo.Get(ctx, cmd.ID)
	if err != nil {
		// Fallback if re-fetch fails — caller has the id already.
		return json.RawMessage(`{"id":"` + cmd.ID + `"}`), nil
	}
	body, _ := json.Marshal(s)
	return body, nil
}
