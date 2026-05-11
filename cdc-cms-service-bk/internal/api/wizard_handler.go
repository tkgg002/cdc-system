package api

import (
	"encoding/json"
	"strings"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// WizardHandler serves the Source->Master automation state machine.
// Create/Execute are destructive (mutate state + kick off pipelines);
// Get/Progress are reads for the FE poll loop.
type WizardHandler struct {
	repo   *repository.WizardRepo
	logger *zap.Logger
}

func NewWizardHandler(repo *repository.WizardRepo, logger *zap.Logger) *WizardHandler {
	return &WizardHandler{repo: repo, logger: logger}
}

type createWizardReq struct {
	SourceName string          `json:"source_name"`
	Payload    json.RawMessage `json:"payload"`
}

// Create starts a new draft session. Body may be empty; callers
// typically pass {source_name} so the first step comes pre-filled.
// POST /api/v1/wizard/sessions
func (h *WizardHandler) Create(c *fiber.Ctx) error {
	var req createWizardReq
	_ = c.BodyParser(&req)

	var payload []byte
	if len(req.Payload) > 0 {
		payload = []byte(req.Payload)
	}
	s := &model.WizardSession{
		ID:          uuid.NewString(),
		SourceName:  strings.TrimSpace(req.SourceName),
		Status:      "draft",
		CurrentStep: 0,
		StepPayload: payload,
		CreatedBy:   middleware.GetUsername(c),
	}
	if err := h.repo.Create(c.Context(), s); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "create wizard: " + err.Error()})
	}
	return c.Status(201).JSON(s)
}

// Get returns the full session row. Used on mount/refresh to rehydrate
// the FE state machine.
// GET /api/v1/wizard/sessions/:id
func (h *WizardHandler) Get(c *fiber.Ctx) error {
	id := c.Params("id")
	s, err := h.repo.Get(c.Context(), id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(s)
}

type patchWizardReq struct {
	CurrentStep *int            `json:"current_step"`
	Status      *string         `json:"status"`
	MasterName  *string         `json:"master_name"`
	ConnectorID *int64          `json:"connector_id"`
	RegistryID  *int64          `json:"registry_id"`
	StepPayload json.RawMessage `json:"step_payload"`
}

// Patch updates step_payload / current_step / status. Only the allowed
// fields are accepted; anything else is silently dropped.
// PATCH /api/v1/wizard/sessions/:id
func (h *WizardHandler) Patch(c *fiber.Ctx) error {
	id := c.Params("id")
	var req patchWizardReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json"})
	}
	updates := map[string]interface{}{}
	if req.CurrentStep != nil {
		updates["current_step"] = *req.CurrentStep
	}
	if req.Status != nil {
		switch *req.Status {
		case "draft", "running", "done", "failed":
			updates["status"] = *req.Status
		default:
			return c.Status(400).JSON(fiber.Map{"error": "invalid status"})
		}
	}
	if req.MasterName != nil {
		updates["master_name"] = *req.MasterName
	}
	if req.ConnectorID != nil {
		updates["connector_id"] = *req.ConnectorID
	}
	if req.RegistryID != nil {
		updates["registry_id"] = *req.RegistryID
	}
	if len(req.StepPayload) > 0 {
		updates["step_payload"] = []byte(req.StepPayload)
	}
	if len(updates) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "nothing to update"})
	}
	if err := h.repo.Update(c.Context(), id, updates); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "update: " + err.Error()})
	}
	s, _ := h.repo.Get(c.Context(), id)
	return c.JSON(s)
}

// Execute flips status->running and logs the intent. The actual
// automation (CreateConnector -> Source -> Register -> Snapshot ->
// poll -> master create+approve) is orchestrated by FE via Patch +
// existing endpoints in this iteration — the server side just records
// progress so F5 resumes cleanly. A future pass can move the pipeline
// fully server-side using this same progress_log.
// POST /api/v1/wizard/sessions/:id/execute
func (h *WizardHandler) Execute(c *fiber.Ctx) error {
	id := c.Params("id")
	s, err := h.repo.Get(c.Context(), id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	if s.Status == "running" {
		return c.Status(409).JSON(fiber.Map{"error": "already running"})
	}
	if err := h.repo.Update(c.Context(), id, map[string]interface{}{
		"status":       "running",
		"current_step": 1,
	}); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "execute: " + err.Error()})
	}
	_ = h.repo.AppendProgress(c.Context(), id, map[string]interface{}{
		"step":    1,
		"event":   "execute_started",
		"actor":   middleware.GetUsername(c),
	})
	return c.Status(202).JSON(fiber.Map{"status": "running", "session_id": id})
}

// Progress — GET /api/v1/wizard/sessions/:id/progress
// Compact snapshot for the FE progress bar (doesn't ship step_payload).
func (h *WizardHandler) Progress(c *fiber.Ctx) error {
	id := c.Params("id")
	s, err := h.repo.Get(c.Context(), id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(fiber.Map{
		"session_id":   s.ID,
		"current_step": s.CurrentStep,
		"status":       s.Status,
		"progress_log": json.RawMessage(s.ProgressLog),
		"updated_at":   s.UpdatedAt,
	})
}
