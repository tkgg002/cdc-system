package api

import (
	"encoding/json"
	"errors"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// WizardHandler serves the Source->Master automation state machine.
// Create/Execute are destructive (mutate state + kick off pipelines);
// Get/Progress are reads — these now delegate to query handlers in
// `internal/app/queries/get_wizard_session.go`.
type WizardHandler struct {
	repo      *repository.WizardRepo
	logger    *zap.Logger
	getQ      *queries.GetWizardSessionHandler
	progressQ *queries.GetWizardProgressHandler
	bus       ports.CommandBus
}

func NewWizardHandler(
	repo *repository.WizardRepo,
	logger *zap.Logger,
	getQ *queries.GetWizardSessionHandler,
	progressQ *queries.GetWizardProgressHandler,
	bus ports.CommandBus,
) *WizardHandler {
	return &WizardHandler{repo: repo, logger: logger, getQ: getQ, progressQ: progressQ, bus: bus}
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

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	cmd := commands.CreateWizardCommand{
		SourceName: req.SourceName,
		Payload:    req.Payload,
		CreatedBy:  user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "create wizard: " + err.Error()})
	}
	c.Set("Content-Type", "application/json")
	return c.Status(201).Send(res.ResultBody)
}

// Get returns the full session row. Used on mount/refresh to rehydrate
// the FE state machine.
// GET /api/v1/wizard/sessions/:id
func (h *WizardHandler) Get(c *fiber.Ctx) error {
	res, err := h.getQ.Handle(c.UserContext(), queries.GetWizardSessionQuery{ID: c.Params("id")})
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(res.Session)
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

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	cmd := commands.PatchWizardCommand{
		ID:          id,
		CurrentStep: req.CurrentStep,
		Status:      req.Status,
		MasterName:  req.MasterName,
		ConnectorID: req.ConnectorID,
		RegistryID:  req.RegistryID,
		StepPayload: req.StepPayload,
		UpdatedBy:   user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrWizardInvalidStatus):
			return c.Status(400).JSON(fiber.Map{"error": "invalid status"})
		case errors.Is(err, commands.ErrWizardNothingToPatch):
			return c.Status(400).JSON(fiber.Map{"error": "nothing to update"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "update: " + err.Error()})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(200).Send(res.ResultBody)
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
	res, err := h.progressQ.Handle(c.UserContext(), queries.GetWizardProgressQuery{ID: c.Params("id")})
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(res.Progress)
}
