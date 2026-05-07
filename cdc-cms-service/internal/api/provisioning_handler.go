// Package api — provisioning_handler.go
//
// REST surface for the Source Provisioning Mode flow (workspace
// feature-cdc-integration / phase provisioning_mode). Architect
// rulings D5 (path scope), D6 (CAS via orchestrator), D8 (trace
// propagation) apply.
//
// Endpoints (all under /api/v1/cms/sources/:id/provisioning):
//   GET    .                    → snapshot
//   POST   ./advance            → fire next step
//   POST   ./pause              → running -> paused
//   POST   ./resume             → paused -> running
//   POST   ./retry              → failed -> from_state, then advance
//   POST   ./archive            → any -> archived
//   POST   ./mode               → flip auto/manual (body {"mode":"auto|manual"})
//
// Error mapping (D5 / architect Phase C ruling):
//   persistence.ErrProvisioningSourceNotFound      → 404
//   persistence.ErrProvisioningInvalidTransition   → 422 (FE shows nice msg)
//   persistence.ErrProvisioningConflict            → 409 (FE retries / refreshes)
//   anything else                              → 500
//
// Auth: must be mounted behind JWTAuth → RequireOpsAdmin (router wires it).
package api

import (
	"errors"
	"strconv"

	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

type ProvisioningHandler struct {
	orch   *persistence.ProvisioningOrchestrator
	logger *zap.Logger
}

func NewProvisioningHandler(orch *persistence.ProvisioningOrchestrator, logger *zap.Logger) *ProvisioningHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ProvisioningHandler{orch: orch, logger: logger}
}

// parseSourceID — common path-param parser.
func (h *ProvisioningHandler) parseSourceID(c *fiber.Ctx) (int64, error) {
	idStr := c.Params("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		return 0, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "invalid source id",
			"path_id": idStr,
		})
	}
	return id, nil
}

// mapErr — translates service errors into HTTP responses.
func (h *ProvisioningHandler) mapErr(c *fiber.Ctx, sourceID int64, err error) error {
	switch {
	case errors.Is(err, persistence.ErrProvisioningSourceNotFound):
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error":     "source not found",
			"source_id": sourceID,
		})
	case errors.Is(err, persistence.ErrProvisioningInvalidTransition):
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"error":     "invalid transition",
			"detail":    err.Error(),
			"source_id": sourceID,
		})
	case errors.Is(err, persistence.ErrProvisioningConflict):
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":     "state changed concurrently — retry after refreshing",
			"source_id": sourceID,
		})
	default:
		h.logger.Error("provisioning handler: unhandled error",
			zap.Int64("source_id", sourceID),
			zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":     "internal error",
			"detail":    err.Error(),
			"source_id": sourceID,
		})
	}
}

// GetState handles GET /api/v1/cms/sources/:id/provisioning
// @Summary      Snapshot of provisioning state for one source
// @Tags         Provisioning
// @Param        id   path     int  true  "source_object_registry.id"
// @Success      200  {object} persistence.SourceProvisioningSnapshot
// @Failure      400,404,500 {object} map[string]any
// @Router       /api/v1/cms/sources/{id}/provisioning [get]
func (h *ProvisioningHandler) GetState(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	snap, err := h.orch.GetState(c.UserContext(), id)
	if err != nil {
		return h.mapErr(c, id, err)
	}
	return c.JSON(snap)
}

// Advance handles POST /api/v1/cms/sources/:id/provisioning/advance
func (h *ProvisioningHandler) Advance(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.Advance(c.UserContext(), id, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"ok":        true,
		"action":    "advance",
		"source_id": id,
		"actor":     actor,
	})
}

// Pause handles POST /api/v1/cms/sources/:id/provisioning/pause
func (h *ProvisioningHandler) Pause(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.Pause(c.UserContext(), id, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.JSON(fiber.Map{"ok": true, "action": "pause", "source_id": id, "actor": actor})
}

// Resume handles POST /api/v1/cms/sources/:id/provisioning/resume
func (h *ProvisioningHandler) Resume(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.Resume(c.UserContext(), id, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.JSON(fiber.Map{"ok": true, "action": "resume", "source_id": id, "actor": actor})
}

// Retry handles POST /api/v1/cms/sources/:id/provisioning/retry
func (h *ProvisioningHandler) Retry(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.Retry(c.UserContext(), id, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"ok": true, "action": "retry", "source_id": id, "actor": actor,
	})
}

// Archive handles POST /api/v1/cms/sources/:id/provisioning/archive
func (h *ProvisioningHandler) Archive(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.Archive(c.UserContext(), id, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.JSON(fiber.Map{"ok": true, "action": "archive", "source_id": id, "actor": actor})
}

type setModeRequest struct {
	Mode string `json:"mode"`
}

// SetMode handles POST /api/v1/cms/sources/:id/provisioning/mode
// Body: {"mode":"auto|manual"}.
func (h *ProvisioningHandler) SetMode(c *fiber.Ctx) error {
	id, errResp := h.parseSourceID(c)
	if errResp != nil {
		return errResp
	}
	var req setModeRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":  "invalid body",
			"detail": err.Error(),
		})
	}
	if req.Mode != "auto" && req.Mode != "manual" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "mode must be 'auto' or 'manual'",
			"got":   req.Mode,
		})
	}
	actor := actorOrAnonymous(c)
	if err := h.orch.SetMode(c.UserContext(), id, req.Mode, actor); err != nil {
		return h.mapErr(c, id, err)
	}
	return c.JSON(fiber.Map{
		"ok": true, "action": "set_mode", "source_id": id, "mode": req.Mode, "actor": actor,
	})
}

func actorOrAnonymous(c *fiber.Ctx) string {
	if u := middleware.GetUsername(c); u != "" {
		return u
	}
	return "anonymous"
}
