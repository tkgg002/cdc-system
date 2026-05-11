package api

import (
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// Swap godoc
// @Summary      Swap physical master tables (async)
// @Description  Kicks off an atomic master-table swap as a background job. Returns 202 + JobID; poll GET /api/jobs/:id for terminal status. Refuses with 409 if another swap for the same master is still in flight.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        name path string true "Master table name"
// @Param        body body SwapRequest true "Swap payload"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters/{name}/swap [post]
func (h *MasterRegistryHandler) Swap(c *fiber.Ctx) error {
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	var req SwapRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}
	if !masterNameRe.MatchString(req.NewTableName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_new_table_name"})
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	createdBy := getActor(c)
	correlationID, _ := c.Locals("correlation_id").(string)
	idempotencyKey := c.Get("Idempotency-Key")

	cmd := commands.MasterSwapCommand{
		MasterName:   name,
		NewTableName: req.NewTableName,
		Reason:       req.Reason,
	}
	ctx := messaging.WithMetadata(c.UserContext(), createdBy, correlationID, idempotencyKey)

	res, err := h.bus.Dispatch(ctx, cmd)
	if err != nil {
		if strings.Contains(err.Error(), "master_swap_in_flight") {
			return c.Status(409).JSON(fiber.Map{"error": "master_swap_in_flight", "detail": err.Error()})
		}
		if strings.Contains(err.Error(), "invalid_") {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		h.logger.Error("master swap dispatch failed", zap.String("master", name), zap.String("new_table", req.NewTableName), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "swap dispatch failed: " + err.Error()})
	}
	return c.Status(202).JSON(fiber.Map{
		"status":      "accepted",
		"master_name": name,
		"job_id":      res.JobID,
	})
}
