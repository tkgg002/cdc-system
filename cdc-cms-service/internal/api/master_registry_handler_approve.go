package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func (h *MasterRegistryHandler) Approve(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.ApproveMasterCommand{
		Name:      name,
		Reason:    strings.TrimSpace(req.Reason),
		UpdatedBy: actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMasterNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrMasterNameAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_name"})
		case errors.Is(err, commands.ErrMasterNotApprovable):
			return c.Status(409).JSON(fiber.Map{"error": "not_approvable", "detail": "master not found OR already approved"})
		default:
			h.logger.Error("approve master failed", zap.String("master", name), zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Type("application/json")
	return c.Status(202).Send(res.ResultBody)
}

func (h *MasterRegistryHandler) Reject(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.RejectMasterCommand{
		Name:      name,
		Reason:    strings.TrimSpace(req.Reason),
		UpdatedBy: actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMasterNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrMasterNameAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_name"})
		case strings.Contains(err.Error(), "invalid_master_name"):
			return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
		case strings.Contains(err.Error(), "reason_required"):
			return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
		default:
			h.logger.Error("master reject failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(202).Send(res.ResultBody)
}
