package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func (h *MasterRegistryHandler) Create(c *fiber.Ctx) error {
	var req CreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}

	trimCreateRequest(&req)

	if !masterNameRe.MatchString(req.MasterName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	if req.MasterSchema == "" {
		req.MasterSchema = "public"
	}
	if !namespaceName.MatchString(req.MasterSchema) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_schema"})
	}

	validType := map[string]bool{
		"copy_1_to_1": true, "filter": true, "aggregate": true,
		"group_by": true, "join": true, "custom_sql": true,
	}
	if !validType[req.TransformType] {
		return c.Status(400).JSON(fiber.Map{
			"error":  "invalid_transform_type",
			"detail": "one of copy_1_to_1|filter|aggregate|group_by|join|custom_sql",
		})
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.CreateMasterCommand{
		MasterName:           req.MasterName,
		MasterSchema:         req.MasterSchema,
		MasterConnectionCode: req.MasterConnectionCode,
		SourceShadow:         req.SourceShadow,
		SourceDatabase:       req.SourceDatabase,
		SourceSchema:         req.SourceSchema,
		SourceNamespace:      req.SourceNamespace,
		SourceTable:          req.SourceTable,
		ShadowSchema:         req.ShadowSchema,
		ShadowTable:          req.ShadowTable,
		TransformType:        req.TransformType,
		Spec:                 req.Spec,
		Reason:               req.Reason,
		UpdatedBy:            actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrShadowBindingNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "shadow_binding_not_found"})
		case errors.Is(err, commands.ErrShadowBindingAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_shadow_binding"})
		case errors.Is(err, commands.ErrMasterConnectionNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "master_connection_not_found"})
		case errors.Is(err, commands.ErrMasterConnectionAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_connection"})
		case errors.Is(err, commands.ErrMasterAlreadyExists):
			return c.Status(409).JSON(fiber.Map{"error": "master_already_exists", "master_name": req.MasterName})
		case strings.Contains(err.Error(), "invalid_"):
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		case strings.Contains(err.Error(), "reason_required"):
			return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
		default:
			h.logger.Error("master create failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(201).Send(res.ResultBody)
}
