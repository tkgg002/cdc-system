package api

import (
	"cdc-cms-service/internal/api/dto"
	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"github.com/gofiber/fiber/v2"
)

// Create godoc
// @Summary      Create a mapping rule
// @Description  Creates a V2 mapping rule in cdc_system.mapping_rule_v2. Supports source_object_id or source/shadow scope resolution; legacy source_table is accepted as fallback.
// @Tags         Mapping Rules
// @Accept       json
// @Produce      json
// @Param        body body dto.MappingRuleCreateRequest true "Mapping rule details"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [post]
func (h *MappingRuleHandler) Create(c *fiber.Ctx) error {
	var req dto.MappingRuleCreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if req.SourceField == "" || req.TargetColumn == "" || req.DataType == "" {
		return c.Status(400).JSON(fiber.Map{"error": "source_field, target_column, data_type are required"})
	}

	username, _ := c.Locals("username").(string)
	cmd := commands.CreateMappingRuleCommand{
		SourceObjectID:  req.SourceObjectID,
		MasterBindingID: req.MasterBindingID,
		SourceDatabase:  req.SourceDatabase,
		SourceSchema:    req.SourceSchema,
		SourceNamespace: req.SourceNamespace,
		SourceTable:     req.SourceTable,
		ShadowSchema:    req.ShadowSchema,
		ShadowTable:     req.ShadowTable,
		SourceField:     req.SourceField,
		SourcePath:      req.SourcePath,
		TargetColumn:    req.TargetColumn,
		DataType:        req.DataType,
		SourceFormat:    req.SourceFormat,
		TransformFn:     req.TransformFn,
		IsNullable:      req.IsNullable,
		IsActive:        req.IsActive,
		Status:          req.Status,
		Notes:           req.Notes,
		UpdatedBy:       username,
	}

	res, err := h.bus.Execute(messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key")), cmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	c.Set("Content-Type", "application/json")
	return c.Status(201).Send(res.ResultBody)
}
