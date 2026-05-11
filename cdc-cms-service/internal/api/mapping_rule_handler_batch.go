package api

import (
	"strconv"

	"cdc-cms-service/internal/api/dto"
	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"github.com/gofiber/fiber/v2"
)

func (h *MappingRuleHandler) Backfill(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	rule, err := h.ruleRepo.GetByID(c.Context(), int64(id))
	if err != nil || rule.ShadowTable == nil {
		return c.Status(404).JSON(fiber.Map{"error": "mapping rule or shadow target not found"})
	}

	username, _ := c.Locals("username").(string)
	cmd := commands.BackfillCommand{TargetTable: *rule.ShadowTable, SourceField: rule.SourceField, TargetColumn: rule.TargetColumn, DataType: rule.DataType}
	if _, derr := h.bus.Dispatch(messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key")), cmd); derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "accepted", "target_table": *rule.ShadowTable, "source_field": rule.SourceField, "target_column": rule.TargetColumn})
}

func (h *MappingRuleHandler) BatchUpdate(c *fiber.Ctx) error {
	var body dto.MappingRuleBatchUpdateRequest
	if err := c.BodyParser(&body); err != nil || body.Status == "" || len(body.IDs) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid payload"})
	}
	username, _ := c.Locals("username").(string)
	baseIdem := c.Get("Idempotency-Key")

	for _, id := range body.IDs {
		rule, err := h.ruleRepo.GetByID(c.Context(), int64(id))
		if err != nil {
			continue
		}
		idem := ""
		if baseIdem != "" {
			idem = baseIdem + ":rule:" + strconv.Itoa(int(id))
		}
		ctx := messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), idem)
		h.bus.Execute(ctx, commands.UpdateMappingRuleCommand{ID: int64(id), Status: body.Status, UpdatedBy: username})

		if body.Status == "approved" && rule.ShadowTable != nil {
			h.bus.Dispatch(ctx, commands.AlterColumnCommand{TargetTable: *rule.ShadowTable, ColumnName: rule.TargetColumn, DataType: rule.DataType, Action: "add"})
			if body.AutoBackfill {
				h.bus.Dispatch(ctx, commands.BackfillCommand{TargetTable: *rule.ShadowTable, SourceField: rule.SourceField, TargetColumn: rule.TargetColumn, DataType: rule.DataType})
			}
			h.natsClient.PublishReload(*rule.ShadowTable, username, "batch_update", "")
		}
	}
	if body.Status != "approved" {
		h.natsClient.PublishReload("*", username, "batch_update", "")
	}
	return c.Status(202).JSON(fiber.Map{"message": "accepted"})
}
