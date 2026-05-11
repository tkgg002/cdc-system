package api

import (
	"strconv"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"github.com/gofiber/fiber/v2"
)

func ptr(s string) *string { if s == "" { return nil }; return &s }

// Reload godoc
// @Summary      Reload mapping rules for workers
// @Description  Publishes a NATS message to trigger workers to reload mapping rules from DB.
// @Tags         Mapping Rules
// @Produce      json
// @Router       /api/mapping-rules/reload [post]
func (h *MappingRuleHandler) Reload(c *fiber.Ctx) error {
	q := queries.ResolveMappingScopeQuery{
		SourceDatabase:  ptr(c.Query("source_database")),
		SourceSchema:    ptr(c.Query("source_schema")),
		SourceNamespace: ptr(c.Query("source_namespace")),
		SourceTable:     c.Query("source_table", c.Query("table")),
		ShadowSchema:    ptr(c.Query("shadow_schema")),
		ShadowTable:     ptr(c.Query("shadow_table")),
	}
	if sid, err := strconv.ParseInt(c.Query("source_object_id"), 10, 64); err == nil {
		q.SourceObjectID = &sid
	}

	target := "*"
	if scope, err := h.resolveQuery.Handle(c.Context(), q); err == nil {
		target = scope.ShadowTable
	}

	username, _ := c.Locals("username").(string)
	if err := h.natsClient.PublishReload(target, username, "reload_mapping", ""); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish reload event"})
	}
	return c.JSON(fiber.Map{"message": "reload signal sent successfully", "target_table": target})
}

// UpdateStatus godoc
// @Summary      Update mapping rule status
// @Description  Updates the status of a V2 mapping rule.
// @Tags         Mapping Rules
// @Router       /api/mapping-rules/{id} [patch]
func (h *MappingRuleHandler) UpdateStatus(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	var body struct{ Status string `json:"status"` }
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if body.Status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "status is required"})
	}
	username, _ := c.Locals("username").(string)
	cmd := commands.UpdateMappingRuleCommand{ID: int64(id), Status: body.Status, UpdatedBy: username}
	res, err := h.bus.Execute(messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key")), cmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	c.Set("Content-Type", "application/json")
	return c.Status(200).Send(res.ResultBody)
}
