package api

import (
	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"github.com/gofiber/fiber/v2"
)

// Tools: Reset Debezium offset via signal
func (h *ReconciliationHandler) ResetDebeziumOffset(c *fiber.Ctx) error {
	var body struct {
		Database   string `json:"database"`
		Collection string `json:"collection"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.DebeziumSignalCommand{
		Type_:      "signal-snapshot",
		Database:   body.Database,
		Collection: body.Collection,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "debezium signal dispatched", "job_id": res.JobID})
}

// Tools: Trigger snapshot for a table
func (h *ReconciliationHandler) TriggerSnapshot(c *fiber.Ctx) error {
	table := c.Params("table")

	var body struct {
		Database   string `json:"database"`
		Collection string `json:"collection"`
	}
	// Ignore parsing error as the body might be empty
	_ = c.BodyParser(&body)

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, derr := h.bus.Dispatch(ctx, commands.DebeziumSnapshotCommand{
		Table:      table,
		Database:   body.Database,
		Collection: body.Collection,
	})
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "snapshot signal dispatched", "table": table, "job_id": res.JobID})
}
