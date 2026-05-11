package api

import (
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
)

func (h *RegistryHandler) Transform(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	if err := h.natsClient.Conn.Publish("cdc.cmd.batch-transform", []byte(entry.TargetTable)); err != nil {
		h.activityLogger.LogAsync(persistence.ActivityEntry{
			Operation: "transform", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: err.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch transform command: " + err.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "transform", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{"user": middleware.GetUsername(c)},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "transform command accepted",
		"target_table": entry.TargetTable,
	})
}

// TransformStatus returns the transform progress for a table
func (h *RegistryHandler) TransformStatus(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	probe, err := h.bridgeReader.ProbeBridgeStatus(c.UserContext(), "public", entry.TargetTable)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if !probe.Exists {
		return c.JSON(fiber.Map{
			"target_table":   entry.TargetTable,
			"total_rows":     0,
			"bridged_rows":   0,
			"pending_bridge": 0,
			"last_bridge_at": entry.LastBridgeAt,
			"status":         "table_not_created",
		})
	}

	return c.JSON(fiber.Map{
		"target_table":   entry.TargetTable,
		"total_rows":     probe.TotalRows,
		"bridged_rows":   probe.RawDataRows,
		"pending_bridge": probe.TotalRows - probe.RawDataRows,
		"last_bridge_at": entry.LastBridgeAt,
	})
}

// CreateDefaultColumns creates CDC table + adds all approved mapping rule columns in one step.
// This is the "tạo field default" action for Luồng 1.
