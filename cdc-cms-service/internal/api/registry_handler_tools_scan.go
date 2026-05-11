package api

import (
	"strconv"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
)

func (h *RegistryHandler) Standardize(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.StandardizeCommand{
		RegistryID:  entry.ID,
		TargetTable: entry.TargetTable,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(persistence.ActivityEntry{
			Operation: "standardize", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch standardize command: " + derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "standardize", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{"user": user},
	})
	return c.Status(202).JSON(fiber.Map{
		"message":      "standardize command accepted",
		"target_table": entry.TargetTable,
	})
}

// ScanFields is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id/scan-fields facade.
func (h *RegistryHandler) ScanFields(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	// Debezium-native scan: worker looks up Mongo source via source_db +
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.ScanFieldsCommand{
		RegistryID:  entry.ID,
		SyncEngine:  entry.SyncEngine,
		SourceType:  entry.SourceType,
		SourceDB:    entry.SourceDB,
		SourceTable: entry.SourceTable,
		TargetTable: entry.TargetTable,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(persistence.ActivityEntry{
			Operation: "scan-fields", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "scan-fields", TargetTable: entry.TargetTable, Status: "accepted",
		Details: map[string]any{"user": user, "sync_engine": entry.SyncEngine},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "scan-fields command accepted",
		"target_table": entry.TargetTable,
		"sync_engine":  entry.SyncEngine,
	})
}
