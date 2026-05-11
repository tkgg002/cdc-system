package api

import (
	"strconv"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
)

func (h *RegistryHandler) CreateDefaultColumns(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.CreateDefaultColumnsCommand{
		RegistryID:      entry.ID,
		TargetTable:     entry.TargetTable,
		SourceTable:     entry.SourceTable,
		PrimaryKeyField: entry.PrimaryKeyField,
		PrimaryKeyType:  entry.PrimaryKeyType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(persistence.ActivityEntry{
			Operation: "create-default-columns", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch: " + derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "create-default-columns", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{
			"pk_field": entry.PrimaryKeyField,
			"pk_type":  entry.PrimaryKeyType,
			"user":     user,
		},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "create-default-columns command accepted",
		"target_table": entry.TargetTable,
	})
}

func (h *RegistryHandler) DetectTimestampField(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.DetectTimestampFieldCommand{
		RegistryID:  entry.ID,
		TargetTable: entry.TargetTable,
		SourceTable: entry.SourceTable,
		SourceDB:    entry.SourceDB,
		SourceType:  entry.SourceType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(persistence.ActivityEntry{
			Operation: "detect-timestamp-field", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "detect-timestamp-field", TargetTable: entry.TargetTable, Status: "accepted",
		Details: map[string]any{"user": user, "source_table": entry.SourceTable},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "timestamp field detection dispatched",
		"target_table": entry.TargetTable,
	})
}
