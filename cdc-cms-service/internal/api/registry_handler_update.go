package api

import (
	"errors"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func (h *RegistryHandler) Update(c *fiber.Ctx) error {
	id, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	existing, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	var update struct {
		SyncEngine     *string `json:"sync_engine"`
		SyncInterval   *string `json:"sync_interval"`
		Priority       *string `json:"priority"`
		IsActive       *bool   `json:"is_active"`
		Notes          *string `json:"notes"`
		TimestampField *string `json:"timestamp_field"`
	}
	if err := c.BodyParser(&update); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if update.TimestampField != nil && !isValidTimestampField(*update.TimestampField) {
		return c.Status(400).JSON(fiber.Map{
			"error": "invalid timestamp_field: must match [A-Za-z_][A-Za-z0-9_]{0,63}",
		})
	}

	user := middleware.GetUsername(c)
	cmd := commands.UpdateRegistryCommand{
		ID:             existing.ID,
		SyncEngine:     update.SyncEngine,
		SyncInterval:   update.SyncInterval,
		Priority:       update.Priority,
		IsActive:       update.IsActive,
		Notes:          update.Notes,
		TimestampField: update.TimestampField,
		UpdatedBy:      user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrRegistryNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not found"})
		case errors.Is(err, commands.ErrRegistryNoFields):
			return c.Status(400).JSON(fiber.Map{"error": "no fields to update"})
		case errors.Is(err, commands.ErrRegistryInvalidTSField):
			return c.Status(400).JSON(fiber.Map{"error": "invalid timestamp_field"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "failed to update: " + err.Error()})
		}
	}

	if h.bus != nil {
		if updated, getErr := h.repo.GetByID(c.Context(), existing.ID); getErr == nil {
			if _, syncErr := h.bus.Execute(ctx, commands.V2SyncCommand{Entry: updated}); syncErr != nil {
				h.logger.Error("post-update v2 sync failed", zap.Uint("registry_id", existing.ID), zap.Error(syncErr))
			}

			// Automated sync trigger: If it's a Debezium source, restart the connector to pick up changes
			if updated.SyncEngine == "debezium" {
				h.logger.Info("triggering debezium restart for auto-sync", zap.Uint("registry_id", updated.ID))
				if _, derr := h.bus.Dispatch(ctx, commands.RestartDebeziumCommand{}); derr != nil {
					h.logger.Warn("auto-sync restart dispatch failed", zap.Error(derr))
				}
			}
		}
	}

	c.Set("Content-Type", "application/json")
	return c.Status(202).Send(res.ResultBody)
}
