package api

import (
	"encoding/json"
	"errors"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func (h *RegistryHandler) Register(c *fiber.Ctx) error {
	var entry model.TableRegistry
	if err := c.BodyParser(&entry); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	registerCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	registerCmd := commands.RegisterRegistryCommand{Entry: entry, CreatedBy: user}
	res, err := h.bus.Execute(registerCtx, registerCmd)
	if err != nil {
		if errors.Is(err, commands.ErrShadowDDLFailed) {
			return c.Status(500).JSON(fiber.Map{"error": "shadow DDL failed: " + err.Error()})
		}
		return c.Status(500).JSON(fiber.Map{"error": "failed to register table: " + err.Error()})
	}

	var body struct {
		Message string              `json:"message"`
		Entry   model.TableRegistry `json:"entry"`
	}
	_ = json.Unmarshal(res.ResultBody, &body)
	created := body.Entry

	dispatched := []string{}
	dispatchIdem := c.Get("Idempotency-Key")
	if dispatchIdem != "" {
		dispatchIdem += ":cdc"
	}
	dispatchCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), dispatchIdem)
	createCmd := commands.CreateDefaultColumnsCommand{
		RegistryID:      created.ID,
		TargetTable:     created.TargetTable,
		SourceTable:     created.SourceTable,
		PrimaryKeyField: created.PrimaryKeyField,
		PrimaryKeyType:  created.PrimaryKeyType,
	}
	if _, derr := h.bus.Dispatch(dispatchCtx, createCmd); derr != nil {
		h.logger.Warn("publish create-default-columns failed", zap.Error(derr))
	} else {
		dispatched = append(dispatched, "cdc.cmd.create-default-columns")
	}

	if h.bus != nil {
		syncCmd := commands.V2SyncCommand{Entry: &created}
		if _, err := h.bus.Execute(dispatchCtx, syncCmd); err != nil {
			h.logger.Error("post-register v2 sync failed", zap.Uint("registry_id", created.ID), zap.Error(err))
		}

		// Automated sync trigger: If it's a Debezium source, restart the connector to pick up the new collection
		if created.SyncEngine == "debezium" {
			h.logger.Info("triggering debezium restart for new source auto-sync", zap.Uint("registry_id", created.ID))
			if _, derr := h.bus.Dispatch(dispatchCtx, commands.RestartDebeziumCommand{}); derr != nil {
				h.logger.Warn("auto-sync restart dispatch failed", zap.Error(derr))
			}
		}
	}

	return c.Status(202).JSON(fiber.Map{
		"message":    "table registered — external sync dispatched",
		"entry":      created,
		"dispatched": dispatched,
	})
}

// Update is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id facade.
