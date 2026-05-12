package api

import (
	"encoding/json"
	"strconv"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/naming"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

func (h *RegistryHandler) BulkRegister(c *fiber.Ctx) error {
	var entries []model.TableRegistry
	if err := c.BodyParser(&entries); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	registerCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	registerCmd := commands.BulkRegisterRegistryCommand{Entries: entries, CreatedBy: user}
	res, err := h.bus.Execute(registerCtx, registerCmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "bulk register failed: " + err.Error()})
	}

	var body struct {
		Message string                `json:"message"`
		Created int                   `json:"created"`
		Entries []model.TableRegistry `json:"entries"`
	}
	_ = json.Unmarshal(res.ResultBody, &body)

	dispatched := 0
	baseIdem := c.Get("Idempotency-Key")
	for _, e := range body.Entries {
		entryIdem := ""
		if baseIdem != "" {
			entryIdem = baseIdem + ":cdc:" + strconv.FormatUint(uint64(e.ID), 10)
		}
		dispatchCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), entryIdem)
		cmd := commands.CreateDefaultColumnsCommand{
			RegistryID:      e.ID,
			ShadowSchema:    naming.ShadowSchemaName(normalizeShadowIdent(e.SourceDB)),
			TargetTable:     e.TargetTable,
			SourceTable:     e.SourceTable,
			PrimaryKeyField: e.PrimaryKeyField,
			PrimaryKeyType:  e.PrimaryKeyType,
		}
		if _, derr := h.bus.Dispatch(dispatchCtx, cmd); derr != nil {
			h.logger.Warn("publish create-default-columns failed", zap.Error(derr), zap.String("table", e.TargetTable))
			continue
		}
		dispatched++
		if h.bus != nil {
			eCopy := e
			syncCmd := commands.V2SyncCommand{Entry: &eCopy}
			if _, err := h.bus.Execute(dispatchCtx, syncCmd); err != nil {
				h.logger.Error("bulk register v2 sync failed", zap.Uint("registry_id", e.ID), zap.Error(err))
			}
		}
	}

	return c.Status(202).JSON(fiber.Map{
		"message":    "tables registered — create-default-columns dispatched per entry",
		"created":    body.Created,
		"dispatched": dispatched,
	})
}

// GetStats is kept as a compatibility delegate for internal callers; the
// public CMS read surface now uses /api/v1/source-objects/stats.
