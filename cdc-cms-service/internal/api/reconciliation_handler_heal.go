package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// TriggerHeal dispatches heal for a specific table
func (h *ReconciliationHandler) TriggerHeal(c *fiber.Ctx) error {
	table := strings.TrimSpace(c.Params("table"))
	if table == "" {
		var scope reconScopeRequest
		_ = c.BodyParser(&scope)
		resolved, err := h.resolveTargetTable(c, scope)
		if err != nil {
			if errors.Is(err, queries.ErrAmbiguousScope) {
				return c.Status(409).JSON(fiber.Map{"error": "ambiguous_reconciliation_scope"})
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return c.Status(404).JSON(fiber.Map{"error": "reconciliation_scope_not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		table = resolved
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, derr := h.bus.Dispatch(ctx, commands.ReconHealCommand{Table: table})
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-heal-trigger", TargetTable: table, Status: "success",
	})

	return c.Status(202).JSON(fiber.Map{"message": "heal dispatched", "table": table, "job_id": res.JobID})
}
