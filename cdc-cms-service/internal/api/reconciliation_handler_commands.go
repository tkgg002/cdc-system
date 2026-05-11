package api

import (
	"encoding/json"
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

// TriggerCheck dispatches reconciliation check via NATS
func (h *ReconciliationHandler) TriggerCheck(c *fiber.Ctx) error {
	tier := c.Query("tier", "1")
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

	cmd := commands.ReconCheckCommand{Tier: tier, Table: table}
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Dispatch(ctx, cmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	payload := map[string]any{}
	if raw, err := json.Marshal(cmd); err == nil {
		_ = json.Unmarshal(raw, &payload)
	}
	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-check", TargetTable: table, Status: "success", Details: payload,
	})

	return c.Status(202).JSON(fiber.Map{
		"message": "reconciliation check dispatched",
		"tier":    tier,
		"table":   table,
		"job_id":  res.JobID,
	})
}

// TriggerCheckAll dispatches Tier 1 check for all tables
func (h *ReconciliationHandler) TriggerCheckAll(c *fiber.Ctx) error {
	var scope reconScopeRequest
	_ = c.BodyParser(&scope)
	tier := c.Query("tier", "1")
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	table, err := h.resolveTargetTable(c, scope)
	if err == nil && table != "" {
		if _, derr := h.bus.Dispatch(ctx, commands.ReconCheckCommand{Tier: tier, Table: table}); derr != nil {
			return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
		}
		return c.Status(202).JSON(fiber.Map{"message": "reconciliation check dispatched", "tier": tier, "table": table})
	}

	if _, derr := h.bus.Dispatch(ctx, commands.ReconCheckCommand{Tier: "1", Table: "*"}); derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "tier 1 check dispatched for all tables"})
}
