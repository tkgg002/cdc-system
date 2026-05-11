package api

import (
	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"github.com/gofiber/fiber/v2"
)

// RetryFailedLog retries a single failed record
func (h *ReconciliationHandler) RetryFailedLog(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	log, err := h.reader.GetFailedLogByID(c.UserContext(), int64(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "record not found"})
	}

	scope, _ := h.reader.GetRetryScopeByLogID(c.UserContext(), int64(id))

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.RetryFailedCommand{
		FailedLogID:    log.ID,
		TargetTable:    log.TargetTable,
		RecordID:       log.RecordID,
		RawJSON:        string(log.RawJSON),
		SourceDatabase: scope.SourceDatabase,
		SourceTable:    scope.ResolvedSourceTable,
		ShadowSchema:   scope.ShadowSchema,
		ShadowTable:    scope.ShadowTable,
		ScopeAmbiguous: scope.ScopeAmbiguous,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	markIdem := c.Get("Idempotency-Key")
	if markIdem != "" {
		markIdem += ":mark"
	}
	markCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), markIdem)
	markCmd := commands.MarkFailedLogRetryingCommand{FailedLogID: log.ID, UpdatedBy: user}
	_, _ = h.bus.Execute(markCtx, markCmd)

	return c.Status(202).JSON(fiber.Map{
		"message": "retry dispatched",
		"id":      id,
		"job_id":  res.JobID,
		"scope": fiber.Map{
			"source_database": queries.StringOrNil(scope.SourceDatabase),
			"source_table":    queries.StringOrNil(scope.ResolvedSourceTable),
			"shadow_schema":   queries.StringOrNil(scope.ShadowSchema),
			"shadow_table":    queries.StringOrNil(scope.ShadowTable),
			"scope_ambiguous": scope.ScopeAmbiguous,
		},
	})
}
