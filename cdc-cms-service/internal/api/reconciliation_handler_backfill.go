package api

import (
	"encoding/json"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"
)

func (h *ReconciliationHandler) TriggerBackfillSourceTs(c *fiber.Ctx) error {
	var body struct {
		Table     string `json:"table"`
		BatchSize int    `json:"batch_size"`
	}
	if err := c.BodyParser(&body); err != nil {
		body = struct {
			Table     string `json:"table"`
			BatchSize int    `json:"batch_size"`
		}{}
	}

	runID := uuid.NewString()
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.ReconBackfillSourceTsCommand{
		Table:     body.Table,
		RunID:     runID,
		BatchSize: body.BatchSize,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	details := map[string]any{}
	if raw, err := json.Marshal(cmd); err == nil {
		_ = json.Unmarshal(raw, &details)
	}
	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-backfill-source-ts", TargetTable: body.Table, Status: "dispatched", Details: details,
	})

	return c.Status(202).JSON(fiber.Map{
		"message":    "backfill dispatched",
		"run_id":     runID,
		"table":      body.Table,
		"job_id":     res.JobID,
		"status_url": "/api/recon/backfill-source-ts/status",
	})
}

func (h *ReconciliationHandler) BackfillSourceTsStatus(c *fiber.Ctx) error {
	rows, err := h.reader.ListBackfillRuns(c.UserContext(), c.Query("table"), c.Query("run_id"), 30)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	type enriched struct {
		queries.BackfillRunRow
		TotalRows     int64   `json:"total_rows"`
		NullRemaining int64   `json:"null_remaining"`
		PercentDone   float64 `json:"percent_done"`
	}
	out := make([]enriched, 0, len(rows))
	seenTable := map[string]struct{}{}
	totals := map[string]int64{}
	remain := map[string]int64{}
	for _, r := range rows {
		if _, ok := seenTable[r.TableName]; ok {
			continue
		}
		seenTable[r.TableName] = struct{}{}
		total, nul, _ := h.reader.CountTableRows(c.UserContext(), r.TableName)
		totals[r.TableName] = total
		remain[r.TableName] = nul
	}
	for _, r := range rows {
		total := totals[r.TableName]
		nul := remain[r.TableName]
		pct := 0.0
		if total > 0 {
			pct = float64(total-nul) / float64(total) * 100.0
		}
		out = append(out, enriched{
			BackfillRunRow: r,
			TotalRows:      total,
			NullRemaining:  nul,
			PercentDone:    pct,
		})
	}

	return c.JSON(fiber.Map{"data": out, "total": len(out)})
}
