package api

import (
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/infra/persistence"

	"github.com/gofiber/fiber/v2"
)

func (h *RegistryHandler) DispatchStatus(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	subject := c.Query("subject")
	sinceStr := c.Query("since")

	// Normalize: subject can be either bare op ("scan-fields") or full subject ("cdc.cmd.scan-fields").
	op := subject
	op = strings.TrimPrefix(op, "cdc.cmd.")

	filter := persistence.ActivityFilter{TargetTable: entry.TargetTable, Operation: op, Limit: 50}
	if sinceStr != "" {
		if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			filter.Since = &ts
		}
	}

	entries, err := h.activityLogger.ListActivityLogs(c.UserContext(), filter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "query failed: " + err.Error()})
	}

	return c.JSON(fiber.Map{
		"target_table": entry.TargetTable,
		"operation":    op,
		"since":        sinceStr,
		"entries":      entries,
		"count":        len(entries),
	})
}

// DetectTimestampField dispatches a re-scan of the Mongo source collection
// so the worker can (re)pick the best timestamp field for recon windowing.
//
// Operators hit this when they see "SRC_FIELD_MISSING" or "timestamp_field
// confidence: low" on a row — the worker will sample the collection, score
// candidates (updated_at / lastUpdatedAt / createdAt / ...), and write the
// winner back into cdc_table_registry (timestamp_field,
// timestamp_field_source=auto, timestamp_field_confidence).
//
// Flow: CMS publishes → worker consumes cdc.cmd.detect-timestamp-field →
// worker updates registry row → next recon tick uses the new field.
//
// The public CMS route now lives under
// /api/v1/source-objects/registry/:id/detect-timestamp-field.
