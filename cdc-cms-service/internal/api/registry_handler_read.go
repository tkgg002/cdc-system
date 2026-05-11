package api

import (
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
)

func (h *RegistryHandler) List(c *fiber.Ctx) error {
	filter := ports.RegistryFilter{
		Page:     intQuery(c, "page", 1),
		PageSize: intQuery(c, "page_size", 20),
	}
	if v := c.Query("source_db"); v != "" {
		filter.SourceDB = &v
	}
	if v := c.Query("sync_engine"); v != "" {
		filter.SyncEngine = &v
	}
	if v := c.Query("priority"); v != "" {
		filter.Priority = &v
	}
	if v := c.Query("is_active"); v != "" {
		b := v == "true"
		filter.IsActive = &b
	}
	if v := c.Query("destination_id"); v != "" {
		filter.DestinationID = &v
	}

	entries, total, err := h.repo.GetAll(c.Context(), filter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch registry"})
	}

	return c.JSON(fiber.Map{"data": entries, "total": total, "page": filter.Page})
}

// Register is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/register facade.
func (h *RegistryHandler) GetStats(c *fiber.Ctx) error {
	stats, err := h.repo.GetStats(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to get stats"})
	}
	return c.JSON(stats)
}

// Standardize is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id/standardize facade.
func (h *RegistryHandler) SyncHealth(c *fiber.Ctx) error {
	res, err := h.syncHealthQ.Handle(c.UserContext(), queries.GetSyncHealthQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(res.Snapshot)
}
