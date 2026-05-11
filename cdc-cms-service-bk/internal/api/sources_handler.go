package api

import (
	"strconv"

	"cdc-cms-service/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// SourcesHandler serves the Connection Fingerprint registry
// (Systematic Flow F-1.2/1.3). Reads only — writes are side effects of
// /api/v1/system/connectors Create/Delete.
type SourcesHandler struct {
	repo   *repository.SourceRepo
	logger *zap.Logger
}

func NewSourcesHandler(repo *repository.SourceRepo, logger *zap.Logger) *SourcesHandler {
	return &SourcesHandler{repo: repo, logger: logger}
}

// List returns every non-deleted source.
// GET /api/v1/sources
func (h *SourcesHandler) List(c *fiber.Ctx) error {
	items, err := h.repo.List(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "list sources: " + err.Error()})
	}
	return c.JSON(fiber.Map{"data": items, "count": len(items)})
}

// Get returns a single source by numeric id. Response includes
// collection_include_list so the Registry modal can populate its
// collection dropdown.
// GET /api/v1/sources/:id
func (h *SourcesHandler) Get(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	s, err := h.repo.GetByID(c.Context(), id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(s)
}
