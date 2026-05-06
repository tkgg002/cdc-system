package api

import (
	"strconv"

	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// SourcesHandler serves the Connection Fingerprint registry
// (Systematic Flow F-1.2/1.3). Reads only — writes are side effects of
// /api/v1/system/connectors Create/Delete. Read paths delegate to
// `internal/app/queries/list_sources.go`.
type SourcesHandler struct {
	logger *zap.Logger
	listQ  *queries.ListSourcesHandler
	getQ   *queries.GetSourceHandler
}

func NewSourcesHandler(
	logger *zap.Logger,
	listQ *queries.ListSourcesHandler,
	getQ *queries.GetSourceHandler,
) *SourcesHandler {
	return &SourcesHandler{logger: logger, listQ: listQ, getQ: getQ}
}

// List returns every non-deleted source.
// GET /api/v1/sources
func (h *SourcesHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.UserContext(), queries.ListSourcesQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "list sources: " + err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
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
	res, err := h.getQ.Handle(c.UserContext(), queries.GetSourceQuery{ID: id})
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	return c.JSON(res.Source)
}
