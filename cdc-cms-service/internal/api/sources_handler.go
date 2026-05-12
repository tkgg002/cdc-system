package api

import (
	"cdc-cms-service/internal/model"
	"strconv"
	"time"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// SourcesHandler serves the Connection Fingerprint registry
type SourcesHandler struct {
	logger *zap.Logger
	listQ  *queries.ListSourcesHandler
	getQ   *queries.GetSourceHandler
	repo   ports.SystemConnectorRepo
}

func NewSourcesHandler(
	logger *zap.Logger,
	listQ *queries.ListSourcesHandler,
	getQ *queries.GetSourceHandler,
	repo ports.SystemConnectorRepo,
) *SourcesHandler {
	return &SourcesHandler{logger: logger, listQ: listQ, getQ: getQ, repo: repo}
}

// List returns every non-deleted source.
func (h *SourcesHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.UserContext(), queries.ListSourcesQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "list sources: " + err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
}

// Create registers a new source connection without a Debezium connector (Flow 1).
// POST /api/v1/sources
func (h *SourcesHandler) Create(c *fiber.Ctx) error {
	var req struct {
		ConnectionCode  string `json:"connection_code"`
		AdapterType     string `json:"adapter_type"`
		ServerAddress   string `json:"server_address"`
		DefaultDatabase string `json:"default_database"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad request"})
	}

	source := model.Source{
		ConnectorName:       req.ConnectionCode, // Use code as name for standalone sources
		SourceType:          req.AdapterType,
		ServerAddress:       req.ServerAddress,
		DatabaseIncludeList: req.DefaultDatabase,
		Status:              "created",
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}

	// Use repository which now handles both cdc_sources and connection_registry
	if err := h.repo.Upsert(c.UserContext(), &source); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to create source: " + err.Error()})
	}

	return c.Status(201).JSON(source)
}



// Get returns a single source by numeric id.
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
