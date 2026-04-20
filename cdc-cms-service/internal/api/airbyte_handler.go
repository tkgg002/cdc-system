package api

import (
	"encoding/json"
	"sort"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/airbyte"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type AirbyteHandler struct {
	client      *airbyte.Client
	repo        *repository.RegistryRepo
	mappingRepo *repository.MappingRuleRepo
	natsClient  *natsconn.NatsClient
	db          *gorm.DB
	logger      *zap.Logger
}

func NewAirbyteHandler(client *airbyte.Client, repo *repository.RegistryRepo, mappingRepo *repository.MappingRuleRepo, natsClient *natsconn.NatsClient, db *gorm.DB, logger *zap.Logger) *AirbyteHandler {
	return &AirbyteHandler{client: client, repo: repo, mappingRepo: mappingRepo, natsClient: natsClient, db: db, logger: logger}
}

// ListSources godoc
// @Summary      List Airbyte Sources
// @Description  Fetches all configured source connectors from Airbyte
// @Tags         Airbyte
// @Produce      json
// @Success      200 {array} airbyte.Source
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/airbyte/sources [get]
func (h *AirbyteHandler) ListSources(c *fiber.Ctx) error {
	sources, err := h.client.ListSources(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(sources)
}
// ListJobs godoc
// @Summary      List Airbyte Jobs
// @Description  Fetches the 20 most recent sync jobs from Airbyte
// @Tags         Airbyte
// @Produce      json
// @Success      200 {array} airbyte.JobStatus
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/airbyte/jobs [get]
func (h *AirbyteHandler) ListJobs(c *fiber.Ctx) error {
	// 1. Get all active registry entries to find connection IDs
	entries, _, err := h.repo.GetAll(c.Context(), repository.RegistryFilter{
		PageSize: 100,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to list registry: " + err.Error()})
	}

	// 2. Collect unique connection IDs
	connIDs := make(map[string]bool)
	for _, entry := range entries {
		if entry.AirbyteConnectionID != nil && *entry.AirbyteConnectionID != "" {
			connIDs[*entry.AirbyteConnectionID] = true
		}
	}

	if len(connIDs) == 0 {
		return c.JSON([]interface{}{})
	}

	// 3. Fetch jobs for EACH connection
	// Note: We could do this in parallel, but for 10-20 connections sequential is acceptable for MVP
	var allJobs []airbyte.JobStatus
	for connID := range connIDs {
		jobs, err := h.client.ListJobs(c.Context(), connID)
		if err != nil {
			h.logger.Warn("failed to fetch jobs for connection", zap.String("id", connID), zap.Error(err))
			continue
		}
		allJobs = append(allJobs, jobs...)
	}

	// 4. Sort and Limit (Keep top 20 recent jobs)
	sort.Slice(allJobs, func(i, j int) bool {
		return allJobs[i].CreatedAt.After(allJobs[j].CreatedAt)
	})

	if len(allJobs) > 20 {
		allJobs = allJobs[:20]
	}

	return c.JSON(allJobs)
}

// GetSyncAudit godoc
// @Summary      Audit Sync State
// @Description  Compares Airbyte connection state with CMS Registry to find inconsistencies
// @Tags         Airbyte
// @Produce      json
// @Success      200 {array} airbyte.StreamAudit
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/airbyte/sync-audit [get]
func (h *AirbyteHandler) GetSyncAudit(c *fiber.Ctx) error {
	// 1. Get all registry entries
	entries, _, err := h.repo.GetAll(c.Context(), repository.RegistryFilter{PageSize: 1000})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to list registry"})
	}

	registryTables := make([]string, 0)
	activeMap := make(map[string]bool)
	connIDs := make(map[string]bool)

	for _, e := range entries {
		registryTables = append(registryTables, e.SourceTable)
		activeMap[e.SourceTable] = e.IsActive
		if e.AirbyteConnectionID != nil && *e.AirbyteConnectionID != "" {
			connIDs[*e.AirbyteConnectionID] = true
		}
	}

	// 2. Fetch all connections (using workspace ID from first entry or config)
	// For simplicity, we'll iterate through connection IDs found in registry
	var allAudits []airbyte.StreamAudit
	for connID := range connIDs {
		conn, err := h.client.GetConnection(c.Context(), connID)
		if err != nil {
			h.logger.Warn("failed to fetch connection for audit", zap.String("id", connID), zap.Error(err))
			continue
		}

		audits := airbyte.CompareCatalog(conn, registryTables, activeMap)
		allAudits = append(allAudits, audits...)
	}

	return c.JSON(allAudits)
}

// ImportStream defines the request for ExecuteImport
type ImportStream struct {
	ConnectionID string   `json:"connection_id"`
	StreamNames  []string `json:"stream_names"`
}

// ListImportableStreams godoc
// @Summary      List importable streams
// @Description  Shows streams from Airbyte that are not yet in the CMS Registry
// @Tags         Airbyte
// @Produce      json
// @Success      200 {array} map[string]interface{}
// @Security     BearerAuth
// @Router       /api/airbyte/import/list [get]
func (h *AirbyteHandler) ListImportableStreams(c *fiber.Ctx) error {
	// 1. Get all registry entries to know what's already imported
	entries, _, err := h.repo.GetAll(c.Context(), repository.RegistryFilter{PageSize: 1000})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to list registry"})
	}
	existingMap := make(map[string]bool)
	for _, e := range entries {
		existingMap[e.SourceTable] = true
	}

	// 2. Fetch all connections
	conns, err := h.client.ListConnections(c.Context(), "") // client uses its internal workspaceID
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to list connections: " + err.Error()})
	}

	type ImportableStream struct {
		ConnectionID   string `json:"connection_id"`
		ConnectionName string `json:"connection_name"`
		StreamName     string `json:"stream_name"`
		Namespace      string `json:"namespace"`
		IsRegistered   bool   `json:"is_registered"`
	}

	var result []ImportableStream
	for _, conn := range conns {
		for _, s := range conn.SyncCatalog.Streams {
			result = append(result, ImportableStream{
				ConnectionID:   conn.ConnectionID,
				ConnectionName: conn.Name,
				StreamName:     s.Stream.Name,
				Namespace:      s.Stream.Namespace,
				IsRegistered:   existingMap[s.Stream.Name],
			})
		}
	}

	return c.JSON(result)
}

// ExecuteImport godoc
// @Summary      Execute smart import
// @Description  Imports selected streams from Airbyte into CMS Registry and creates default mapping rules
// @Tags         Airbyte
// @Accept       json
// @Produce      json
// @Param        body body ImportStream true "Import configuration"
// @Success      201 {object} map[string]interface{}
// @Security     BearerAuth
// @Router       /api/airbyte/import/execute [post]
func (h *AirbyteHandler) ExecuteImport(c *fiber.Ctx) error {
	var req ImportStream
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.ConnectionID == "" || len(req.StreamNames) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "connection_id and stream_names are required"})
	}

	// Rule B: CMS không fetch catalog + không tạo CDC table đồng bộ.
	// Dispatch to Worker via NATS — Worker sẽ: fetch Airbyte catalog, insert
	// cdc_table_registry, create mapping rules, create CDC table.
	payload, _ := json.Marshal(map[string]interface{}{
		"connection_id": req.ConnectionID,
		"stream_names":  req.StreamNames,
		"triggered_by":  middleware.GetUsername(c),
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.import-streams", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + err.Error()})
	}

	// Reload signal cho worker subscribers
	h.natsClient.PublishReload("*", middleware.GetUsername(c), "airbyte_import", "")

	return c.Status(202).JSON(fiber.Map{
		"message":       "import-streams command accepted",
		"connection_id": req.ConnectionID,
		"stream_count":  len(req.StreamNames),
	})
}

// ListDestinations returns all Airbyte destinations
func (h *AirbyteHandler) ListDestinations(c *fiber.Ctx) error {
	destinations, err := h.client.ListDestinations(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(destinations)
}

// ListConnectionDetails returns connections with stream count summary
func (h *AirbyteHandler) ListConnectionDetails(c *fiber.Ctx) error {
	details, err := h.client.ListConnectionDetails(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(details)
}

// GetConnectionStreams returns streams of a connection + registry comparison
func (h *AirbyteHandler) GetConnectionStreams(c *fiber.Ctx) error {
	connID := c.Params("id")
	streams, err := h.client.GetConnectionStreams(c.Context(), connID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Enrich with registry info
	for i, s := range streams {
		var count int64
		h.db.Model(&model.TableRegistry{}).Where("source_table = ? AND airbyte_connection_id = ?", s.Name, connID).Count(&count)
		streams[i].IsRegistered = count > 0
	}

	return c.JSON(fiber.Map{"connection_id": connID, "streams": streams, "total": len(streams)})
}
