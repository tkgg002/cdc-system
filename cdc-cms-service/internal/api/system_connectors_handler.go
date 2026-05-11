package api

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	infrahttp "cdc-cms-service/internal/infra/http"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// SystemConnectorsHandler is the admin-plane proxy for Kafka Connect REST.
// Debezium connectors directly from the CMS UI.
//
// All reads go through the shared (admin|operator) chain; mutations
// (restart / pause / resume / task-restart) go through the destructive
// chain (JWT → RequireOpsAdmin → Idempotency → Audit). Router wires
// this distinction, not the handler.
//
// Phase 2 v2 / P2.T2.6 — Kafka Connect HTTP plumbing extracted to
// `internal/infra/http/kafka_connect.go`. Reads delegate to query
// handlers in `internal/app/queries/list_connectors.go`. Writes still
// live here; P3 will move them to commands + worker.
type SystemConnectorsHandler struct {
	client     *infrahttp.KafkaConnectClient
	sourceRepo ports.SystemConnectorRepo
	bus        ports.CommandBus
	logger     *zap.Logger
	listQ      *queries.ListConnectorsHandler
	getQ       *queries.GetConnectorHandler
	pluginsQ   *queries.ListConnectorPluginsHandler
}

// NewSystemConnectorsHandler wires the proxy with the Source fingerprint
// repo. sourceRepo may be nil in test builds — Create falls back to
// best-effort Warn logging when it is missing.
//
// The same `client` is shared with the query handlers (one connection
// pool / timeout config across all 8 endpoints). All 6 destructive
// operations route through the CommandBus for cdc_jobs audit +
// idempotency replay.
func NewSystemConnectorsHandler(
	client *infrahttp.KafkaConnectClient,
	sourceRepo ports.SystemConnectorRepo,
	bus ports.CommandBus,
	logger *zap.Logger,
	listQ *queries.ListConnectorsHandler,
	getQ *queries.GetConnectorHandler,
	pluginsQ *queries.ListConnectorPluginsHandler,
) *SystemConnectorsHandler {
	return &SystemConnectorsHandler{
		client:     client,
		sourceRepo: sourceRepo,
		bus:        bus,
		logger:     logger,
		listQ:      listQ,
		getQ:       getQ,
		pluginsQ:   pluginsQ,
	}
}

var connectorNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,128}$`)

// ---- READ routes (shared chain) ----

// List returns every connector with its task-level state + config.
// GET /api/v1/system/connectors
func (h *SystemConnectorsHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.UserContext(), queries.ListConnectorsQuery{})
	if err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "kafka_connect_unreachable", "detail": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
}

// Get fetches full status + config for a single connector.
// GET /api/v1/system/connectors/:name
func (h *SystemConnectorsHandler) Get(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	res, err := h.getQ.Handle(c.UserContext(), queries.GetConnectorQuery{Name: name})
	if err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "status_lookup_failed", "detail": err.Error()})
	}
	return c.JSON(fiber.Map{
		"name":   res.Name,
		"status": res.Status,
		"config": res.Config,
	})
}

// Plugins lists installed connector plugins for the new-source wizard.
// GET /api/v1/system/connector-plugins
func (h *SystemConnectorsHandler) Plugins(c *fiber.Ctx) error {
	res, err := h.pluginsQ.Handle(c.UserContext(), queries.ListConnectorPluginsQuery{})
	if err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "plugins_lookup_failed", "detail": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
}

// ---- WRITE routes (destructive chain) ----

// Restart triggers a full connector restart (connector + tasks).
// POST /api/v1/system/connectors/:name/restart
func (h *SystemConnectorsHandler) Restart(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if err := h.dispatchLifecycle(c, name, "restart", ""); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "restart_failed", "detail": err.Error()})
	}
	h.logger.Info("connector restarted", zap.String("connector", name))
	return c.Status(202).JSON(fiber.Map{"status": "restart_triggered", "connector": name})
}

// RestartTask restarts a single failed task.
// POST /api/v1/system/connectors/:name/tasks/:taskId/restart
func (h *SystemConnectorsHandler) RestartTask(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	taskID := strings.TrimSpace(c.Params("taskId"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if matched, _ := regexp.MatchString(`^\d{1,4}$`, taskID); !matched {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_task_id"})
	}
	if err := h.dispatchLifecycle(c, name, "restart-task", taskID); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "task_restart_failed", "detail": err.Error()})
	}
	h.logger.Info("connector task restarted",
		zap.String("connector", name), zap.String("task_id", taskID))
	return c.Status(202).JSON(fiber.Map{"status": "task_restart_triggered", "connector": name, "task_id": taskID})
}

// Create forwards a new connector config to Kafka Connect.
// POST /api/v1/system/connectors
// Body: {"name": "...", "config": {"connector.class": "...", ...}}
func (h *SystemConnectorsHandler) Create(c *fiber.Ctx) error {
	var req struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json"})
	}
	if !connectorNameRE.MatchString(req.Name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if len(req.Config) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "config_required"})
	}
	if _, ok := req.Config["connector.class"]; !ok {
		return c.Status(400).JSON(fiber.Map{"error": "connector.class_required"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
	var fingerprint *model.Source
	if h.sourceRepo != nil {
		fp := parseFingerprint(req.Config)
		rawCfg, _ := json.Marshal(infrahttp.FilterSafeConfig(req.Config))
		fingerprint = &model.Source{
			ConnectorName:         req.Name,
			SourceType:            fp.sourceType,
			ConnectorClass:        req.Config["connector.class"],
			TopicPrefix:           fp.topicPrefix,
			ServerAddress:         fp.serverAddress,
			DatabaseIncludeList:   fp.dbList,
			CollectionIncludeList: fp.collectionList,
			RawConfigSanitized:    rawCfg,
			Status:                "created",
			CreatedBy:             user,
		}
	}
	cmd := commands.CreateSystemConnectorCommand{
		Name:        req.Name,
		Config:      req.Config,
		Fingerprint: fingerprint,
		CreatedBy:   user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "connector_create_failed", "detail": err.Error()})
	}
	h.logger.Info("connector created", zap.String("connector", req.Name))

	var body map[string]interface{}
	_ = json.Unmarshal(res.ResultBody, &body)
	return c.Status(201).JSON(body)
}

// UpdateConfig updates an existing connector config and refreshes the
// persisted source fingerprint row.
// PATCH /api/v1/system/connectors/:name/config
func (h *SystemConnectorsHandler) UpdateConfig(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	var req struct {
		Config map[string]string `json:"config"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json"})
	}
	if len(req.Config) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "config_required"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
	fp := parseFingerprint(req.Config)
	rawCfg, _ := json.Marshal(infrahttp.FilterSafeConfig(req.Config))
	cmd := commands.UpdateSystemConnectorConfigCommand{
		Name:   name,
		Config: req.Config,
		Fingerprint: &model.Source{
			ConnectorName:         name,
			SourceType:            fp.sourceType,
			ConnectorClass:        req.Config["connector.class"],
			TopicPrefix:           fp.topicPrefix,
			ServerAddress:         fp.serverAddress,
			DatabaseIncludeList:   fp.dbList,
			CollectionIncludeList: fp.collectionList,
			RawConfigSanitized:    rawCfg,
			Status:                "created",
			CreatedBy:             user,
		},
		UpdatedBy: user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "connector_update_failed", "detail": err.Error()})
	}
	var body map[string]interface{}
	_ = json.Unmarshal(res.ResultBody, &body)
	return c.Status(200).JSON(body)
}

// Delete removes a connector (use with care — consumer offsets may replay).
// DELETE /api/v1/system/connectors/:name
func (h *SystemConnectorsHandler) Delete(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
	cmd := commands.DeleteSystemConnectorCommand{Name: name, DeletedBy: user}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	if _, err := h.bus.Execute(ctx, cmd); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "delete_failed", "detail": err.Error()})
	}
	h.logger.Info("connector deleted", zap.String("connector", name))
	return c.Status(202).JSON(fiber.Map{"status": "delete_triggered", "connector": name})
}

// Pause / Resume for maintenance.
// POST /api/v1/system/connectors/:name/pause
func (h *SystemConnectorsHandler) Pause(c *fiber.Ctx) error {
	return h.lifecycleOp(c, "pause")
}

// POST /api/v1/system/connectors/:name/resume
func (h *SystemConnectorsHandler) Resume(c *fiber.Ctx) error {
	return h.lifecycleOp(c, "resume")
}

func (h *SystemConnectorsHandler) lifecycleOp(c *fiber.Ctx, op string) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if err := h.dispatchLifecycle(c, name, op, ""); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": op + "_failed", "detail": err.Error()})
	}
	h.logger.Info("connector lifecycle op",
		zap.String("connector", name), zap.String("op", op))
	return c.Status(202).JSON(fiber.Map{"status": op + "_triggered", "connector": name})
}

// dispatchLifecycle is the shared bus.Execute wrapper for restart /
// restart-task / pause / resume. Caller validates name + taskID and
// maps the bus error back to its endpoint-specific 502 message.
func (h *SystemConnectorsHandler) dispatchLifecycle(c *fiber.Ctx, name, op, taskID string) error {
	if h.bus == nil {
		return errors.New("command bus not ready")
	}
	user := middleware.GetUsername(c)
	cmd := commands.LifecycleSystemConnectorCommand{
		Name:      name,
		Operation: op,
		TaskID:    taskID,
		Actor:     user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	_, err := h.bus.Execute(ctx, cmd)
	return err
}

// fingerprint is the minimal set of identity fields the CMS keeps
// for each connector — enough to rebuild "who watches what" without
// having to hit Kafka Connect.
type fingerprint struct {
	sourceType     string
	topicPrefix    string
	serverAddress  string
	dbList         string
	collectionList string
}

// parseFingerprint extracts the identity fields from a Kafka Connect
// connector config. Driven by connector.class so we pick the right
// source-specific keys (MongoDB vs MySQL vs Postgres).
func parseFingerprint(cfg map[string]string) fingerprint {
	fp := fingerprint{topicPrefix: cfg["topic.prefix"]}
	cls := cfg["connector.class"]
	switch {
	case strings.Contains(cls, "MongoDb"):
		fp.sourceType = "mongodb"
		fp.serverAddress = cfg["mongodb.connection.string"]
		fp.dbList = cfg["database.include.list"]
		fp.collectionList = cfg["collection.include.list"]
	case strings.Contains(cls, "MySql"):
		fp.sourceType = "mysql"
		fp.serverAddress = joinHostPort(cfg["database.hostname"], cfg["database.port"])
		fp.dbList = cfg["database.include.list"]
		fp.collectionList = cfg["table.include.list"]
	case strings.Contains(cls, "Postgres"):
		fp.sourceType = "postgresql"
		fp.serverAddress = joinHostPort(cfg["database.hostname"], cfg["database.port"])
		fp.dbList = cfg["database.dbname"]
		fp.collectionList = cfg["table.include.list"]
	default:
		fp.sourceType = "unknown"
	}
	return fp
}

func joinHostPort(host, port string) string {
	if host == "" {
		return ""
	}
	if port == "" {
		return host
	}
	return host + ":" + port
}
