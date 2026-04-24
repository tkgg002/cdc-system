package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

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
type SystemConnectorsHandler struct {
	kafkaConnectURL string
	httpClient      *http.Client
	logger          *zap.Logger
}

func NewSystemConnectorsHandler(kafkaConnectURL string, logger *zap.Logger) *SystemConnectorsHandler {
	return &SystemConnectorsHandler{
		kafkaConnectURL: strings.TrimRight(kafkaConnectURL, "/"),
		httpClient:      &http.Client{Timeout: 10 * time.Second},
		logger:          logger,
	}
}

var connectorNameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,128}$`)

// ---- READ routes (shared chain) ----

// List returns every connector with its task-level state + config.state.
// GET /api/v1/system/connectors
func (h *SystemConnectorsHandler) List(c *fiber.Ctx) error {
	var names []string
	if err := h.doJSON(c.Context(), http.MethodGet, "/connectors", nil, &names); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "kafka_connect_unreachable", "detail": err.Error()})
	}

	type connectorView struct {
		Name      string           `json:"name"`
		State     string           `json:"state"`
		Type      string           `json:"type"`
		Connector string           `json:"connector_class"`
		Tasks     []connectorTask  `json:"tasks"`
		Config    map[string]string `json:"config,omitempty"`
	}

	out := make([]connectorView, 0, len(names))
	for _, name := range names {
		v := connectorView{Name: name}
		// Status
		var statusResp connectorStatusResp
		if err := h.doJSON(c.Context(), http.MethodGet,
			"/connectors/"+url.PathEscape(name)+"/status", nil, &statusResp); err == nil {
			v.State = statusResp.Connector.State
			v.Type = statusResp.Type
			v.Tasks = statusResp.Tasks
		}
		// Config — lightweight subset so the response stays BI-friendly.
		var cfg map[string]string
		if err := h.doJSON(c.Context(), http.MethodGet,
			"/connectors/"+url.PathEscape(name)+"/config", nil, &cfg); err == nil {
			v.Connector = cfg["connector.class"]
			v.Config = filterSafeConfig(cfg)
		}
		out = append(out, v)
	}
	return c.JSON(fiber.Map{"data": out, "count": len(out)})
}

type connectorStatusResp struct {
	Type      string          `json:"type"`
	Connector connectorState  `json:"connector"`
	Tasks     []connectorTask `json:"tasks"`
}

type connectorState struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

type connectorTask struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

// Get fetches full status + config for a single connector.
// GET /api/v1/system/connectors/:name
func (h *SystemConnectorsHandler) Get(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}

	var status connectorStatusResp
	if err := h.doJSON(c.Context(), http.MethodGet,
		"/connectors/"+url.PathEscape(name)+"/status", nil, &status); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "status_lookup_failed", "detail": err.Error()})
	}

	var cfg map[string]string
	_ = h.doJSON(c.Context(), http.MethodGet,
		"/connectors/"+url.PathEscape(name)+"/config", nil, &cfg)

	return c.JSON(fiber.Map{
		"name":   name,
		"status": status,
		"config": filterSafeConfig(cfg),
	})
}

// Plugins lists installed connector plugins for the new-source wizard.
// GET /api/v1/system/connector-plugins
func (h *SystemConnectorsHandler) Plugins(c *fiber.Ctx) error {
	var plugins []map[string]any
	if err := h.doJSON(c.Context(), http.MethodGet, "/connector-plugins", nil, &plugins); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "plugins_lookup_failed", "detail": err.Error()})
	}
	return c.JSON(fiber.Map{"data": plugins, "count": len(plugins)})
}

// ---- WRITE routes (destructive chain) ----

// Restart triggers a full connector restart (connector + tasks).
// POST /api/v1/system/connectors/:name/restart
func (h *SystemConnectorsHandler) Restart(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	path := fmt.Sprintf("/connectors/%s/restart?includeTasks=true&onlyFailed=false", url.PathEscape(name))
	if err := h.doJSON(c.Context(), http.MethodPost, path, nil, nil); err != nil {
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
	path := fmt.Sprintf("/connectors/%s/tasks/%s/restart", url.PathEscape(name), url.PathEscape(taskID))
	if err := h.doJSON(c.Context(), http.MethodPost, path, nil, nil); err != nil {
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
	payload := map[string]any{"name": req.Name, "config": req.Config}
	var resp map[string]any
	if err := h.doJSON(c.Context(), http.MethodPost, "/connectors", payload, &resp); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "connector_create_failed", "detail": err.Error()})
	}
	h.logger.Info("connector created", zap.String("connector", req.Name))
	return c.Status(201).JSON(resp)
}

// Delete removes a connector (use with care — consumer offsets may replay).
// DELETE /api/v1/system/connectors/:name
func (h *SystemConnectorsHandler) Delete(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	if err := h.doJSON(c.Context(), http.MethodDelete, "/connectors/"+url.PathEscape(name), nil, nil); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": "delete_failed", "detail": err.Error()})
	}
	h.logger.Info("connector deleted", zap.String("connector", name))
	return c.Status(202).JSON(fiber.Map{"status": "delete_triggered", "connector": name})
}

// Pause / Resume for maintenance.
// POST /api/v1/system/connectors/:name/pause
func (h *SystemConnectorsHandler) Pause(c *fiber.Ctx) error {
	return h.lifecycleOp(c, "pause", http.MethodPut)
}

// POST /api/v1/system/connectors/:name/resume
func (h *SystemConnectorsHandler) Resume(c *fiber.Ctx) error {
	return h.lifecycleOp(c, "resume", http.MethodPut)
}

func (h *SystemConnectorsHandler) lifecycleOp(c *fiber.Ctx, op, method string) error {
	name := strings.TrimSpace(c.Params("name"))
	if !connectorNameRE.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_connector_name"})
	}
	path := fmt.Sprintf("/connectors/%s/%s", url.PathEscape(name), op)
	if err := h.doJSON(c.Context(), method, path, nil, nil); err != nil {
		return c.Status(502).JSON(fiber.Map{"error": op + "_failed", "detail": err.Error()})
	}
	h.logger.Info("connector lifecycle op",
		zap.String("connector", name), zap.String("op", op))
	return c.Status(202).JSON(fiber.Map{"status": op + "_triggered", "connector": name})
}

// ---- internal HTTP helper ----

func (h *SystemConnectorsHandler) doJSON(ctx interface{}, method, relPath string, body any, target any) error {
	u := h.kafkaConnectURL + relPath

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reqBody = strings.NewReader(string(b))
	}

	req, err := http.NewRequest(method, u, reqBody)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect call: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("kafka connect HTTP %d: %s", resp.StatusCode, string(raw))
	}
	if target == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

// filterSafeConfig strips credentials / internal-only keys before returning
// config to the UI.
func filterSafeConfig(cfg map[string]string) map[string]string {
	if cfg == nil {
		return nil
	}
	out := make(map[string]string, len(cfg))
	for k, v := range cfg {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "password") || strings.Contains(lk, "secret") ||
			strings.Contains(lk, "token") || strings.Contains(lk, "credentials") ||
			strings.Contains(lk, "ssl.key") {
			out[k] = "***"
			continue
		}
		out[k] = v
	}
	return out
}
