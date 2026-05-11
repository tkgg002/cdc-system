// Package api — alerts_handler.go (Phase 6).
//
// HTTP surface for the alert state machine. All routes under this handler
// require JWT auth at the router level; Ack/Silence additionally require
// the `admin` role (the router wires this — see router.go). The fingerprint
// path parameter is accepted verbatim because it is a sha256 hex digest and
// cannot be reversed to the underlying label set by an attacker.
//
// Response shape: arrays of model.Alert with stable JSON tags. The FE banner
// consumes these without transformation.
package api

import (
	"errors"
	"time"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/service"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// AlertsHandler exposes the state machine over HTTP.
type AlertsHandler struct {
	am     *service.AlertManager
	logger *zap.Logger
}

// NewAlertsHandler wires the handler. The manager may be nil in degraded
// startup (e.g. DB offline); all routes will return 503 in that case.
func NewAlertsHandler(am *service.AlertManager, logger *zap.Logger) *AlertsHandler {
	return &AlertsHandler{am: am, logger: logger}
}

// Active returns firing + acknowledged alerts.
// GET /api/alerts/active
func (h *AlertsHandler) Active(c *fiber.Ctx) error {
	if h.am == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerts manager not ready"})
	}
	rows, err := h.am.ListActive(c.Context())
	if err != nil {
		h.logger.Warn("list active alerts failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "failed to list alerts"})
	}
	return c.JSON(fiber.Map{"alerts": rows, "count": len(rows)})
}

// Silenced returns alerts whose silence window is still active.
// GET /api/alerts/silenced
func (h *AlertsHandler) Silenced(c *fiber.Ctx) error {
	if h.am == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerts manager not ready"})
	}
	rows, err := h.am.ListSilenced(c.Context())
	if err != nil {
		h.logger.Warn("list silenced alerts failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "failed to list silenced alerts"})
	}
	return c.JSON(fiber.Map{"alerts": rows, "count": len(rows)})
}

// History returns resolved alerts within [from, to]. Both are optional; they
// accept RFC3339 timestamps (e.g. 2026-04-17T00:00:00Z). A missing bound
// means "open-ended on that side". `limit` caps at 500 (default 100).
// GET /api/alerts/history?from=X&to=Y&limit=N
func (h *AlertsHandler) History(c *fiber.Ctx) error {
	if h.am == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerts manager not ready"})
	}
	var from, to time.Time
	if s := c.Query("from"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid 'from' (expect RFC3339)"})
		}
		from = t
	}
	if s := c.Query("to"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid 'to' (expect RFC3339)"})
		}
		to = t
	}
	limit := c.QueryInt("limit", 100)
	rows, err := h.am.ListHistory(c.Context(), from, to, limit)
	if err != nil {
		h.logger.Warn("list alert history failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "failed to list history"})
	}
	return c.JSON(fiber.Map{"alerts": rows, "count": len(rows)})
}

// ackRequest is the JSON body for /ack. A short reason is required so the
// audit log captures intent.
type ackRequest struct {
	Reason string `json:"reason"`
}

// Ack marks an alert as acknowledged by the authenticated user.
// POST /api/alerts/:fingerprint/ack
//
// NOTE: Audit middleware from Phase 4 (Security) will wrap this route when
// merged. Until then we explicitly log the ack event with user + fingerprint
// so operators keep an append-only trail.
// TODO(phase4-audit): remove manual log once audit middleware lands.
func (h *AlertsHandler) Ack(c *fiber.Ctx) error {
	if h.am == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerts manager not ready"})
	}
	fp := c.Params("fingerprint")
	if fp == "" {
		return c.Status(400).JSON(fiber.Map{"error": "fingerprint required"})
	}
	var body ackRequest
	_ = c.BodyParser(&body)

	user := middleware.GetUsername(c)
	if err := h.am.Ack(c.Context(), fp, user); err != nil {
		if errors.Is(err, errors.New("alert not firing or not found")) || err.Error() == "alert not firing or not found" {
			return c.Status(404).JSON(fiber.Map{"error": err.Error()})
		}
		h.logger.Warn("ack failed", zap.String("fingerprint", fp), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "ack failed"})
	}
	h.logger.Info("alert acknowledged",
		zap.String("fingerprint", fp),
		zap.String("user", user),
		zap.String("reason", body.Reason))
	return c.JSON(fiber.Map{"ok": true})
}

// silenceRequest is the JSON body for /silence.
type silenceRequest struct {
	Until  time.Time `json:"until"` // RFC3339
	Reason string    `json:"reason"`
}

// Silence mutes an alert until the given deadline.
// POST /api/alerts/:fingerprint/silence
func (h *AlertsHandler) Silence(c *fiber.Ctx) error {
	if h.am == nil {
		return c.Status(503).JSON(fiber.Map{"error": "alerts manager not ready"})
	}
	fp := c.Params("fingerprint")
	if fp == "" {
		return c.Status(400).JSON(fiber.Map{"error": "fingerprint required"})
	}
	var body silenceRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	if body.Until.IsZero() {
		return c.Status(400).JSON(fiber.Map{"error": "'until' is required (RFC3339)"})
	}
	if body.Reason == "" {
		return c.Status(400).JSON(fiber.Map{"error": "'reason' is required"})
	}

	user := middleware.GetUsername(c)
	if err := h.am.Silence(c.Context(), fp, user, body.Until, body.Reason); err != nil {
		h.logger.Warn("silence failed", zap.String("fingerprint", fp), zap.Error(err))
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	h.logger.Info("alert silenced",
		zap.String("fingerprint", fp),
		zap.String("user", user),
		zap.Time("until", body.Until),
		zap.String("reason", body.Reason))
	return c.JSON(fiber.Map{"ok": true})
}
