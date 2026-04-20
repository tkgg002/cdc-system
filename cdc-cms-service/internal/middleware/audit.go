// Package middleware — audit.go (Phase 4 Security)
//
// Audit logging for destructive admin endpoints. Every request that
// reaches a handler wrapped with Audit() produces exactly one row in
// `admin_actions` (see migrations/005_admin_actions.sql).
//
// Design choices:
//
//   1. Async write path: the hot HTTP reply is never blocked on a DB
//      insert. Audit events are pushed into a buffered channel and
//      drained by a single worker goroutine that batches inserts. If
//      the channel is full (slow DB, long queue) we DROP the OLDEST
//      event and increment `cdc_audit_log_dropped_total`. Dropping
//      oldest is preferred over newest because stale audit events are
//      less valuable for real-time security response.
//
//   2. Action name: derived from the route path. The router passes a
//      mapping (path → action) through the config. Unknown routes fall
//      back to "<METHOD>_<path>" so nothing is silently un-audited.
//
//   3. Payload capture: we clone the request body before c.Next() so
//      that handlers which consume it still work. Size is capped at
//      64 KiB to protect Postgres. Larger bodies are truncated with a
//      trailing marker.
//
//   4. Security: user_agent and ip_address come from request headers
//      → treat them as untrusted input. We store raw (for forensics)
//      but cap length. The `reason` field is required on the request
//      body — a missing reason returns 400.
package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	auditQueueSize   = 100
	auditPayloadCap  = 64 * 1024
	auditReasonMin   = 10
	auditUACap       = 512
	auditTargetCap   = 256
	auditFlushPeriod = 2 * time.Second
)

// AuditEvent mirrors a row in admin_actions. Only the fields we set
// explicitly are included — `id` and `created_at` are server-generated.
type AuditEvent struct {
	UserID         string
	Action         string
	Target         string
	Payload        []byte // raw JSON, capped
	Reason         string
	Result         string
	IdempotencyKey string
	IPAddress      string
	UserAgent      string
	CreatedAt      time.Time
}

// AuditLogger owns the async write pipeline. One instance per server.
type AuditLogger struct {
	db      *gorm.DB
	logger  *zap.Logger
	queue   chan AuditEvent
	dropped atomic.Int64

	// ActionMap resolves route patterns → canonical action names.
	// Example: "/api/reconciliation/heal/:table" → "heal".
	ActionMap map[string]string

	stopOnce sync.Once
	stopped  chan struct{}
}

// NewAuditLogger wires the async writer. The caller MUST start it via
// Run() and cancel via Stop() on shutdown.
func NewAuditLogger(db *gorm.DB, logger *zap.Logger, actionMap map[string]string) *AuditLogger {
	if actionMap == nil {
		actionMap = defaultActionMap()
	}
	return &AuditLogger{
		db:        db,
		logger:    logger,
		queue:     make(chan AuditEvent, auditQueueSize),
		ActionMap: actionMap,
		stopped:   make(chan struct{}),
	}
}

// Run drains the queue until ctx is cancelled. Intended to be invoked
// in its own goroutine by the server bootstrap.
func (a *AuditLogger) Run(ctx context.Context) {
	defer close(a.stopped)
	ticker := time.NewTicker(auditFlushPeriod)
	defer ticker.Stop()

	batch := make([]AuditEvent, 0, 16)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := a.writeBatch(ctx, batch); err != nil {
			a.logger.Error("audit batch insert failed",
				zap.Int("size", len(batch)), zap.Error(err))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining before exit — best-effort.
			for len(a.queue) > 0 {
				batch = append(batch, <-a.queue)
			}
			flush()
			return
		case ev := <-a.queue:
			batch = append(batch, ev)
			if len(batch) >= 16 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Stop is a graceful no-op helper — Run exits on ctx cancel.
func (a *AuditLogger) Stop() {
	a.stopOnce.Do(func() {})
}

// DroppedCount is used by metrics endpoints (expose as
// cdc_audit_log_dropped_total).
func (a *AuditLogger) DroppedCount() int64 { return a.dropped.Load() }

// writeBatch performs a single multi-row INSERT via GORM raw SQL.
// We bypass the GORM model layer because admin_actions has no Go
// model yet and we don't want to pull one in just for this logger.
func (a *AuditLogger) writeBatch(parent context.Context, batch []AuditEvent) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	// Build a multi-row INSERT. We use parameterized placeholders —
	// never string-concatenate user input (defends against SQL
	// injection via payload or reason).
	var (
		sb   strings.Builder
		args []interface{}
	)
	sb.WriteString("INSERT INTO admin_actions ")
	sb.WriteString("(user_id, action, target, payload, reason, result, idempotency_key, ip_address, user_agent, created_at) VALUES ")
	for i, ev := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)")
		payload := ev.Payload
		if payload == nil {
			payload = []byte("null")
		}
		args = append(args,
			ev.UserID, ev.Action, ev.Target, string(payload),
			ev.Reason, ev.Result, ev.IdempotencyKey,
			ev.IPAddress, ev.UserAgent, ev.CreatedAt,
		)
	}

	return a.db.WithContext(ctx).Exec(sb.String(), args...).Error
}

// enqueue pushes an event; on full queue it drops the OLDEST waiting
// event (non-blocking swap) and increments the dropped counter.
func (a *AuditLogger) enqueue(ev AuditEvent) {
	select {
	case a.queue <- ev:
		return
	default:
		// Queue full — drop the oldest, then push.
		select {
		case <-a.queue:
			a.dropped.Add(1)
		default:
		}
		select {
		case a.queue <- ev:
		default:
			a.dropped.Add(1)
		}
	}
}

// Middleware returns a fiber.Handler that audits the wrapped request.
//
// This must run AFTER JWTAuth (needs username) and AFTER the body is
// buffered by Fiber (default). If combined with Idempotency, order is:
//     RBAC → Idempotency → Audit → handler
//
// We capture REQUEST metadata up-front, then enqueue the event AFTER
// c.Next() so we can record the final status code. Reason comes from
// the JSON body field `reason` — a missing/too-short reason fails the
// request with 400 (same requirement as FE confirm modal).
func (a *AuditLogger) Middleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Clone body before the handler consumes/mutates it.
		rawBody := bytes.Clone(c.Body())

		// Extract `reason` (required). We parse leniently — handlers
		// may want different schemas, we only peek at one field.
		reason := extractReason(rawBody)
		if len(strings.TrimSpace(reason)) < auditReasonMin {
			return c.Status(400).JSON(fiber.Map{
				"error":       "missing or too-short `reason`",
				"min_length":  auditReasonMin,
				"help":        "destructive actions require a reason (>= 10 chars)",
			})
		}

		// Build event with request-time fields. Result is filled after.
		userID := "unknown"
		if u, ok := c.Locals("username").(string); ok && u != "" {
			userID = u
		}

		route := c.Route().Path
		action := a.actionFor(c.Method(), route)

		target := deriveTarget(c)
		if len(target) > auditTargetCap {
			target = target[:auditTargetCap]
		}

		payload := rawBody
		if len(payload) > auditPayloadCap {
			payload = append([]byte{}, rawBody[:auditPayloadCap]...)
			payload = append(payload, []byte(`"<truncated>"`)...)
		}

		ua := c.Get("User-Agent")
		if len(ua) > auditUACap {
			ua = ua[:auditUACap]
		}

		ev := AuditEvent{
			UserID:         userID,
			Action:         action,
			Target:         target,
			Payload:        payload,
			Reason:         reason,
			IdempotencyKey: c.Get("Idempotency-Key"),
			IPAddress:      c.IP(),
			UserAgent:      ua,
			CreatedAt:      time.Now().UTC(),
		}

		// Run handler.
		handlerErr := c.Next()

		status := c.Response().StatusCode()
		if handlerErr != nil || status >= 400 {
			// Record the error body (capped) to help forensics.
			body := c.Response().Body()
			if len(body) > 1024 {
				body = body[:1024]
			}
			ev.Result = "error: status=" +
				itoa(status) + " body=" + string(body)
		} else {
			ev.Result = "success"
		}

		a.enqueue(ev)
		return handlerErr
	}
}

// actionFor resolves a route pattern to its canonical action name.
func (a *AuditLogger) actionFor(method, route string) string {
	if action, ok := a.ActionMap[route]; ok {
		return action
	}
	// Fallback: METHOD + path, lowercased with colons stripped.
	return strings.ToLower(method) + "_" +
		strings.ReplaceAll(strings.ReplaceAll(route, "/", "_"), ":", "")
}

// defaultActionMap — route patterns → canonical action names used by
// Phase-4 plan. Keep in sync with router.go mount points.
func defaultActionMap() map[string]string {
	return map[string]string{
		"/api/reconciliation/heal/:table":     "heal",
		"/api/reconciliation/check":           "recon_check_all",
		"/api/reconciliation/check/:table":    "recon_check",
		"/api/failed-sync-logs/:id/retry":     "retry_failed",
		"/api/tools/reset-debezium-offset":    "reset_offset",
		"/api/tools/trigger-snapshot/:table":  "trigger_snapshot",
		"/api/tools/restart-debezium":         "restart_connector",
	}
}

// deriveTarget pulls the most specific URL segment that identifies the
// resource being acted on. Preference order:
//   1. :table param (heal, snapshot, check)
//   2. :id param    (retry)
//   3. :name param  (connector)
//   4. body.collection (reset-offset)
//   5. empty string
func deriveTarget(c *fiber.Ctx) string {
	if t := c.Params("table"); t != "" {
		return t
	}
	if id := c.Params("id"); id != "" {
		return id
	}
	if n := c.Params("name"); n != "" {
		return n
	}
	// Best-effort body peek for reset-offset.
	var body struct {
		Collection string `json:"collection"`
		Database   string `json:"database"`
	}
	if err := json.Unmarshal(c.Body(), &body); err == nil {
		if body.Collection != "" {
			return body.Database + "." + body.Collection
		}
	}
	return ""
}

// extractReason parses the JSON body and returns the `reason` string
// if present. Silent on parse error — caller will reject short reasons.
func extractReason(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var probe struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		if errors.Is(err, nil) {
			return ""
		}
		return ""
	}
	return probe.Reason
}

// itoa — trivial int→string without importing strconv only for this.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [16]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
