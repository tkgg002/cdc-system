package api

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransmuteScheduleHandler — Sprint 5 dashboard UI for Cron/Immediate/
// Post-ingest schedules. Mounts under /api/v1/schedules/*. Reads
// delegate to `internal/app/queries/list_transmute_schedules.go`;
// writes still live here (P3 will move them to commands).
type TransmuteScheduleHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	bus    ports.CommandBus
	logger *zap.Logger
	cronP  cron.Parser
	listQ  *queries.ListTransmuteSchedulesHandler
}

func NewTransmuteScheduleHandler(
	db *gorm.DB,
	nats *natsconn.NatsClient,
	bus ports.CommandBus,
	logger *zap.Logger,
	listQ *queries.ListTransmuteSchedulesHandler,
) *TransmuteScheduleHandler {
	return &TransmuteScheduleHandler{
		db:     db,
		nats:   nats,
		bus:    bus,
		logger: logger,
		cronP:  cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		listQ:  listQ,
	}
}

var schedNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ScheduleRow re-exported from queries via type alias so any external
// caller / Swagger tooling keeps compiling.
type ScheduleRow = queries.TransmuteScheduleRow

// List — GET /api/v1/schedules
func (h *TransmuteScheduleHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.UserContext(), queries.ListTransmuteSchedulesQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
}

type ScheduleCreateRequest struct {
	MasterTable string `json:"master_table"`
	Mode        string `json:"mode"`     // cron|immediate|post_ingest
	CronExpr    string `json:"cron_expr"` // required if mode=cron
	IsEnabled   bool   `json:"is_enabled"`
	Reason      string `json:"reason"`
}

// Create — POST /api/v1/schedules (destructive).
func (h *TransmuteScheduleHandler) Create(c *fiber.Ctx) error {
	var req ScheduleCreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json"})
	}
	if !schedNameRe.MatchString(req.MasterTable) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_table"})
	}
	validMode := map[string]bool{"cron": true, "immediate": true, "post_ingest": true}
	if !validMode[req.Mode] {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_mode"})
	}
	if req.Mode == "cron" {
		if strings.TrimSpace(req.CronExpr) == "" {
			return c.Status(400).JSON(fiber.Map{"error": "cron_expr_required_for_cron_mode"})
		}
		if _, err := h.cronP.Parse(req.CronExpr); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid_cron", "detail": err.Error()})
		}
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)

	var nextRunAt *time.Time
	if req.Mode == "cron" {
		s, _ := h.cronP.Parse(req.CronExpr)
		next := s.Next(time.Now())
		nextRunAt = &next
	}

	err := h.db.WithContext(c.Context()).Exec(
		`INSERT INTO cdc_system.transmute_schedule
		   (master_table, mode, cron_expr, next_run_at, is_enabled, created_by, created_at, updated_at)
		 VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, NOW(), NOW())
		 ON CONFLICT (master_table, mode) DO UPDATE
		   SET cron_expr = EXCLUDED.cron_expr,
		       next_run_at = EXCLUDED.next_run_at,
		       is_enabled = EXCLUDED.is_enabled,
		       updated_at = NOW()`,
		req.MasterTable, req.Mode, req.CronExpr, nextRunAt, req.IsEnabled, actor,
	).Error
	if err != nil {
		h.logger.Error("schedule upsert failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.Status(201).JSON(fiber.Map{"status": "created", "master_table": req.MasterTable, "mode": req.Mode})
}

type ScheduleToggleRequest struct {
	IsEnabled bool   `json:"is_enabled"`
	Reason    string `json:"reason"`
}

// Toggle — PATCH /api/v1/schedules/:id (destructive).
func (h *TransmuteScheduleHandler) Toggle(c *fiber.Ctx) error {
	id := c.Params("id")
	var req ScheduleToggleRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}
	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_system.transmute_schedule
		    SET is_enabled = ?, updated_at = NOW()
		  WHERE id = ?`,
		req.IsEnabled, id,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"status": "toggled", "id": id, "is_enabled": req.IsEnabled})
}

// RunNow — POST /api/v1/schedules/:id/run-now (destructive). Publishes
// cdc.cmd.transmute immediately; separate from scheduler tick.
func (h *TransmuteScheduleHandler) RunNow(c *fiber.Ctx) error {
	id := c.Params("id")
	var row ScheduleRow
	err := h.db.WithContext(c.Context()).Raw(`
		SELECT
			ts.id,
			mb.master_table        AS master_table,
			ts.mode,
			ts.cron_expr,
			ts.last_run_at,
			ts.next_run_at,
			ts.last_status,
			ts.last_error,
			ts.last_stats,
			ts.is_enabled,
			ts.created_by,
			ts.created_at,
			ts.updated_at
		FROM cdc_system.transmute_schedule ts
		LEFT JOIN cdc_system.master_binding mb ON mb.id = ts.master_binding_id
		WHERE ts.id = ?
		LIMIT 1
	`, id).Scan(&row).Error
	if err != nil || row.ID == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	actor := getActor(c)
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
	cmd := commands.TransmuteRunCommand{
		MasterTable:   row.MasterTable,
		TriggeredBy:   actor,
		CorrelationID: "run-now-" + id + "-" + time.Now().UTC().Format(time.RFC3339Nano),
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(502).JSON(fiber.Map{"error": "publish_failed", "detail": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{
		"status":       "dispatched",
		"id":           id,
		"master_table": row.MasterTable,
		"job_id":       res.JobID,
	})
}

// ---- Mapping-rule preview for JsonPath editor (§Dashboard.1) ----

// PreviewRequest — POST /api/v1/mapping-rules/preview body.
type PreviewRequest struct {
	ShadowTable string `json:"shadow_table"`
	JSONPath    string `json:"jsonpath"`
	TransformFn string `json:"transform_fn"`
	DataType    string `json:"data_type"`
	SampleLimit int    `json:"sample_limit"`
}

// PreviewResult — one per sample row.
type PreviewResult struct {
	SourceID   string          `json:"source_id"`
	RawAfter   json.RawMessage `json:"raw_after"`
	Extracted  any             `json:"extracted"`
	Violation  string          `json:"violation,omitempty"`
	ParseError string          `json:"parse_error,omitempty"`
}
