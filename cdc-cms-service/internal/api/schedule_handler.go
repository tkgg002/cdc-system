package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// ScheduleHandler — /api/worker-schedule. Read path (List) delegates
// to `internal/app/queries/list_worker_schedules.go`; writes still
// live here (P3 will move them to commands). The reader is kept on
// the struct because Create + Update need to project the post-write
// shape via GetResponseByID.
type ScheduleHandler struct {
	db     *gorm.DB
	reader queries.WorkerScheduleReader
	listQ  *queries.ListWorkerSchedulesHandler
	bus    ports.CommandBus
}

func NewScheduleHandler(
	db *gorm.DB,
	reader queries.WorkerScheduleReader,
	listQ *queries.ListWorkerSchedulesHandler,
	bus ports.CommandBus,
) *ScheduleHandler {
	return &ScheduleHandler{db: db, reader: reader, listQ: listQ, bus: bus}
}

// WorkerScheduleScope / WorkerScheduleResponse re-exported from
// queries via type alias so Swagger tooling and any external callers
// keep compiling.
type WorkerScheduleScope = queries.WorkerScheduleScope
type WorkerScheduleResponse = queries.WorkerScheduleResponse

type WorkerScheduleCreateRequest struct {
	Operation       string  `json:"operation"`
	TargetTable     *string `json:"target_table"`
	SourceDatabase  *string `json:"source_database"`
	SourceSchema    *string `json:"source_schema"`
	SourceNamespace *string `json:"source_namespace"`
	SourceTable     *string `json:"source_table"`
	ShadowSchema    *string `json:"shadow_schema"`
	ShadowTable     *string `json:"shadow_table"`
	IntervalMinutes int     `json:"interval_minutes"`
	IsEnabled       *bool   `json:"is_enabled"`
	Notes           *string `json:"notes"`
}

type WorkerScheduleUpdateRequest struct {
	IntervalMinutes *int    `json:"interval_minutes"`
	IsEnabled       *bool   `json:"is_enabled"`
	Notes           *string `json:"notes"`
}

type workerScheduleScopeCandidate struct {
	TargetTable      string
	SourceObjectID   *int64
	SourceDatabase   *string
	SourceSchema     *string
	SourceNamespace  *string
	SourceTable      *string
	ShadowBindingID  *int64
	ShadowSchema     *string
	ShadowTable      *string
	PhysicalTableFQN *string
}

// getResponseByID is a thin shim over the reader so post-write paths
// (Create / Update) can project the canonical response shape without
// owning SQL. Kept as a method to preserve the existing call sites.
func (h *ScheduleHandler) getResponseByID(ctx *fiber.Ctx, id uint) (*WorkerScheduleResponse, error) {
	return h.reader.GetResponseByID(ctx.UserContext(), id)
}

func trimmed(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	return &s
}

func (h *ScheduleHandler) resolveTargetTable(ctx *fiber.Ctx, req WorkerScheduleCreateRequest) (*workerScheduleScopeCandidate, error) {
	targetTable := trimmed(req.TargetTable)
	shadowSchema := trimmed(req.ShadowSchema)
	shadowTable := trimmed(req.ShadowTable)
	sourceDatabase := trimmed(req.SourceDatabase)
	sourceSchema := trimmed(req.SourceSchema)
	sourceNamespace := trimmed(req.SourceNamespace)
	sourceTable := trimmed(req.SourceTable)

	if targetTable == nil && shadowTable == nil && sourceTable == nil {
		return nil, nil
	}

	if targetTable != nil && shadowSchema == nil && sourceDatabase == nil && sourceSchema == nil && sourceNamespace == nil && sourceTable == nil && shadowTable == nil {
		return &workerScheduleScopeCandidate{TargetTable: *targetTable}, nil
	}

	query := `
		SELECT
			s.shadow_table AS target_table,
			s.source_object_id,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			s.id AS shadow_binding_id,
			s.shadow_schema,
			s.shadow_table,
			s.physical_table_fqn
		FROM cdc_system.shadow_binding s
		JOIN cdc_system.source_object_registry so
		  ON so.id = s.source_object_id
		WHERE s.is_active = TRUE
	`
	args := make([]interface{}, 0, 6)
	if targetTable != nil {
		query += ` AND s.shadow_table = ?`
		args = append(args, *targetTable)
	}
	if shadowSchema != nil {
		query += ` AND s.shadow_schema = ?`
		args = append(args, *shadowSchema)
	}
	if shadowTable != nil {
		query += ` AND s.shadow_table = ?`
		args = append(args, *shadowTable)
	}
	if sourceDatabase != nil {
		query += ` AND so.source_database = ?`
		args = append(args, *sourceDatabase)
	}
	if sourceSchema != nil {
		query += ` AND so.source_schema = ?`
		args = append(args, *sourceSchema)
	}
	if sourceNamespace != nil {
		query += ` AND so.source_namespace = ?`
		args = append(args, *sourceNamespace)
	}
	if sourceTable != nil {
		query += ` AND so.source_object_name = ?`
		args = append(args, *sourceTable)
	}
	query += ` ORDER BY s.updated_at DESC, s.id DESC LIMIT 2`

	var matches []workerScheduleScopeCandidate
	if err := h.db.WithContext(ctx.Context()).Raw(query, args...).Scan(&matches).Error; err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(matches) > 1 {
		return nil, errors.New("ambiguous_scope")
	}
	return &matches[0], nil
}

func boolValue(ptr *bool, fallback bool) bool {
	if ptr == nil {
		return fallback
	}
	return *ptr
}

// List godoc
// @Summary      List worker schedules
// @Description  Returns operator-flow schedules with enriched source/shadow scope from V2 metadata when available.
// @Tags         Operations
// @Produce      json
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/worker-schedule [get]
func (h *ScheduleHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.UserContext(), queries.ListWorkerSchedulesQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data})
}

// Update godoc
// @Summary      Update worker schedule
// @Description  Updates interval, enabled state, or notes for an existing operator-flow schedule.
// @Tags         Operations
// @Accept       json
// @Produce      json
// @Param        id   path int                        true  "Schedule ID"
// @Param        body body WorkerScheduleUpdateRequest true  "Schedule update payload"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/worker-schedule/{id} [patch]
func (h *ScheduleHandler) Update(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	var body WorkerScheduleUpdateRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	cmd := commands.UpdateScheduleCommand{
		ID:              int64(id),
		IntervalMinutes: body.IntervalMinutes,
		IsEnabled:       body.IsEnabled,
		Notes:           body.Notes,
		UpdatedBy:       user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	if _, err := h.bus.Execute(ctx, cmd); err != nil {
		switch {
		case errors.Is(err, commands.ErrScheduleNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "schedule_not_found"})
		case errors.Is(err, commands.ErrScheduleNoFields):
			return c.Status(400).JSON(fiber.Map{"error": "nothing to update"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
	}

	row, err := h.getResponseByID(c, uint(id))
	if err != nil {
		return c.JSON(fiber.Map{"message": "schedule updated"})
	}
	return c.JSON(fiber.Map{"message": "schedule updated", "data": row})
}

// Create godoc
// @Summary      Create worker schedule
// @Description  Creates an operator-flow schedule override. Accepts legacy target_table and can also resolve richer source/shadow scope against V2 metadata.
// @Tags         Operations
// @Accept       json
// @Produce      json
// @Param        body body WorkerScheduleCreateRequest true "Schedule create payload"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/worker-schedule [post]
func (h *ScheduleHandler) Create(c *fiber.Ctx) error {
	var req WorkerScheduleCreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if strings.TrimSpace(req.Operation) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "operation required"})
	}
	if req.IntervalMinutes <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "interval_minutes must be > 0"})
	}

	resolved, err := h.resolveTargetTable(c, req)
	if err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "schedule_scope_not_found"})
		case err.Error() == "ambiguous_scope":
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_schedule_scope"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
	}

	schedule := model.WorkerSchedule{
		Operation:       strings.TrimSpace(req.Operation),
		IntervalMinutes: req.IntervalMinutes,
		IsEnabled:       boolValue(req.IsEnabled, true),
		Notes:           req.Notes,
	}
	if resolved != nil {
		schedule.TargetTable = &resolved.TargetTable
	} else if trimmed(req.TargetTable) != nil {
		schedule.TargetTable = trimmed(req.TargetTable)
	}

	if err := h.db.WithContext(c.Context()).Create(&schedule).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	row, err := h.getResponseByID(c, schedule.ID)
	if err != nil {
		return c.Status(201).JSON(fiber.Map{"data": schedule})
	}
	return c.Status(201).JSON(fiber.Map{"data": row})
}
