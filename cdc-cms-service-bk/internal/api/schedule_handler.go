package api

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type ScheduleHandler struct {
	db *gorm.DB
}

func NewScheduleHandler(db *gorm.DB) *ScheduleHandler {
	return &ScheduleHandler{db: db}
}

type WorkerScheduleScope struct {
	SourceObjectID   *int64  `json:"source_object_id,omitempty"`
	SourceDatabase   *string `json:"source_database,omitempty"`
	SourceSchema     *string `json:"source_schema,omitempty"`
	SourceNamespace  *string `json:"source_namespace,omitempty"`
	SourceTable      *string `json:"source_table,omitempty"`
	ShadowBindingID  *int64  `json:"shadow_binding_id,omitempty"`
	ShadowSchema     *string `json:"shadow_schema,omitempty"`
	ShadowTable      *string `json:"shadow_table,omitempty"`
	PhysicalTableFQN *string `json:"physical_table_fqn,omitempty"`
	ScopeAmbiguous   bool    `json:"scope_ambiguous"`
}

type WorkerScheduleResponse struct {
	ID              uint                `json:"id"`
	Operation       string              `json:"operation"`
	TargetTable     *string             `json:"target_table"`
	IntervalMinutes int                 `json:"interval_minutes"`
	IsEnabled       bool                `json:"is_enabled"`
	LastRunAt       *time.Time          `json:"last_run_at"`
	NextRunAt       *time.Time          `json:"next_run_at"`
	RunCount        int64               `json:"run_count"`
	LastError       *string             `json:"last_error"`
	Notes           *string             `json:"notes"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	Scope           WorkerScheduleScope `json:"scope"`
}

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

type workerScheduleScanRow struct {
	ID               uint       `gorm:"column:id"`
	Operation        string     `gorm:"column:operation"`
	TargetTable      *string    `gorm:"column:target_table"`
	IntervalMinutes  int        `gorm:"column:interval_minutes"`
	IsEnabled        bool       `gorm:"column:is_enabled"`
	LastRunAt        *time.Time `gorm:"column:last_run_at"`
	NextRunAt        *time.Time `gorm:"column:next_run_at"`
	RunCount         int64      `gorm:"column:run_count"`
	LastError        *string    `gorm:"column:last_error"`
	Notes            *string    `gorm:"column:notes"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
	UpdatedAt        time.Time  `gorm:"column:updated_at"`
	SourceObjectID   *int64     `gorm:"column:source_object_id"`
	SourceDatabase   *string    `gorm:"column:source_database"`
	SourceSchema     *string    `gorm:"column:source_schema"`
	SourceNamespace  *string    `gorm:"column:source_namespace"`
	SourceTable      *string    `gorm:"column:source_table"`
	ShadowBindingID  *int64     `gorm:"column:shadow_binding_id"`
	ShadowSchema     *string    `gorm:"column:shadow_schema"`
	ShadowTable      *string    `gorm:"column:shadow_table"`
	PhysicalTableFQN *string    `gorm:"column:physical_table_fqn"`
	ScopeAmbiguous   bool       `gorm:"column:scope_ambiguous"`
}

func (h *ScheduleHandler) listResponses(ctx *fiber.Ctx) ([]WorkerScheduleResponse, error) {
	var scan []workerScheduleScanRow
	err := h.db.WithContext(ctx.Context()).Raw(`
		SELECT
			ws.id,
			ws.operation,
			ws.target_table,
			ws.interval_minutes,
			ws.is_enabled,
			ws.last_run_at,
			ws.next_run_at,
			ws.run_count,
			ws.last_error,
			ws.notes,
			ws.created_at,
			ws.updated_at,
			sb.source_object_id,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_binding_id,
			sb.shadow_schema,
			sb.shadow_table,
			sb.physical_table_fqn,
			COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
		FROM cdc_system.cdc_worker_schedule ws
		LEFT JOIN LATERAL (
			SELECT
				s.id AS shadow_binding_id,
				s.source_object_id,
				s.shadow_schema,
				s.shadow_table,
				s.physical_table_fqn
			FROM cdc_system.shadow_binding s
			WHERE ws.target_table IS NOT NULL
			  AND s.shadow_table = ws.target_table
			  AND s.is_active = TRUE
			ORDER BY s.updated_at DESC, s.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS binding_count
			FROM cdc_system.shadow_binding s
			WHERE ws.target_table IS NOT NULL
			  AND s.shadow_table = ws.target_table
			  AND s.is_active = TRUE
		) scope_counts ON TRUE
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		ORDER BY ws.operation, ws.target_table NULLS FIRST, ws.id
	`).Scan(&scan).Error
	if err != nil {
		return nil, err
	}
	rows := make([]WorkerScheduleResponse, 0, len(scan))
	for _, s := range scan {
		rows = append(rows, WorkerScheduleResponse{
			ID:              s.ID,
			Operation:       s.Operation,
			TargetTable:     s.TargetTable,
			IntervalMinutes: s.IntervalMinutes,
			IsEnabled:       s.IsEnabled,
			LastRunAt:       s.LastRunAt,
			NextRunAt:       s.NextRunAt,
			RunCount:        s.RunCount,
			LastError:       s.LastError,
			Notes:           s.Notes,
			CreatedAt:       s.CreatedAt,
			UpdatedAt:       s.UpdatedAt,
			Scope: WorkerScheduleScope{
				SourceObjectID:   s.SourceObjectID,
				SourceDatabase:   s.SourceDatabase,
				SourceSchema:     s.SourceSchema,
				SourceNamespace:  s.SourceNamespace,
				SourceTable:      s.SourceTable,
				ShadowBindingID:  s.ShadowBindingID,
				ShadowSchema:     s.ShadowSchema,
				ShadowTable:      s.ShadowTable,
				PhysicalTableFQN: s.PhysicalTableFQN,
				ScopeAmbiguous:   s.ScopeAmbiguous,
			},
		})
	}
	return rows, nil
}

func (h *ScheduleHandler) getResponseByID(ctx *fiber.Ctx, id uint) (*WorkerScheduleResponse, error) {
	rows, err := h.listResponses(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ID == id {
			return &rows[i], nil
		}
	}
	return nil, gorm.ErrRecordNotFound
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
	rows, err := h.listResponses(c)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": rows})
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

	updates := map[string]interface{}{}
	if body.IntervalMinutes != nil {
		updates["interval_minutes"] = *body.IntervalMinutes
	}
	if body.IsEnabled != nil {
		updates["is_enabled"] = *body.IsEnabled
	}
	if body.Notes != nil {
		updates["notes"] = *body.Notes
	}

	if len(updates) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "nothing to update"})
	}

	var existing model.WorkerSchedule
	if err := h.db.WithContext(c.Context()).First(&existing, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(404).JSON(fiber.Map{"error": "schedule_not_found"})
		}
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	updates["updated_at"] = time.Now()
	if err := h.db.WithContext(c.Context()).Model(&model.WorkerSchedule{}).Where("id = ?", id).Updates(updates).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	detailsJSON, _ := json.Marshal(updates)
	now := time.Now()
	logTarget := "all_schedules"
	if existing.TargetTable != nil && strings.TrimSpace(*existing.TargetTable) != "" {
		logTarget = *existing.TargetTable
	}
	h.db.Create(&model.ActivityLog{
		Operation:   "schedule-update",
		TargetTable: logTarget,
		Status:      "success",
		Details:     detailsJSON,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

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
