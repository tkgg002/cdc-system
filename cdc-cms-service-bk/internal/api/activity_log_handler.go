package api

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type ActivityLogHandler struct {
	db *gorm.DB
}

func NewActivityLogHandler(db *gorm.DB) *ActivityLogHandler {
	return &ActivityLogHandler{db: db}
}

type ActivityLogRow struct {
	ID              uint64  `json:"id"`
	Operation       string  `json:"operation"`
	TargetTable     string  `json:"target_table"`
	SourceDatabase  *string `json:"source_database,omitempty"`
	SourceSchema    *string `json:"source_schema,omitempty"`
	SourceNamespace *string `json:"source_namespace,omitempty"`
	SourceTable     *string `json:"source_table,omitempty"`
	ShadowSchema    *string `json:"shadow_schema,omitempty"`
	ShadowTable     *string `json:"shadow_table,omitempty"`
	ScopeAmbiguous  bool    `json:"scope_ambiguous"`
	Status          string  `json:"status"`
	RowsAffected    int64   `json:"rows_affected"`
	DurationMs      *int    `json:"duration_ms"`
	Details         any     `json:"details"`
	ErrorMessage    *string `json:"error_message"`
	TriggeredBy     string  `json:"triggered_by"`
	StartedAt       string  `json:"started_at"`
	CompletedAt     *string `json:"completed_at"`
}

type OpStat struct {
	Operation string `json:"operation"`
	Total     int64  `json:"total"`
	Success   int64  `json:"success"`
	Error     int64  `json:"error"`
	Skipped   int64  `json:"skipped"`
}

func optQuery(c *fiber.Ctx, key string) *string {
	v := strings.TrimSpace(c.Query(key))
	if v == "" {
		return nil
	}
	return &v
}

func (h *ActivityLogHandler) baseActivityQuery() string {
	return `
		FROM cdc_activity_log al
		LEFT JOIN LATERAL (
			SELECT
				sb.source_object_id,
				sb.shadow_schema,
				sb.shadow_table
			FROM cdc_system.shadow_binding sb
			WHERE al.target_table IS NOT NULL
			  AND al.target_table <> '*'
			  AND sb.shadow_table = al.target_table
			  AND sb.is_active = TRUE
			ORDER BY sb.updated_at DESC, sb.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS binding_count
			FROM cdc_system.shadow_binding sb
			WHERE al.target_table IS NOT NULL
			  AND al.target_table <> '*'
			  AND sb.shadow_table = al.target_table
			  AND sb.is_active = TRUE
		) scope_counts ON TRUE
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		WHERE 1=1
	`
}

// List godoc
// @Summary      List activity logs
// @Description  Returns paginated activity logs enriched with source/shadow scope from V2 metadata. Supports source/shadow filters while keeping target_table as compatibility fallback.
// @Tags         Activity Log
// @Produce      json
// @Param        operation query string false "Operation filter"
// @Param        status query string false "Status filter"
// @Param        triggered_by query string false "Triggered by filter"
// @Param        target_table query string false "Legacy target_table filter"
// @Param        source_database query string false "Source database filter"
// @Param        source_table query string false "Source table filter"
// @Param        shadow_schema query string false "Shadow schema filter"
// @Param        shadow_table query string false "Shadow table filter"
// @Param        page query int false "Page number"
// @Param        page_size query int false "Page size"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/activity-log [get]
func (h *ActivityLogHandler) List(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	query := `
		SELECT
			al.id,
			al.operation,
			al.target_table,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table,
			COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous,
			al.status,
			al.rows_affected,
			al.duration_ms,
			al.details,
			al.error_message,
			al.triggered_by,
			TO_CHAR(al.started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
			CASE
				WHEN al.completed_at IS NULL THEN NULL
				ELSE TO_CHAR(al.completed_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')
			END AS completed_at
	` + h.baseActivityQuery()

	countQuery := `SELECT COUNT(*) ` + h.baseActivityQuery()
	args := make([]interface{}, 0, 8)
	countArgs := make([]interface{}, 0, 8)
	appendFilter := func(clause string, value string) {
		query += clause
		countQuery += clause
		args = append(args, value)
		countArgs = append(countArgs, value)
	}

	if op := optQuery(c, "operation"); op != nil {
		appendFilter(` AND al.operation = ?`, *op)
	}
	if table := optQuery(c, "target_table"); table != nil {
		appendFilter(` AND al.target_table = ?`, *table)
	}
	if status := optQuery(c, "status"); status != nil {
		appendFilter(` AND al.status = ?`, *status)
	}
	if triggeredBy := optQuery(c, "triggered_by"); triggeredBy != nil {
		appendFilter(` AND al.triggered_by = ?`, *triggeredBy)
	}
	if sourceDatabase := optQuery(c, "source_database"); sourceDatabase != nil {
		appendFilter(` AND so.source_database = ?`, *sourceDatabase)
	}
	if sourceTable := optQuery(c, "source_table"); sourceTable != nil {
		appendFilter(` AND so.source_object_name = ?`, *sourceTable)
	}
	if shadowSchema := optQuery(c, "shadow_schema"); shadowSchema != nil {
		appendFilter(` AND sb.shadow_schema = ?`, *shadowSchema)
	}
	if shadowTable := optQuery(c, "shadow_table"); shadowTable != nil {
		appendFilter(` AND sb.shadow_table = ?`, *shadowTable)
	}

	var total int64
	if err := h.db.Raw(countQuery, countArgs...).Scan(&total).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	query += ` ORDER BY al.started_at DESC OFFSET ? LIMIT ?`
	args = append(args, (page-1)*pageSize, pageSize)

	var logs []ActivityLogRow
	if err := h.db.Raw(query, args...).Scan(&logs).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"data":      logs,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// Stats godoc
// @Summary      Get activity log stats
// @Description  Returns 24h aggregated activity stats and recent errors enriched with V2 source/shadow scope.
// @Tags         Activity Log
// @Produce      json
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/activity-log/stats [get]
func (h *ActivityLogHandler) Stats(c *fiber.Ctx) error {
	var stats []OpStat
	if err := h.db.Raw(`
		SELECT
			operation,
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'success') as success,
			COUNT(*) FILTER (WHERE status = 'error') as error,
			COUNT(*) FILTER (WHERE status = 'skipped') as skipped
		FROM cdc_activity_log
		WHERE started_at > NOW() - INTERVAL '24 hours'
		GROUP BY operation
		ORDER BY total DESC
	`).Scan(&stats).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	var recentErrors []ActivityLogRow
	query := `
		SELECT
			al.id,
			al.operation,
			al.target_table,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table,
			COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous,
			al.status,
			al.rows_affected,
			al.duration_ms,
			al.details,
			al.error_message,
			al.triggered_by,
			TO_CHAR(al.started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
			CASE
				WHEN al.completed_at IS NULL THEN NULL
				ELSE TO_CHAR(al.completed_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')
			END AS completed_at
	` + h.baseActivityQuery() + `
		AND al.status = 'error'
		ORDER BY al.started_at DESC
		LIMIT 10
	`
	if err := h.db.Raw(query).Scan(&recentErrors).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"stats_24h":     stats,
		"recent_errors": recentErrors,
	})
}
