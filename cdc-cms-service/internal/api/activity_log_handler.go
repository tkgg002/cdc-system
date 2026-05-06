package api

import (
	"strconv"
	"strings"

	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
)

// ActivityLogHandler is the HTTP edge for /api/activity-log. SQL +
// LATERAL joins moved to `internal/infra/persistence/activity_log_
// read_repo_gorm.go`; orchestration moved to `internal/app/queries/
// list_activity_logs.go` + `get_activity_stats.go`. The handler now
// only translates fiber.Ctx → query, then projects the result to the
// same wire shape the legacy handler used.
type ActivityLogHandler struct {
	listQ  *queries.ListActivityLogsHandler
	statsQ *queries.GetActivityStatsHandler
}

func NewActivityLogHandler(
	listQ *queries.ListActivityLogsHandler,
	statsQ *queries.GetActivityStatsHandler,
) *ActivityLogHandler {
	return &ActivityLogHandler{listQ: listQ, statsQ: statsQ}
}

// ActivityLogRow / OpStat re-exported from queries via type alias so
// the Swagger comments + any external callers keep compiling.
type ActivityLogRow = queries.ActivityLogRow
type OpStat = queries.OpStat

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

	res, err := h.listQ.Handle(c.UserContext(), queries.ListActivityLogsQuery{
		Filter: queries.ActivityLogFilter{
			Operation:      strings.TrimSpace(c.Query("operation")),
			Status:         strings.TrimSpace(c.Query("status")),
			TriggeredBy:    strings.TrimSpace(c.Query("triggered_by")),
			TargetTable:    strings.TrimSpace(c.Query("target_table")),
			SourceDatabase: strings.TrimSpace(c.Query("source_database")),
			SourceTable:    strings.TrimSpace(c.Query("source_table")),
			ShadowSchema:   strings.TrimSpace(c.Query("shadow_schema")),
			ShadowTable:    strings.TrimSpace(c.Query("shadow_table")),
		},
		Page:     page,
		PageSize: pageSize,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"data":      res.Data,
		"total":     res.Total,
		"page":      res.Page,
		"page_size": res.PageSize,
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
	res, err := h.statsQ.Handle(c.UserContext(), queries.GetActivityStatsQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"stats_24h":     res.Stats24h,
		"recent_errors": res.RecentErrors,
	})
}
