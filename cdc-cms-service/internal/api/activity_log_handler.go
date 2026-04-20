package api

import (
	"strconv"

	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type ActivityLogHandler struct {
	db *gorm.DB
}

func NewActivityLogHandler(db *gorm.DB) *ActivityLogHandler {
	return &ActivityLogHandler{db: db}
}

// List returns paginated activity logs with optional filters
// GET /api/activity-log?operation=bridge&target_table=merchants&status=error&page=1&page_size=50
func (h *ActivityLogHandler) List(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	query := h.db.Model(&model.ActivityLog{}).Order("started_at DESC")

	if op := c.Query("operation"); op != "" {
		query = query.Where("operation = ?", op)
	}
	if table := c.Query("target_table"); table != "" {
		query = query.Where("target_table = ?", table)
	}
	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if triggeredBy := c.Query("triggered_by"); triggeredBy != "" {
		query = query.Where("triggered_by = ?", triggeredBy)
	}

	var total int64
	query.Count(&total)

	var logs []model.ActivityLog
	query.Offset((page - 1) * pageSize).Limit(pageSize).Find(&logs)

	return c.JSON(fiber.Map{
		"data":      logs,
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// Stats returns aggregated activity statistics
// GET /api/activity-log/stats
func (h *ActivityLogHandler) Stats(c *fiber.Ctx) error {
	type OpStat struct {
		Operation string `json:"operation"`
		Total     int64  `json:"total"`
		Success   int64  `json:"success"`
		Error     int64  `json:"error"`
		Skipped   int64  `json:"skipped"`
	}

	var stats []OpStat
	h.db.Raw(`
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
	`).Scan(&stats)

	var recentErrors []model.ActivityLog
	h.db.Where("status = 'error'").Order("started_at DESC").Limit(10).Find(&recentErrors)

	return c.JSON(fiber.Map{
		"stats_24h":     stats,
		"recent_errors": recentErrors,
	})
}
