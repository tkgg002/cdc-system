package api

import (
	"cdc-cms-service/internal/app/queries"
	"github.com/gofiber/fiber/v2"
	"strconv"
)

// LatestReport delegates the SQL to queries.ListLatestReportsHandler
// and runs the enrichment loop here.
func (h *ReconciliationHandler) LatestReport(c *fiber.Ctx) error {
	res, err := h.listLatestQ.Handle(c.UserContext(), queries.ListLatestReportsQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	rows := res.Data

	for i := range rows {
		errCode := ""
		if rows[i].ErrorCode != nil {
			errCode = *rows[i].ErrorCode
		}
		driftPct, computed, finalCode := queries.ComputeDriftStatus(rows[i].NullableSourceCount, rows[i].DestCount, errCode)
		rows[i].DriftPct = driftPct
		rows[i].ComputedStatus = computed
		if finalCode != "" {
			rows[i].ErrorMessageVI = queries.ErrorMessagesVI[finalCode]
			if rows[i].ErrorCode == nil {
				fc := finalCode
				rows[i].ErrorCode = &fc
			}
		}
		rows[i].SourceQueryMethod = queries.DeriveSourceQueryMethod(rows[i].TimestampField, rows[i].CheckType)
	}

	return c.JSON(fiber.Map{"data": rows, "total": len(rows)})
}

// TableHistory delegates the SQL+pagination to queries.GetTableHistoryHandler.
func (h *ReconciliationHandler) TableHistory(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	res, err := h.getHistoryQ.Handle(c.UserContext(), queries.GetTableHistoryQuery{
		Table:    c.Params("table"),
		Page:     page,
		PageSize: pageSize,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "total": res.Total, "page": res.Page})
}

// ListFailedLogs returns failed sync logs.
func (h *ReconciliationHandler) ListFailedLogs(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "30"))
	res, err := h.listFailedQ.Handle(c.UserContext(), queries.ListFailedLogsQuery{
		Filter: queries.FailedLogFilter{
			TargetTable: c.Query("target_table"),
			Status:      c.Query("status"),
			ErrorType:   c.Query("error_type"),
		},
		Page:     page,
		PageSize: pageSize,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "total": res.Total, "page": res.Page})
}
