package api

import (
	"strconv"
	"strings"

	"cdc-cms-service/internal/api/dto"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/domain/mapping"
	"github.com/gofiber/fiber/v2"
)

// List godoc
// @Summary      List mapping rules
// @Description  Returns V2 mapping rules, optionally filtered by source object or source/shadow scope. Legacy table/source_table params are still accepted as fallback.
// @Tags         Mapping Rules
// @Produce      json
// @Param        source_object_id query int false "Filter by source object ID"
// @Param        source_database query string false "Filter by source database"
// @Param        source_table query string false "Filter by source object name"
// @Param        shadow_schema query string false "Filter by shadow schema"
// @Param        shadow_table query string false "Filter by shadow table"
// @Param        status query string false "Filter by status"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [get]
func (h *MappingRuleHandler) List(c *fiber.Ctx) error {
	filterSourceTable := strings.TrimSpace(c.Query("source_table", c.Query("table", c.Query("table_name"))))
	sourceObjectID, _ := strconv.ParseInt(c.Query("source_object_id"), 10, 64)
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))

	q := queries.ListMappingRulesQuery{
		Filter: mapping.Filter{
			Status:         mapping.Status(strings.TrimSpace(c.Query("status"))),
			RuleType:       mapping.RuleType(strings.TrimSpace(c.Query("rule_type"))),
			SourceObjectID: sourceObjectID,
			SourceDatabase: strings.TrimSpace(c.Query("source_database")),
			SourceTable:    filterSourceTable,
			ShadowSchema:   strings.TrimSpace(c.Query("shadow_schema")),
			ShadowTable:    strings.TrimSpace(c.Query("shadow_table")),
		},
		Page:     page,
		PageSize: pageSize,
	}

	res, err := h.listQuery.Handle(c.Context(), q)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch mapping rules: " + err.Error()})
	}

	rows := make([]dto.MappingRuleRow, len(res.Data))
	for i, r := range res.Data {
		rows[i] = dto.RuleToRow(r)
	}
	return c.JSON(fiber.Map{"data": rows, "count": len(rows), "total": res.Total, "page": res.Page, "page_size": res.PageSize})
}
