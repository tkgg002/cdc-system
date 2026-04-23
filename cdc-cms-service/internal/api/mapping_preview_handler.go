package api

import (
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// MappingPreviewHandler — §Dashboard.1. POST /api/v1/mapping-rules/preview
// lets the FE JsonPath editor validate a rule against live shadow samples
// BEFORE the admin saves it. No writes.
type MappingPreviewHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewMappingPreviewHandler(db *gorm.DB, logger *zap.Logger) *MappingPreviewHandler {
	return &MappingPreviewHandler{db: db, logger: logger}
}

// Preview — POST /api/v1/mapping-rules/preview
func (h *MappingPreviewHandler) Preview(c *fiber.Ctx) error {
	var req PreviewRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json"})
	}
	if !propIdentRe.MatchString(req.ShadowTable) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_shadow_table"})
	}
	if strings.TrimSpace(req.JSONPath) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "jsonpath_required"})
	}
	limit := req.SampleLimit
	if limit <= 0 || limit > 10 {
		limit = 3
	}

	// Fetch shadow sample rows — _raw_data + _gpay_source_id.
	var rows []struct {
		GpayID   int64  `gorm:"column:_gpay_id"`
		SourceID string `gorm:"column:_gpay_source_id"`
		RawData  []byte `gorm:"column:_raw_data"`
	}
	q := `SELECT _gpay_id, _gpay_source_id, _raw_data FROM cdc_internal.` + `"` + req.ShadowTable + `"` + ` ORDER BY _synced_at DESC LIMIT ?`
	if err := h.db.WithContext(c.Context()).Raw(q, limit).Scan(&rows).Error; err != nil {
		h.logger.Error("preview: shadow read failed",
			zap.String("table", req.ShadowTable), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "shadow_read_failed", "detail": err.Error()})
	}
	if len(rows) == 0 {
		return c.Status(200).JSON(fiber.Map{
			"data":    []PreviewResult{},
			"message": "shadow table is empty — no samples to preview",
		})
	}

	// Evaluate JsonPath via gjson — same engine the Transmuter uses.
	out := make([]PreviewResult, 0, len(rows))
	for _, r := range rows {
		pr := PreviewResult{SourceID: r.SourceID, RawAfter: r.RawData}
		res := gjson.GetBytes(r.RawData, req.JSONPath)
		if !res.Exists() {
			pr.Violation = "path_not_found"
			out = append(out, pr)
			continue
		}
		switch res.Type {
		case gjson.Null:
			pr.Extracted = nil
		case gjson.False:
			pr.Extracted = false
		case gjson.True:
			pr.Extracted = true
		case gjson.Number:
			if strings.ContainsAny(res.Raw, ".eE") {
				pr.Extracted = res.Float()
			} else {
				pr.Extracted = res.Int()
			}
		case gjson.String:
			pr.Extracted = res.String()
		case gjson.JSON:
			pr.Extracted = res.Raw
		}
		out = append(out, pr)
	}
	return c.JSON(fiber.Map{"data": out, "count": len(out)})
}
