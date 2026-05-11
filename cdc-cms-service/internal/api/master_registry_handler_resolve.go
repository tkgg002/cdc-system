package api

import (
	"strings"

	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type masterBindingTarget struct {
	ID          int64
	MasterTable string
}

func (h *MasterRegistryHandler) resolveMasterBindingByName(ctx *fiber.Ctx, masterName string) (*masterBindingTarget, error) {
	query := `
		SELECT id, master_table 
		FROM cdc_system.master_table_registry 
		WHERE master_table = ?
	`
	var rows []masterBindingTarget
	if err := h.db.WithContext(ctx.Context()).Raw(query, masterName).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, fiber.NewError(409, "ambiguous_master_name")
	}
	return &rows[0], nil
}

func trimString(v string) string {
	return strings.TrimSpace(v)
}

func getActor(c *fiber.Ctx) string {
	if u := middleware.GetUsername(c); u != "" {
		return u
	}
	return "system"
}

func trimCreateRequest(req *CreateRequest) {
	req.MasterName = strings.TrimSpace(req.MasterName)
	req.MasterSchema = strings.TrimSpace(req.MasterSchema)
	req.MasterConnectionCode = strings.TrimSpace(req.MasterConnectionCode)
	req.SourceShadow = strings.TrimSpace(req.SourceShadow)
	req.SourceDatabase = strings.TrimSpace(req.SourceDatabase)
	req.SourceSchema = strings.TrimSpace(req.SourceSchema)
	req.SourceNamespace = strings.TrimSpace(req.SourceNamespace)
	req.SourceTable = strings.TrimSpace(req.SourceTable)
	req.ShadowSchema = strings.TrimSpace(req.ShadowSchema)
	req.ShadowTable = strings.TrimSpace(req.ShadowTable)
	req.TransformType = strings.TrimSpace(req.TransformType)
}
