package api

import (
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type HealthHandler struct {
	db *gorm.DB
}

func NewHealthHandler(db *gorm.DB) *HealthHandler {
	return &HealthHandler{db: db}
}

// Health godoc
// @Summary      Health check
// @Description  Returns service health status
// @Tags         Health
// @Produce      json
// @Success      200 {object} map[string]string
// @Router       /health [get]
func (h *HealthHandler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok", "service": "cdc-cms"})
}

// Ready godoc
// @Summary      Readiness check
// @Description  Returns service readiness (checks DB connection)
// @Tags         Health
// @Produce      json
// @Success      200 {object} map[string]string
// @Failure      503 {object} map[string]string
// @Router       /ready [get]
func (h *HealthHandler) Ready(c *fiber.Ctx) error {
	sqlDB, _ := h.db.DB()
	if err := sqlDB.Ping(); err != nil {
		return c.Status(503).JSON(fiber.Map{"status": "not ready", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"status": "ready"})
}
