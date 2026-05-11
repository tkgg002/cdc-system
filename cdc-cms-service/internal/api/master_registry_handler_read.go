package api

import (
	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// MasterRow is the wire shape of one row in /api/v1/masters. Aliased
// to the Q-side read model so server-side callers + Swagger see the
// same type.
type MasterRow = queries.MasterListItem

// List godoc
// @Summary      List master bindings
// @Description  Returns V2 master bindings enriched with source and shadow metadata from cdc_system.
// @Tags         Masters
// @Produce      json
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters [get]
func (h *MasterRegistryHandler) List(c *fiber.Ctx) error {
	res, err := h.listQ.Handle(c.Context(), queries.ListMastersQuery{})
	if err != nil {
		h.logger.Error("master list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
}
