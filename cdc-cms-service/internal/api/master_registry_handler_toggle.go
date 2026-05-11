package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/infra/messaging"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

func (h *MasterRegistryHandler) ToggleActive(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	target, err := h.resolveMasterBindingByName(c, name)
	if err != nil {
		switch err.Error() {
		case "ambiguous_master_name":
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_name"})
		default:
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return c.Status(404).JSON(fiber.Map{"error": "not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := getActor(c)
	cmd := commands.ToggleMasterActiveCommand{
		MasterBindingID: uint64(target.ID),
		UpdatedBy:       user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	if _, err := h.bus.Execute(ctx, cmd); err != nil {
		switch {
		case errors.Is(err, commands.ErrMasterBindingNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrMasterRequiresApproved):
			return c.Status(409).JSON(fiber.Map{"error": "requires_approved", "detail": "cannot set is_active=true until schema_status='approved'"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	return c.JSON(fiber.Map{"status": "toggled", "master_name": name})
}
