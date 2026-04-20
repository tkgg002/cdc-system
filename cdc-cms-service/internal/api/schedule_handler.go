package api

import (
	"encoding/json"
	"time"

	"cdc-cms-service/internal/model"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type ScheduleHandler struct {
	db *gorm.DB
}

func NewScheduleHandler(db *gorm.DB) *ScheduleHandler {
	return &ScheduleHandler{db: db}
}

// List returns all worker schedules
func (h *ScheduleHandler) List(c *fiber.Ctx) error {
	var schedules []model.WorkerSchedule
	h.db.Order("operation, target_table").Find(&schedules)
	return c.JSON(fiber.Map{"data": schedules})
}

// Update modifies a schedule (interval, enabled, etc.)
func (h *ScheduleHandler) Update(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	var body struct {
		IntervalMinutes *int    `json:"interval_minutes"`
		IsEnabled       *bool   `json:"is_enabled"`
		Notes           *string `json:"notes"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	updates := map[string]interface{}{}
	if body.IntervalMinutes != nil {
		updates["interval_minutes"] = *body.IntervalMinutes
	}
	if body.IsEnabled != nil {
		updates["is_enabled"] = *body.IsEnabled
	}
	if body.Notes != nil {
		updates["notes"] = *body.Notes
	}

	if len(updates) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "nothing to update"})
	}

	updates["updated_at"] = time.Now()
	if err := h.db.Model(&model.WorkerSchedule{}).Where("id = ?", id).Updates(updates).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Log activity
	detailsJSON, _ := json.Marshal(updates)
	now := time.Now()
	h.db.Create(&model.ActivityLog{
		Operation:   "schedule-update",
		TargetTable: "cdc_worker_schedule",
		Status:      "success",
		Details:     detailsJSON,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

	return c.JSON(fiber.Map{"message": "schedule updated"})
}

// Create adds a new per-table schedule override
func (h *ScheduleHandler) Create(c *fiber.Ctx) error {
	var schedule model.WorkerSchedule
	if err := c.BodyParser(&schedule); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if schedule.Operation == "" {
		return c.Status(400).JSON(fiber.Map{"error": "operation required"})
	}

	if err := h.db.Create(&schedule).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(201).JSON(fiber.Map{"data": schedule})
}
