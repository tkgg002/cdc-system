package api

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// JobHandler — read-only surface for the CommandBus job tracker.
// Phase 2 v2 / P3. Writes (status transitions) are owned by the worker
// JobMonitor and the CommandBus itself; this handler only projects.
type JobHandler struct {
	getQ *queries.GetJobHandler
}

func NewJobHandler(getQ *queries.GetJobHandler) *JobHandler {
	return &JobHandler{getQ: getQ}
}

// Get godoc
// @Summary      Get async job
// @Description  Returns the cdc_system.cdc_jobs row written by the CommandBus on Dispatch and updated by the worker JobMonitor on cdc.evt.X.completed.
// @Tags         Jobs
// @Produce      json
// @Param        id   path string true "Job UUID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/jobs/{id} [get]
func (h *JobHandler) Get(c *fiber.Ctx) error {
	id := strings.TrimSpace(c.Params("id"))
	if id == "" {
		return c.Status(400).JSON(fiber.Map{"error": "id required"})
	}
	res, err := h.getQ.Handle(c.UserContext(), queries.GetJobQuery{ID: id})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return c.Status(404).JSON(fiber.Map{"error": "job_not_found"})
		}
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Job})
}
