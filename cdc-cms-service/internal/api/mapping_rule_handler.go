package api

import (
	"encoding/json"
	"strconv"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type MappingRuleHandler struct {
	repo         *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	natsClient   *natsconn.NatsClient
	db           *gorm.DB
}

func NewMappingRuleHandler(repo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, nats *natsconn.NatsClient, db ...*gorm.DB) *MappingRuleHandler {
	h := &MappingRuleHandler{repo: repo, registryRepo: registryRepo, natsClient: nats}
	if len(db) > 0 {
		h.db = db[0]
	}
	return h
}

// List godoc
// @Summary      List mapping rules
// @Description  Returns all mapping rules, optionally filtered by table
// @Tags         Mapping Rules
// @Produce      json
// @Param        table query string false "Filter by source table"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [get]
func (h *MappingRuleHandler) List(c *fiber.Ctx) error {
	table := c.Query("table")
	status := c.Query("status")
	ruleType := c.Query("rule_type")
	sourceTable := c.Query("source_table")
	tableName := c.Query("table_name")

	// Support multiple param names for table filter
	filterTable := table
	if filterTable == "" {
		filterTable = sourceTable
	}
	if filterTable == "" {
		filterTable = tableName
	}

	var tablePtr, statusPtr, ruleTypePtr *string
	if filterTable != "" {
		tablePtr = &filterTable
	}
	if status != "" {
		statusPtr = &status
	}
	if ruleType != "" {
		ruleTypePtr = &ruleType
	}

	// Pagination
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	rules, total, err := h.repo.GetAllFilteredPaginated(c.Context(), tablePtr, statusPtr, ruleTypePtr, page, pageSize)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch mapping rules: " + err.Error()})
	}

	return c.JSON(fiber.Map{
		"data":      rules,
		"count":     len(rules),
		"total":     total,
		"page":      page,
		"page_size": pageSize,
	})
}

// Create godoc
// @Summary      Create a mapping rule
// @Description  Creates a new field mapping rule and publishes config reload
// @Tags         Mapping Rules
// @Accept       json
// @Produce      json
// @Param        body body model.MappingRule true "Mapping rule details"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [post]
func (h *MappingRuleHandler) Create(c *fiber.Ctx) error {
	var rule model.MappingRule
	if err := c.BodyParser(&rule); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	username := middleware.GetUsername(c)
	rule.CreatedBy = &username
	rule.UpdatedBy = &username

	if err := h.repo.Create(c.Context(), &rule); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to create mapping rule: " + err.Error()})
	}

	return c.Status(201).JSON(fiber.Map{"message": "mapping rule created", "data": rule})
}

// Reload godoc
// @Summary      Reload mapping rules for workers
// @Description  Publishes a NATS message to trigger workers to reload mapping rules from DB
// @Tags         Mapping Rules
// @Produce      json
// @Param        table query string false "Specific table to reload"
// @Success      200 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules/reload [post]
func (h *MappingRuleHandler) Reload(c *fiber.Ctx) error {
	table := c.Query("table")
	if err := h.natsClient.PublishReload(table, middleware.GetUsername(c), "reload_mapping", ""); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish reload event"})
	}

	return c.JSON(fiber.Map{"message": "reload signal sent successfully"})
}

// UpdateStatus godoc
// @Summary      Update mapping rule status
// @Description  Updates the status of a mapping rule (approve/reject)
// @Tags         Mapping Rules
// @Accept       json
// @Produce      json
// @Param        id   path int true "Mapping Rule ID"
// @Param        body body object true "Status update"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules/{id} [patch]
func (h *MappingRuleHandler) UpdateStatus(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	var body struct {
		Status string `json:"status"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if body.Status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "status is required"})
	}

	// Verify rule exists
	rule, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "mapping rule not found"})
	}

	username := middleware.GetUsername(c)
	if err := h.repo.UpdateStatus(c.Context(), uint(id), body.Status, username); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to update mapping rule: " + err.Error()})
	}

	// Publish reload signal
	h.natsClient.PublishReload(rule.SourceTable, middleware.GetUsername(c), "mapping_status_update", "")

	return c.JSON(fiber.Map{"message": "mapping rule updated", "id": id, "status": body.Status})
}

// Backfill godoc
// @Summary      Backfill data for a mapping rule
// @Description  Populates the target column with values from _raw_data for existing rows where target column is NULL
// @Tags         Mapping Rules
// @Produce      json
// @Param        id   path int true "Mapping Rule ID"
// @Success      200 {object} map[string]interface{}
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules/{id}/backfill [post]
func (h *MappingRuleHandler) Backfill(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	rule, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "mapping rule not found"})
	}

	// 1. Get registry entry for the source table
	reg, err := h.registryRepo.GetBySourceTable(c.Context(), rule.SourceTable)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry not found for table: " + rule.SourceTable})
	}

	// 2. Perform backfill asynchronously via NATS
	payload, _ := json.Marshal(map[string]interface{}{
		"target_table":  reg.TargetTable,
		"source_field":  rule.SourceField,
		"target_column": rule.TargetColumn,
		"data_type":     rule.DataType,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.backfill", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch backfill command: " + err.Error()})
	}

	return c.Status(202).JSON(fiber.Map{
		"message":       "backfill command accepted",
		"target_table":  reg.TargetTable,
		"source_field":  rule.SourceField,
		"target_column": rule.TargetColumn,
	})
}

// BatchUpdate updates status for multiple mapping rules at once + optional auto-backfill
// Rule B: status update = config-write sync trong CMS. ALTER TABLE là DW mutate
// → dispatch NATS cho Worker (cdc.cmd.alter-column per rule).
func (h *MappingRuleHandler) BatchUpdate(c *fiber.Ctx) error {
	var body struct {
		IDs          []uint `json:"ids"`
		Status       string `json:"status"`
		AutoBackfill bool   `json:"auto_backfill"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if len(body.IDs) == 0 || body.Status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ids and status required"})
	}

	username := middleware.GetUsername(c)
	updated := 0
	dispatched := 0
	backfilled := 0

	for _, id := range body.IDs {
		if err := h.repo.UpdateStatus(c.Context(), id, body.Status, username); err != nil {
			continue
		}
		updated++

		// Khi approve: dispatch ALTER TABLE async thay vì chạy đồng bộ.
		if body.Status == "approved" {
			rule, err := h.repo.GetByID(c.Context(), id)
			if err != nil {
				continue
			}
			reg, err := h.registryRepo.GetBySourceTable(c.Context(), rule.SourceTable)
			if err != nil {
				continue
			}

			alterPayload, _ := json.Marshal(map[string]interface{}{
				"registry_id":    reg.ID,
				"target_table":   reg.TargetTable,
				"source_table":   rule.SourceTable,
				"source_field":   rule.SourceField,
				"target_column":  rule.TargetColumn,
				"data_type":      rule.DataType,
				"mapping_rule_id": rule.ID,
			})
			if err := h.natsClient.Conn.Publish("cdc.cmd.alter-column", alterPayload); err == nil {
				dispatched++
			}

			if body.AutoBackfill {
				payload, _ := json.Marshal(map[string]interface{}{
					"target_table":  reg.TargetTable,
					"source_field":  rule.SourceField,
					"target_column": rule.TargetColumn,
					"data_type":     rule.DataType,
				})
				if err := h.natsClient.Conn.Publish("cdc.cmd.backfill", payload); err == nil {
					backfilled++
				}
			}
		}
	}

	h.natsClient.PublishReload("*", username, "batch_update", "")

	return c.Status(202).JSON(fiber.Map{
		"message":    "batch update accepted — alter-column dispatched per approved rule",
		"updated":    updated,
		"dispatched": dispatched,
		"backfilled": backfilled,
		"total":      len(body.IDs),
	})
}
