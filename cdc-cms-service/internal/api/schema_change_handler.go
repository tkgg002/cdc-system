package api

import (
	"strconv"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/internal/service"

	"github.com/gofiber/fiber/v2"
)

type SchemaChangeHandler struct {
	pendingRepo   *repository.PendingFieldRepo
	schemaLogRepo *repository.SchemaLogRepo
	approvalSvc   *service.ApprovalService
}

func NewSchemaChangeHandler(
	pendingRepo *repository.PendingFieldRepo,
	schemaLogRepo *repository.SchemaLogRepo,
	approvalSvc *service.ApprovalService,
) *SchemaChangeHandler {
	return &SchemaChangeHandler{pendingRepo: pendingRepo, schemaLogRepo: schemaLogRepo, approvalSvc: approvalSvc}
}

// GetPending godoc
// @Summary      List pending schema changes
// @Description  Returns pending fields detected by Schema Inspector, with pagination and filters
// @Tags         Schema Changes
// @Accept       json
// @Produce      json
// @Param        status     query string false "Filter by status" default(pending) Enums(pending, approved, rejected)
// @Param        source_db  query string false "Filter by source database"
// @Param        table      query string false "Filter by table name"
// @Param        page       query int    false "Page number" default(1)
// @Param        page_size  query int    false "Page size"   default(20)
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/schema-changes/pending [get]
func (h *SchemaChangeHandler) GetPending(c *fiber.Ctx) error {
	status := c.Query("status", "pending")
	sourceDB := c.Query("source_db")
	tableName := c.Query("table")
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))

	var sourceDBPtr, tablePtr *string
	if sourceDB != "" {
		sourceDBPtr = &sourceDB
	}
	if tableName != "" {
		tablePtr = &tableName
	}

	fields, total, err := h.pendingRepo.GetByStatus(c.Context(), status, sourceDBPtr, tablePtr, page, pageSize)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch pending changes"})
	}

	return c.JSON(fiber.Map{"data": fields, "total": total, "page": page})
}

// Approve godoc
// @Summary      Approve a schema change
// @Description  Approves a pending field: ALTER TABLE ADD COLUMN, create mapping rule, publish config reload
// @Tags         Schema Changes
// @Accept       json
// @Produce      json
// @Param        id   path int                       true  "Pending field ID"
// @Param        body body service.ApproveRequest     true  "Approval details"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/schema-changes/{id}/approve [post]
func (h *SchemaChangeHandler) Approve(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	var req service.ApproveRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.TargetColumnName == "" || req.FinalType == "" {
		return c.Status(400).JSON(fiber.Map{"error": "target_column_name and final_type are required"})
	}

	username := middleware.GetUsername(c)
	pf, err := h.approvalSvc.Approve(c.Context(), uint(id), req, username)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "schema change approved", "data": pf})
}

// Reject godoc
// @Summary      Reject a schema change
// @Description  Rejects a pending field with a reason
// @Tags         Schema Changes
// @Accept       json
// @Produce      json
// @Param        id   path int                      true  "Pending field ID"
// @Param        body body service.RejectRequest     true  "Rejection reason"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/schema-changes/{id}/reject [post]
func (h *SchemaChangeHandler) Reject(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	var req service.RejectRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.RejectionReason == "" {
		return c.Status(400).JSON(fiber.Map{"error": "rejection_reason is required"})
	}

	username := middleware.GetUsername(c)
	pf, err := h.approvalSvc.Reject(c.Context(), uint(id), req, username)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "schema change rejected", "data": pf})
}

// GetHistory godoc
// @Summary      Get schema change history
// @Description  Returns audit log of all schema changes (ALTER TABLE, etc.)
// @Tags         Schema Changes
// @Produce      json
// @Param        table      query string false "Filter by table name"
// @Param        source_db  query string false "Filter by source database"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/schema-changes/history [get]
func (h *SchemaChangeHandler) GetHistory(c *fiber.Ctx) error {
	table := c.Query("table")
	sourceDB := c.Query("source_db")

	var tablePtr, sourceDBPtr *string
	if table != "" {
		tablePtr = &table
	}
	if sourceDB != "" {
		sourceDBPtr = &sourceDB
	}

	logs, err := h.schemaLogRepo.GetByTable(c.Context(), tablePtr, sourceDBPtr)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch history"})
	}

	return c.JSON(fiber.Map{"data": logs, "count": len(logs)})
}
