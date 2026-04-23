package api

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// MasterRegistryHandler — Sprint 5 §R8 admin plane for master tables.
// Mounts under /api/v1/masters/*. Write ops go through destructive chain;
// read ops are shared (admin|operator).
type MasterRegistryHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
}

func NewMasterRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *MasterRegistryHandler {
	return &MasterRegistryHandler{db: db, nats: nats, logger: logger}
}

var masterNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// MasterRow is the API-facing projection.
type MasterRow struct {
	ID               int64           `json:"id"`
	MasterName       string          `json:"master_name"`
	SourceShadow     string          `json:"source_shadow"`
	TransformType    string          `json:"transform_type"`
	Spec             json.RawMessage `json:"spec"`
	IsActive         bool            `json:"is_active"`
	SchemaStatus     string          `json:"schema_status"`
	SchemaReviewedBy *string         `json:"schema_reviewed_by,omitempty"`
	SchemaReviewedAt *time.Time      `json:"schema_reviewed_at,omitempty"`
	RejectionReason  *string         `json:"rejection_reason,omitempty"`
	CreatedBy        *string         `json:"created_by,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// List — GET /api/v1/masters
func (h *MasterRegistryHandler) List(c *fiber.Ctx) error {
	var rows []MasterRow
	err := h.db.WithContext(c.Context()).Raw(
		`SELECT id, master_name, source_shadow, transform_type, spec,
		        is_active, schema_status, schema_reviewed_by, schema_reviewed_at,
		        rejection_reason, created_by, created_at, updated_at
		   FROM cdc_internal.master_table_registry
		  ORDER BY source_shadow, master_name`,
	).Scan(&rows).Error
	if err != nil {
		h.logger.Error("master list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": rows, "count": len(rows)})
}

// CreateRequest — POST /api/v1/masters body.
type CreateRequest struct {
	MasterName    string          `json:"master_name"`
	SourceShadow  string          `json:"source_shadow"`
	TransformType string          `json:"transform_type"`
	Spec          json.RawMessage `json:"spec"`
	Reason        string          `json:"reason"`
}

// Create — POST /api/v1/masters (destructive chain).
func (h *MasterRegistryHandler) Create(c *fiber.Ctx) error {
	var req CreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}
	if !masterNameRe.MatchString(req.MasterName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	if !masterNameRe.MatchString(req.SourceShadow) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_shadow"})
	}
	validType := map[string]bool{
		"copy_1_to_1": true, "filter": true, "aggregate": true,
		"group_by": true, "join": true,
	}
	if !validType[req.TransformType] {
		return c.Status(400).JSON(fiber.Map{
			"error":  "invalid_transform_type",
			"detail": "one of copy_1_to_1|filter|aggregate|group_by|join",
		})
	}
	if len(req.Spec) == 0 {
		req.Spec = json.RawMessage("{}")
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)

	err := h.db.WithContext(c.Context()).Exec(
		`INSERT INTO cdc_internal.master_table_registry
		   (master_name, source_shadow, transform_type, spec, is_active,
		    schema_status, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?::jsonb, false, 'pending_review', ?, NOW(), NOW())`,
		req.MasterName, req.SourceShadow, req.TransformType, string(req.Spec), actor,
	).Error
	if err != nil {
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return c.Status(409).JSON(fiber.Map{"error": "master_already_exists", "master_name": req.MasterName})
		}
		h.logger.Error("master create failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}

	h.logger.Info("master created",
		zap.String("master", req.MasterName),
		zap.String("source_shadow", req.SourceShadow),
		zap.String("actor", actor))

	return c.Status(201).JSON(fiber.Map{
		"master_name":   req.MasterName,
		"schema_status": "pending_review",
		"next":          "POST /api/v1/masters/" + req.MasterName + "/approve",
	})
}

// ApproveRequest — body for approve/reject.
type ApproveRequest struct {
	Reason string `json:"reason"`
}

// Approve — POST /api/v1/masters/:name/approve (destructive chain).
// Flips schema_status=approved and dispatches cdc.cmd.master-create to
// the worker, which runs MasterDDLGenerator.Apply.
func (h *MasterRegistryHandler) Approve(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)

	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_internal.master_table_registry
		    SET schema_status = 'approved',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = NULL,
		        updated_at = NOW()
		  WHERE master_name = ?
		    AND schema_status IN ('pending_review','rejected','failed')`,
		actor, name,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(409).JSON(fiber.Map{
			"error":  "not_approvable",
			"detail": "master not found OR already approved",
		})
	}

	// Dispatch DDL creation to worker.
	payload, _ := json.Marshal(map[string]string{
		"master_table":   name,
		"triggered_by":   actor,
		"correlation_id": "approve-" + name + "-" + time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err := h.nats.Conn.Publish("cdc.cmd.master-create", payload); err != nil {
		h.logger.Warn("master-create publish failed", zap.String("master", name), zap.Error(err))
		return c.Status(202).JSON(fiber.Map{
			"status":       "approved_but_dispatch_failed",
			"master_name":  name,
			"dispatch_err": err.Error(),
		})
	}

	h.logger.Info("master approved + dispatched",
		zap.String("master", name), zap.String("actor", actor))

	return c.Status(202).JSON(fiber.Map{
		"status":      "approved",
		"master_name": name,
		"dispatched":  "cdc.cmd.master-create",
	})
}

// Reject — POST /api/v1/masters/:name/reject (destructive).
func (h *MasterRegistryHandler) Reject(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)

	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_internal.master_table_registry
		    SET schema_status = 'rejected',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = ?,
		        is_active = false,
		        updated_at = NOW()
		  WHERE master_name = ?`,
		actor, req.Reason, name,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"status": "rejected", "master_name": name})
}

// ToggleActive — POST /api/v1/masters/:name/toggle-active (destructive).
// Requires schema_status='approved' — otherwise CHECK constraint rejects.
func (h *MasterRegistryHandler) ToggleActive(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}

	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_internal.master_table_registry
		    SET is_active = NOT is_active, updated_at = NOW()
		  WHERE master_name = ?`,
		name,
	)
	if res.Error != nil {
		if strings.Contains(res.Error.Error(), "master_active_requires_approved") {
			return c.Status(409).JSON(fiber.Map{
				"error":  "requires_approved",
				"detail": "cannot set is_active=true until schema_status='approved'",
			})
		}
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"status": "toggled", "master_name": name})
}

// getActor pulls username from JWT locals; falls back to "admin" in dev.
func getActor(c *fiber.Ctx) string {
	if s, ok := c.Locals("sub").(string); ok && s != "" {
		return s
	}
	if s, ok := c.Locals("username").(string); ok && s != "" {
		return s
	}
	return "admin"
}
