package api

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SchemaProposalHandler — Sprint 5 §R9 admin plane for schema proposals.
// Listens on /api/v1/schema-proposals/*. Read ops shared; write ops
// destructive.
type SchemaProposalHandler struct {
	db     *gorm.DB
	bus    ports.CommandBus
	logger *zap.Logger
}

func NewSchemaProposalHandler(db *gorm.DB, bus ports.CommandBus, logger *zap.Logger) *SchemaProposalHandler {
	return &SchemaProposalHandler{db: db, bus: bus, logger: logger}
}

var propIdentRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type ProposalRow struct {
	ID                  int64           `json:"id"`
	TableName           string          `json:"table_name"`
	TableLayer          string          `json:"table_layer"`
	ColumnName          string          `json:"column_name"`
	ProposedDataType    string          `json:"proposed_data_type"`
	ProposedJSONPath    *string         `json:"proposed_jsonpath,omitempty"`
	ProposedTransformFn *string         `json:"proposed_transform_fn,omitempty"`
	ProposedIsNullable  bool            `json:"proposed_is_nullable"`
	SampleValues        json.RawMessage `json:"sample_values,omitempty"`
	Status              string          `json:"status"`
	SubmittedBy         string          `json:"submitted_by"`
	SubmittedAt         time.Time       `json:"submitted_at"`
	ReviewedBy          *string         `json:"reviewed_by,omitempty"`
	ReviewedAt          *time.Time      `json:"reviewed_at,omitempty"`
	AppliedAt           *time.Time      `json:"applied_at,omitempty"`
	RejectionReason     *string         `json:"rejection_reason,omitempty"`
	OverrideDataType    *string         `json:"override_data_type,omitempty"`
	OverrideJSONPath    *string         `json:"override_jsonpath,omitempty"`
	OverrideTransformFn *string         `json:"override_transform_fn,omitempty"`
	ErrorMessage        *string         `json:"error_message,omitempty"`
}

// List — GET /api/v1/schema-proposals?status=pending
func (h *SchemaProposalHandler) List(c *fiber.Ctx) error {
	status := c.Query("status", "")
	var rows []ProposalRow
	q := h.db.WithContext(c.Context()).Table("cdc_system.schema_proposal")
	if status != "" {
		q = q.Where("status = ?", status)
	}
	err := q.Order("submitted_at DESC").Limit(200).Scan(&rows).Error
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error", "detail": err.Error()})
	}
	return c.JSON(fiber.Map{"data": rows, "count": len(rows)})
}

// Get — GET /api/v1/schema-proposals/:id
func (h *SchemaProposalHandler) Get(c *fiber.Ctx) error {
	id := c.Params("id")
	var row ProposalRow
	err := h.db.WithContext(c.Context()).Table("cdc_system.schema_proposal").
		Where("id = ?", id).Scan(&row).Error
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if row.ID == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"data": row})
}

// ApproveProposalRequest — optional overrides + reason.
type ApproveProposalRequest struct {
	OverrideDataType    *string `json:"override_data_type"`
	OverrideJSONPath    *string `json:"override_jsonpath"`
	OverrideTransformFn *string `json:"override_transform_fn"`
	Reason              string  `json:"reason"`
}

// Approve — POST /api/v1/schema-proposals/:id/approve (destructive).
// Delegates to ApproveSchemaProposalCommand: shadow layer ALTER TABLE
// on resolved shadow_schema; master layer ALTER TABLE public + INSERT
// cdc_mapping_rules. Full transaction in the command handler;
// rollback on any error with best-effort failure-mark.
func (h *SchemaProposalHandler) Approve(c *fiber.Ctx) error {
	idStr := c.Params("id")
	proposalID, perr := strconv.ParseInt(idStr, 10, 64)
	if perr != nil || proposalID <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_id"})
	}

	var req ApproveProposalRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	user := middleware.GetUsername(c)
	cmd := commands.ApproveSchemaProposalCommand{
		ProposalID:          proposalID,
		OverrideDataType:    req.OverrideDataType,
		OverrideJSONPath:    req.OverrideJSONPath,
		OverrideTransformFn: req.OverrideTransformFn,
		Reason:              req.Reason,
		ReviewedBy:          actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrSchemaProposalNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrSchemaProposalNotPending):
			return c.Status(409).JSON(fiber.Map{"error": "not_pending"})
		case errors.Is(err, commands.ErrSchemaProposalInvalidDataType):
			return c.Status(400).JSON(fiber.Map{"error": "invalid_data_type"})
		case errors.Is(err, commands.ErrSchemaProposalInvalidIdent):
			return c.Status(400).JSON(fiber.Map{"error": "invalid_identifiers_in_proposal"})
		case errors.Is(err, commands.ErrSchemaProposalApplyFailed):
			h.logger.Error("proposal approve failed",
				zap.Int64("id", proposalID), zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "apply_failed", "detail": err.Error()})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "internal_error", "detail": err.Error()})
		}
	}

	h.logger.Info("proposal approved",
		zap.Int64("id", proposalID),
		zap.String("actor", actor))

	var body map[string]interface{}
	_ = json.Unmarshal(res.ResultBody, &body)
	return c.JSON(body)
}

// RejectProposalRequest — reason required.
type RejectProposalRequest struct {
	Reason string `json:"reason"`
}

// Reject — POST /api/v1/schema-proposals/:id/reject (destructive).
func (h *SchemaProposalHandler) Reject(c *fiber.Ctx) error {
	id := c.Params("id")
	var req RejectProposalRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}
	actor := getActor(c)
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
	cmd := commands.RejectSchemaProposalCommand{
		ProposalID:      id,
		RejectionReason: req.Reason,
		ReviewedBy:      actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	if _, err := h.bus.Execute(ctx, cmd); err != nil {
		switch {
		case errors.Is(err, commands.ErrSchemaProposalNotPendingOrNotFound):
			return c.Status(409).JSON(fiber.Map{"error": "not_pending_or_not_found"})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	return c.JSON(fiber.Map{"status": "rejected", "id": id})
}
