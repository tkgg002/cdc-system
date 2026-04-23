package api

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SchemaProposalHandler — Sprint 5 §R9 admin plane for schema proposals.
// Listens on /api/v1/schema-proposals/*. Read ops shared; write ops
// destructive.
type SchemaProposalHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewSchemaProposalHandler(db *gorm.DB, logger *zap.Logger) *SchemaProposalHandler {
	return &SchemaProposalHandler{db: db, logger: logger}
}

var (
	propIdentRe   = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	propColumnRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)
	propTypeRe    = regexp.MustCompile(`^(SMALLINT|INTEGER|BIGINT|REAL|DOUBLE PRECISION|BOOLEAN|DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL|JSON|JSONB|UUID|INET|CIDR|MACADDR|BYTEA|TEXT|CHAR\([1-9][0-9]{0,7}\)|VARCHAR\([1-9][0-9]{0,7}\)|NUMERIC\([1-9][0-9]?,[0-9][0-9]?\)|DECIMAL\([1-9][0-9]?,[0-9][0-9]?\))$`)
)

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
	q := h.db.WithContext(c.Context()).Table("cdc_internal.schema_proposal")
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
	err := h.db.WithContext(c.Context()).Table("cdc_internal.schema_proposal").
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
// Applies ALTER TABLE for shadow layer; inserts mapping_rule row for
// master layer. Full transaction; rollback on any error.
func (h *SchemaProposalHandler) Approve(c *fiber.Ctx) error {
	id := c.Params("id")

	var req ApproveProposalRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	// Load the proposal.
	var row ProposalRow
	if err := h.db.WithContext(c.Context()).Table("cdc_internal.schema_proposal").
		Where("id = ?", id).Scan(&row).Error; err != nil || row.ID == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	if row.Status != "pending" {
		return c.Status(409).JSON(fiber.Map{"error": "not_pending", "status": row.Status})
	}

	finalType := row.ProposedDataType
	if req.OverrideDataType != nil && *req.OverrideDataType != "" {
		finalType = *req.OverrideDataType
	}
	if !propTypeRe.MatchString(finalType) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_data_type", "data_type": finalType})
	}
	if !propIdentRe.MatchString(row.TableName) || !propColumnRe.MatchString(row.ColumnName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_identifiers_in_proposal"})
	}

	actor := getActor(c)

	err := h.db.WithContext(c.Context()).Transaction(func(tx *gorm.DB) error {
		switch row.TableLayer {
		case "shadow":
			stmt := fmt.Sprintf(
				`ALTER TABLE cdc_internal.%q ADD COLUMN IF NOT EXISTS %q %s`,
				row.TableName, row.ColumnName, finalType,
			)
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("alter shadow: %w", err)
			}
		case "master":
			stmt := fmt.Sprintf(
				`ALTER TABLE public.%q ADD COLUMN IF NOT EXISTS %q %s`,
				row.TableName, row.ColumnName, finalType,
			)
			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("alter master: %w", err)
			}
			finalPath := ""
			if req.OverrideJSONPath != nil {
				finalPath = *req.OverrideJSONPath
			} else if row.ProposedJSONPath != nil {
				finalPath = *row.ProposedJSONPath
			}
			finalFn := ""
			if req.OverrideTransformFn != nil {
				finalFn = *req.OverrideTransformFn
			} else if row.ProposedTransformFn != nil {
				finalFn = *row.ProposedTransformFn
			}
			// Insert mapping rule pointing to the new master column.
			insertSQL := `INSERT INTO cdc_mapping_rules
			  (source_table, master_table, source_field, target_column, data_type,
			   source_format, jsonpath, transform_fn, is_active, status,
			   approved_by_admin, approved_at, created_by, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, 'debezium_after', NULLIF(?, ''), NULLIF(?, ''),
			         true, 'approved', true, NOW(), ?, NOW(), NOW())
			 ON CONFLICT DO NOTHING`
			if err := tx.Exec(insertSQL,
				row.TableName, row.TableName,
				row.ColumnName, row.ColumnName, finalType,
				finalPath, finalFn, actor,
			).Error; err != nil {
				return fmt.Errorf("insert mapping_rule: %w", err)
			}
		default:
			return fmt.Errorf("invalid table_layer: %s", row.TableLayer)
		}

		// Mark proposal approved.
		if err := tx.Exec(
			`UPDATE cdc_internal.schema_proposal
			    SET status = 'approved',
			        reviewed_by = ?,
			        reviewed_at = NOW(),
			        applied_at = NOW(),
			        override_data_type = ?,
			        override_jsonpath = ?,
			        override_transform_fn = ?,
			        error_message = NULL,
			        updated_at = NOW()
			  WHERE id = ?`,
			actor, req.OverrideDataType, req.OverrideJSONPath, req.OverrideTransformFn, id,
		).Error; err != nil {
			return fmt.Errorf("mark approved: %w", err)
		}
		return nil
	})
	if err != nil {
		// Mark failed (best-effort, don't rollback row).
		_ = h.db.WithContext(c.Context()).Exec(
			`UPDATE cdc_internal.schema_proposal SET status='failed', error_message=?, updated_at=NOW() WHERE id=?`,
			err.Error(), id,
		).Error
		h.logger.Error("proposal approve failed",
			zap.String("id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "apply_failed", "detail": err.Error()})
	}

	h.logger.Info("proposal approved",
		zap.String("id", id),
		zap.String("table", row.TableName),
		zap.String("column", row.ColumnName),
		zap.String("final_type", finalType),
		zap.String("actor", actor))

	return c.JSON(fiber.Map{
		"status":     "approved",
		"id":         id,
		"table":      row.TableName,
		"column":     row.ColumnName,
		"final_type": finalType,
	})
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

	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_internal.schema_proposal
		    SET status = 'rejected',
		        reviewed_by = ?,
		        reviewed_at = NOW(),
		        rejection_reason = ?,
		        updated_at = NOW()
		  WHERE id = ? AND status = 'pending'`,
		actor, req.Reason, id,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(409).JSON(fiber.Map{"error": "not_pending_or_not_found"})
	}
	return c.JSON(fiber.Map{"status": "rejected", "id": id})
}
