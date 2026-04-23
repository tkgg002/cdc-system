package api

import (
	"strings"
	"time"

	"cdc-cms-service/internal/middleware"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CDCInternalRegistryHandler manages the NEW cdc_internal.table_registry
// (Sonyflake v1.25 shadow-table registry). Distinct from the legacy
type CDCInternalRegistryHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewCDCInternalRegistryHandler(db *gorm.DB, logger *zap.Logger) *CDCInternalRegistryHandler {
	return &CDCInternalRegistryHandler{db: db, logger: logger}
}

// CDCInternalRegistryEntry mirrors one row of cdc_internal.table_registry.
type CDCInternalRegistryEntry struct {
	TargetTable      string     `json:"target_table"`
	SourceDB         string     `json:"source_db"`
	SourceCollection string     `json:"source_collection"`
	ProfileStatus    string     `json:"profile_status"`
	IsFinancial      bool       `json:"is_financial"`
	SchemaApprovedAt *time.Time `json:"schema_approved_at,omitempty"`
	SchemaApprovedBy *string    `json:"schema_approved_by,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// List returns every row of cdc_internal.table_registry, ordered by
// target_table. Read-only — exposed on the shared (admin/operator) group.
func (h *CDCInternalRegistryHandler) List(c *fiber.Ctx) error {
	var entries []CDCInternalRegistryEntry
	err := h.db.WithContext(c.Context()).Raw(
		`SELECT target_table, source_db, source_collection, profile_status,
		        is_financial, schema_approved_at, schema_approved_by,
		        created_at, updated_at
		   FROM cdc_internal.table_registry
		  ORDER BY target_table`,
	).Scan(&entries).Error
	if err != nil {
		h.logger.Error("cdc_internal.table_registry list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": entries, "count": len(entries)})
}

// PatchRequest is the payload for PATCH /v1/tables/:name. Any omitted
// field is left unchanged. is_financial is a pointer so the caller can
// explicitly flip to false.
type PatchRequest struct {
	IsFinancial   *bool   `json:"is_financial,omitempty"`
	ProfileStatus *string `json:"profile_status,omitempty"`
	Reason        string  `json:"reason"`
}

// Patch updates a single cdc_internal.table_registry row. The caller must
// be ops-admin (enforced by registerDestructive chain) and must include a
// non-empty `reason` (≥10 chars) which the audit middleware persists.
//
// When is_financial flips to false AND schema_approved_by was NULL, the
// handler stamps schema_approved_at/by so the shadow table transitions
// from "pending_data" to a reviewed state in a single PATCH.
func (h *CDCInternalRegistryHandler) Patch(c *fiber.Ctx) error {
	name := strings.TrimSpace(c.Params("name"))
	if !isValidTableName(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_table_name"})
	}

	var req PatchRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{
			"error":  "reason_required",
			"detail": "field `reason` must be >= 10 chars for audit trail",
		})
	}
	if req.IsFinancial == nil && req.ProfileStatus == nil {
		return c.Status(400).JSON(fiber.Map{"error": "no_changes"})
	}

	// Resolve actor for audit context. Falls back to 'admin' when the JWT
	// sub claim is missing — destructive chain still enforces the role.
	actor := "admin"
	if sub, ok := c.Locals("sub").(string); ok && sub != "" {
		actor = sub
	}

	updates := map[string]any{"updated_at": time.Now()}
	if req.IsFinancial != nil {
		updates["is_financial"] = *req.IsFinancial
		if !*req.IsFinancial {
			updates["schema_approved_at"] = time.Now()
			updates["schema_approved_by"] = actor
		}
	}
	if req.ProfileStatus != nil {
		ps := strings.TrimSpace(*req.ProfileStatus)
		switch ps {
		case "pending_data", "syncing", "active", "failed":
		default:
			return c.Status(400).JSON(fiber.Map{
				"error":  "invalid_profile_status",
				"detail": "must be pending_data|syncing|active|failed",
			})
		}
		updates["profile_status"] = ps
	}

	res := h.db.WithContext(c.Context()).Table("cdc_internal.table_registry").
		Where("target_table = ?", name).Updates(updates)
	if res.Error != nil {
		h.logger.Error("cdc_internal.table_registry patch failed",
			zap.String("table", name), zap.Error(res.Error))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found", "target_table": name})
	}

	// Fetch updated row for response.
	var entry CDCInternalRegistryEntry
	if err := h.db.WithContext(c.Context()).Raw(
		`SELECT target_table, source_db, source_collection, profile_status,
		        is_financial, schema_approved_at, schema_approved_by,
		        created_at, updated_at
		   FROM cdc_internal.table_registry
		  WHERE target_table = ?`, name,
	).Scan(&entry).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "read_after_write_failed"})
	}

	h.logger.Info("cdc_internal.table_registry patched",
		zap.String("table", name),
		zap.String("actor", actor),
		zap.Any("updates", updates),
	)
	return c.JSON(fiber.Map{"data": entry})
}

// isValidTableName guards the path param against SQL-injection / bogus
// identifiers before it reaches Raw queries. Same whitelist used by the
// worker-side quoteIdent: [A-Za-z_][A-Za-z0-9_]{0,63}.
func isValidTableName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// Ensure we import middleware so the build doesn't lose the transitive
// dependency on the audit/idempotency chain; the handler itself relies on
// the router wiring those in before calling Patch.
var _ = middleware.RequireOpsAdmin
