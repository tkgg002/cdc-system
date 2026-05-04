package api

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cdc-cms-service/internal/service"
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
	swap   *service.MasterSwap
	logger *zap.Logger
}

func NewMasterRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, swap *service.MasterSwap, logger *zap.Logger) *MasterRegistryHandler {
	return &MasterRegistryHandler{db: db, nats: nats, swap: swap, logger: logger}
}

var (
	masterNameRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	namespaceName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

type MasterRow struct {
	ID                   int64           `json:"id"`
	BindingCode          string          `json:"binding_code"`
	MasterName           string          `json:"master_name"`
	MasterSchema         string          `json:"master_schema"`
	MasterDatabase       *string         `json:"master_database,omitempty"`
	MasterConnectionCode *string         `json:"master_connection_code,omitempty"`
	SourceShadow         string          `json:"source_shadow"`
	SourceDatabase       *string         `json:"source_database,omitempty"`
	SourceSchema         *string         `json:"source_schema,omitempty"`
	SourceNamespace      *string         `json:"source_namespace,omitempty"`
	SourceTable          *string         `json:"source_table,omitempty"`
	ShadowBindingID      *int64          `json:"shadow_binding_id,omitempty"`
	ShadowSchema         *string         `json:"shadow_schema,omitempty"`
	ShadowTable          *string         `json:"shadow_table,omitempty"`
	PhysicalTableFQN     *string         `json:"physical_table_fqn,omitempty"`
	TransformType        string          `json:"transform_type"`
	Spec                 json.RawMessage `json:"spec"`
	IsActive             bool            `json:"is_active"`
	SchemaStatus         string          `json:"schema_status"`
	SchemaReviewedBy     *string         `json:"schema_reviewed_by,omitempty"`
	SchemaReviewedAt     *time.Time      `json:"schema_reviewed_at,omitempty"`
	RejectionReason      *string         `json:"rejection_reason,omitempty"`
	CreatedBy            *string         `json:"created_by,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

type masterConnectionTarget struct {
	ID              int64
	ConnectionCode  string
	DefaultDatabase *string
	DefaultSchema   *string
}

type shadowBindingTarget struct {
	ShadowBindingID int64
	SourceObjectID  int64
	SourceDatabase  *string
	SourceSchema    *string
	SourceNamespace *string
	SourceTable     *string
	ShadowSchema    string
	ShadowTable     string
}

type masterBindingTarget struct {
	ID          int64
	MasterTable string
}

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
	var rows []MasterRow
	err := h.db.WithContext(c.Context()).Raw(
		`SELECT
			mb.id,
			mb.binding_code,
			mb.master_table AS master_name,
			mb.master_schema,
			mb.master_database,
			mc.connection_code AS master_connection_code,
			COALESCE(sb.shadow_schema || '.' || sb.shadow_table, sb.shadow_table, '') AS source_shadow,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			mb.shadow_binding_id,
			sb.shadow_schema,
			sb.shadow_table,
			mb.physical_table_fqn,
			mb.transform_type,
			mb.transform_spec AS spec,
			mb.is_active,
			mb.schema_status,
			mb.schema_reviewed_by,
			mb.schema_reviewed_at,
			mb.rejection_reason,
			mb.created_by,
			mb.created_at,
			mb.updated_at
		FROM cdc_system.master_binding mb
		LEFT JOIN cdc_system.shadow_binding sb
		  ON sb.id = mb.shadow_binding_id
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.id = mb.source_object_id
		LEFT JOIN cdc_system.connection_registry mc
		  ON mc.id = mb.master_connection_id
		ORDER BY mb.master_schema, mb.master_table`,
	).Scan(&rows).Error
	if err != nil {
		h.logger.Error("master list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": rows, "count": len(rows)})
}

type CreateRequest struct {
	MasterName           string          `json:"master_name"`
	MasterSchema         string          `json:"master_schema"`
	MasterConnectionCode string          `json:"master_connection_code"`
	SourceShadow         string          `json:"source_shadow"`
	SourceDatabase       string          `json:"source_database"`
	SourceSchema         string          `json:"source_schema"`
	SourceNamespace      string          `json:"source_namespace"`
	SourceTable          string          `json:"source_table"`
	ShadowSchema         string          `json:"shadow_schema"`
	ShadowTable          string          `json:"shadow_table"`
	TransformType        string          `json:"transform_type"`
	Spec                 json.RawMessage `json:"spec"`
	Reason               string          `json:"reason"`
}

func trimString(v string) string {
	return strings.TrimSpace(v)
}

func normalizeBindingCode(parts ...string) string {
	joined := strings.Join(parts, "_")
	joined = strings.ToLower(joined)
	replacer := regexp.MustCompile(`[^a-z0-9_]+`)
	joined = replacer.ReplaceAllString(joined, "_")
	joined = strings.Trim(joined, "_")
	if joined == "" {
		joined = "binding"
	}
	if len(joined) > 120 {
		joined = joined[:120]
	}
	return joined
}

func (h *MasterRegistryHandler) resolveMasterConnection(ctx *fiber.Ctx, req CreateRequest) (*masterConnectionTarget, error) {
	code := trimString(req.MasterConnectionCode)
	query := `
		SELECT id, connection_code, default_database, default_schema
		FROM cdc_system.connection_registry
		WHERE role_type = 'master'
		  AND status = 'active'
	`
	args := []interface{}{}
	if code != "" {
		query += ` AND connection_code = ?`
		args = append(args, code)
	}
	query += ` ORDER BY updated_at DESC, id DESC LIMIT 2`

	var rows []masterConnectionTarget
	if err := h.db.WithContext(ctx.Context()).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 && code == "" {
		return nil, fmt.Errorf("ambiguous_master_connection")
	}
	return &rows[0], nil
}

func (h *MasterRegistryHandler) resolveShadowBinding(ctx *fiber.Ctx, req CreateRequest) (*shadowBindingTarget, error) {
	shadowSchema := trimString(req.ShadowSchema)
	shadowTable := trimString(req.ShadowTable)
	sourceDatabase := trimString(req.SourceDatabase)
	sourceSchema := trimString(req.SourceSchema)
	sourceNamespace := trimString(req.SourceNamespace)
	sourceTable := trimString(req.SourceTable)
	sourceShadow := trimString(req.SourceShadow)

	query := `
		SELECT
			sb.id AS shadow_binding_id,
			sb.source_object_id,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table
		FROM cdc_system.shadow_binding sb
		JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		WHERE sb.is_active = TRUE
	`
	args := make([]interface{}, 0, 6)
	if shadowSchema != "" {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, shadowSchema)
	}
	if shadowTable != "" {
		query += ` AND sb.shadow_table = ?`
		args = append(args, shadowTable)
	}
	if sourceShadow != "" && shadowTable == "" {
		query += ` AND sb.shadow_table = ?`
		args = append(args, sourceShadow)
	}
	if sourceDatabase != "" {
		query += ` AND so.source_database = ?`
		args = append(args, sourceDatabase)
	}
	if sourceSchema != "" {
		query += ` AND so.source_schema = ?`
		args = append(args, sourceSchema)
	}
	if sourceNamespace != "" {
		query += ` AND so.source_namespace = ?`
		args = append(args, sourceNamespace)
	}
	if sourceTable != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, sourceTable)
	}
	query += ` ORDER BY sb.updated_at DESC, sb.id DESC LIMIT 2`

	var rows []shadowBindingTarget
	if err := h.db.WithContext(ctx.Context()).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, fmt.Errorf("ambiguous_shadow_binding")
	}
	return &rows[0], nil
}

func (h *MasterRegistryHandler) resolveMasterBindingByName(ctx *fiber.Ctx, masterName string) (*masterBindingTarget, error) {
	var rows []masterBindingTarget
	err := h.db.WithContext(ctx.Context()).Raw(
		`SELECT id, master_table
		   FROM cdc_system.master_binding
		  WHERE master_table = ?
		  ORDER BY updated_at DESC, id DESC
		  LIMIT 2`,
		masterName,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, fmt.Errorf("ambiguous_master_name")
	}
	return &rows[0], nil
}

// Create godoc
// @Summary      Create master binding
// @Description  Creates a V2 master binding in cdc_system. Accepts richer shadow/source scope and keeps source_shadow as compatibility fallback.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        body body CreateRequest true "Master binding create payload"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters [post]
func (h *MasterRegistryHandler) Create(c *fiber.Ctx) error {
	var req CreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}

	req.MasterName = trimString(req.MasterName)
	req.MasterSchema = trimString(req.MasterSchema)
	req.MasterConnectionCode = trimString(req.MasterConnectionCode)
	req.SourceShadow = trimString(req.SourceShadow)
	req.SourceDatabase = trimString(req.SourceDatabase)
	req.SourceSchema = trimString(req.SourceSchema)
	req.SourceNamespace = trimString(req.SourceNamespace)
	req.SourceTable = trimString(req.SourceTable)
	req.ShadowSchema = trimString(req.ShadowSchema)
	req.ShadowTable = trimString(req.ShadowTable)
	req.TransformType = trimString(req.TransformType)

	if !masterNameRe.MatchString(req.MasterName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	if req.MasterSchema == "" {
		req.MasterSchema = "public"
	}
	if !namespaceName.MatchString(req.MasterSchema) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_schema"})
	}

	validType := map[string]bool{
		"copy_1_to_1": true, "filter": true, "aggregate": true,
		"group_by": true, "join": true, "custom_sql": true,
	}
	if !validType[req.TransformType] {
		return c.Status(400).JSON(fiber.Map{
			"error":  "invalid_transform_type",
			"detail": "one of copy_1_to_1|filter|aggregate|group_by|join|custom_sql",
		})
	}
	if len(req.Spec) == 0 {
		req.Spec = json.RawMessage("{}")
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	shadowBinding, err := h.resolveShadowBinding(c, req)
	if err != nil {
		switch err.Error() {
		case "ambiguous_shadow_binding":
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_shadow_binding"})
		default:
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{"error": "shadow_binding_not_found"})
			}
			h.logger.Error("resolve shadow binding failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	masterConn, err := h.resolveMasterConnection(c, req)
	if err != nil {
		switch err.Error() {
		case "ambiguous_master_connection":
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_connection"})
		default:
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{"error": "master_connection_not_found"})
			}
			h.logger.Error("resolve master connection failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	if req.MasterSchema == "public" && masterConn.DefaultSchema != nil && strings.TrimSpace(*masterConn.DefaultSchema) != "" {
		req.MasterSchema = strings.TrimSpace(*masterConn.DefaultSchema)
	}

	actor := getActor(c)
	bindingCode := normalizeBindingCode("mb", req.MasterSchema, req.MasterName, fmt.Sprintf("%d", time.Now().UTC().Unix()))
	physicalTableFQN := req.MasterSchema + "." + req.MasterName

	err = h.db.WithContext(c.Context()).Exec(
		`INSERT INTO cdc_system.master_binding
		   (binding_code, source_object_id, shadow_binding_id, master_connection_id,
		    master_database, master_schema, master_table, physical_table_fqn,
		    transform_type, transform_spec, schema_status, is_active, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'pending_review', false, ?, NOW(), NOW())`,
		bindingCode,
		shadowBinding.SourceObjectID,
		shadowBinding.ShadowBindingID,
		masterConn.ID,
		masterConn.DefaultDatabase,
		req.MasterSchema,
		req.MasterName,
		physicalTableFQN,
		req.TransformType,
		string(req.Spec),
		actor,
	).Error
	if err != nil {
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return c.Status(409).JSON(fiber.Map{"error": "master_already_exists", "master_name": req.MasterName})
		}
		h.logger.Error("master create failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}

	return c.Status(201).JSON(fiber.Map{
		"master_name":            req.MasterName,
		"master_schema":          req.MasterSchema,
		"master_connection_code": masterConn.ConnectionCode,
		"shadow_schema":          shadowBinding.ShadowSchema,
		"shadow_table":           shadowBinding.ShadowTable,
		"schema_status":          "pending_review",
		"next":                   "POST /api/v1/masters/" + req.MasterName + "/approve",
	})
}

type ApproveRequest struct {
	Reason string `json:"reason"`
}

// Approve godoc
// @Summary      Approve master binding
// @Description  Marks a V2 master binding as approved and dispatches cdc.cmd.master-create to the worker.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        name path string true "Master table name"
// @Param        body body ApproveRequest true "Approval payload"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters/{name}/approve [post]
func (h *MasterRegistryHandler) Approve(c *fiber.Ctx) error {
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
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{"error": "not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)
	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_system.master_binding
		    SET schema_status = 'approved',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = NULL,
		        updated_at = NOW()
		  WHERE id = ?
		    AND schema_status IN ('pending_review','rejected','failed')`,
		actor, target.ID,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(409).JSON(fiber.Map{"error": "not_approvable", "detail": "master not found OR already approved"})
	}

	payload, _ := json.Marshal(map[string]string{
		"master_table":   name,
		"triggered_by":   actor,
		"correlation_id": "approve-" + name + "-" + time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err := h.nats.Conn.Publish("cdc.cmd.master-create", payload); err != nil {
		h.logger.Warn("master-create publish failed", zap.String("master", name), zap.Error(err))
		return c.Status(202).JSON(fiber.Map{"status": "approved_but_dispatch_failed", "master_name": name, "dispatch_err": err.Error()})
	}

	return c.Status(202).JSON(fiber.Map{"status": "approved", "master_name": name, "dispatched": "cdc.cmd.master-create"})
}

// Reject godoc
// @Summary      Reject master binding
// @Description  Rejects a V2 master binding and clears active state.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        name path string true "Master table name"
// @Param        body body ApproveRequest true "Rejection payload"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters/{name}/reject [post]
func (h *MasterRegistryHandler) Reject(c *fiber.Ctx) error {
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
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{"error": "not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	actor := getActor(c)
	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_system.master_binding
		    SET schema_status = 'rejected',
		        schema_reviewed_by = ?,
		        schema_reviewed_at = NOW(),
		        rejection_reason = ?,
		        is_active = false,
		        updated_at = NOW()
		  WHERE id = ?`,
		actor, req.Reason, target.ID,
	)
	if res.Error != nil {
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"status": "rejected", "master_name": name})
}

// ToggleActive godoc
// @Summary      Toggle master binding active state
// @Description  Flips is_active on a V2 master binding. Active=true still requires schema_status=approved.
// @Tags         Masters
// @Produce      json
// @Param        name path string true "Master table name"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters/{name}/toggle-active [post]
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
			if err == gorm.ErrRecordNotFound {
				return c.Status(404).JSON(fiber.Map{"error": "not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}

	res := h.db.WithContext(c.Context()).Exec(
		`UPDATE cdc_system.master_binding
		    SET is_active = NOT is_active, updated_at = NOW()
		  WHERE id = ?`,
		target.ID,
	)
	if res.Error != nil {
		if strings.Contains(res.Error.Error(), "v2_master_active_requires_approved") {
			return c.Status(409).JSON(fiber.Map{"error": "requires_approved", "detail": "cannot set is_active=true until schema_status='approved'"})
		}
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if res.RowsAffected == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(fiber.Map{"status": "toggled", "master_name": name})
}

type SwapRequest struct {
	NewTableName string `json:"new_table_name"`
	Reason       string `json:"reason"`
}

// Swap godoc
// @Summary      Swap physical master tables
// @Description  Performs atomic table swap for a master table name. Current implementation still targets the physical table name and assumes unique master_table naming.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        name path string true "Master table name"
// @Param        body body SwapRequest true "Swap payload"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/masters/{name}/swap [post]
func (h *MasterRegistryHandler) Swap(c *fiber.Ctx) error {
	if h.swap == nil {
		return c.Status(500).JSON(fiber.Map{"error": "swap service not wired"})
	}
	name := strings.TrimSpace(c.Params("name"))
	if !masterNameRe.MatchString(name) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
	}
	var req SwapRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad_json", "detail": err.Error()})
	}
	if !masterNameRe.MatchString(req.NewTableName) {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_new_table_name"})
	}
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if err := h.swap.Swap(c.Context(), name, req.NewTableName, req.Reason); err != nil {
		if strings.Contains(err.Error(), "lock timeout") || strings.Contains(err.Error(), "canceling statement") {
			return c.Status(409).JSON(fiber.Map{"error": "lock_timeout", "detail": err.Error()})
		}
		h.logger.Error("master swap failed", zap.String("master", name), zap.String("new_table", req.NewTableName), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "swap failed: " + err.Error()})
	}
	return c.Status(200).JSON(fiber.Map{"status": "swapped", "master_name": name})
}

func getActor(c *fiber.Ctx) string {
	if s, ok := c.Locals("sub").(string); ok && s != "" {
		return s
	}
	if s, ok := c.Locals("username").(string); ok && s != "" {
		return s
	}
	return "admin"
}
