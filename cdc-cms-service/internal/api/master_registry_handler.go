package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// MasterRegistryHandler — Sprint 5 §R8 admin plane for master tables.
// Mounts under /api/v1/masters/*. Write ops go through destructive chain;
// read ops are shared (admin|operator).
//
// Phase 2 v2 / P2 — `List` delegates to the CQRS Q-side handler in
// `internal/app/queries/list_masters.go`; raw SQL lives in
// `internal/infra/persistence/master_read_repo_gorm.go`. Write ops
// (Create / Approve / Reject / ToggleActive / Swap) move to
// `app/commands/` in P3.
type MasterRegistryHandler struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	swap   *persistence.MasterSwap
	logger *zap.Logger
	listQ  *queries.ListMastersHandler
	bus    ports.CommandBus
}

func NewMasterRegistryHandler(db *gorm.DB, nats *natsconn.NatsClient, swap *persistence.MasterSwap, logger *zap.Logger, listQ *queries.ListMastersHandler, bus ports.CommandBus) *MasterRegistryHandler {
	return &MasterRegistryHandler{db: db, nats: nats, swap: swap, logger: logger, listQ: listQ, bus: bus}
}

var (
	masterNameRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	namespaceName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// MasterRow is the wire shape of one row in /api/v1/masters. Aliased
// to the Q-side read model so server-side callers + Swagger see the
// same type.
type MasterRow = queries.MasterListItem

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
	res, err := h.listQ.Handle(c.Context(), queries.ListMastersQuery{})
	if err != nil {
		h.logger.Error("master list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	return c.JSON(fiber.Map{"data": res.Data, "count": res.Count})
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
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.CreateMasterCommand{
		MasterName:           req.MasterName,
		MasterSchema:         req.MasterSchema,
		MasterConnectionCode: req.MasterConnectionCode,
		SourceShadow:         req.SourceShadow,
		SourceDatabase:       req.SourceDatabase,
		SourceSchema:         req.SourceSchema,
		SourceNamespace:      req.SourceNamespace,
		SourceTable:          req.SourceTable,
		ShadowSchema:         req.ShadowSchema,
		ShadowTable:          req.ShadowTable,
		TransformType:        req.TransformType,
		Spec:                 req.Spec,
		Reason:               req.Reason,
		UpdatedBy:            actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrShadowBindingNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "shadow_binding_not_found"})
		case errors.Is(err, commands.ErrShadowBindingAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_shadow_binding"})
		case errors.Is(err, commands.ErrMasterConnectionNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "master_connection_not_found"})
		case errors.Is(err, commands.ErrMasterConnectionAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_connection"})
		case errors.Is(err, commands.ErrMasterAlreadyExists):
			return c.Status(409).JSON(fiber.Map{"error": "master_already_exists", "master_name": req.MasterName})
		case strings.Contains(err.Error(), "invalid_"):
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		case strings.Contains(err.Error(), "reason_required"):
			return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
		default:
			h.logger.Error("master create failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(201).Send(res.ResultBody)
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

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.ApproveMasterCommand{
		Name:      name,
		Reason:    strings.TrimSpace(req.Reason),
		UpdatedBy: actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMasterNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrMasterNameAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_name"})
		case errors.Is(err, commands.ErrMasterNotApprovable):
			return c.Status(409).JSON(fiber.Map{"error": "not_approvable", "detail": "master not found OR already approved"})
		default:
			h.logger.Error("approve master failed", zap.String("master", name), zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Type("application/json")
	return c.Status(202).Send(res.ResultBody)
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

	var req ApproveRequest
	_ = c.BodyParser(&req)
	if len(strings.TrimSpace(req.Reason)) < 10 {
		return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	actor := getActor(c)
	cmd := commands.RejectMasterCommand{
		Name:      name,
		Reason:    strings.TrimSpace(req.Reason),
		UpdatedBy: actor,
	}
	ctx := messaging.WithMetadata(c.UserContext(), actor, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMasterNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not_found"})
		case errors.Is(err, commands.ErrMasterNameAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_master_name"})
		case strings.Contains(err.Error(), "invalid_master_name"):
			return c.Status(400).JSON(fiber.Map{"error": "invalid_master_name"})
		case strings.Contains(err.Error(), "reason_required"):
			return c.Status(400).JSON(fiber.Map{"error": "reason_required_min_10_chars"})
		default:
			h.logger.Error("master reject failed", zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(200).Send(res.ResultBody)
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

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}
	user := middleware.GetUsername(c)
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

type SwapRequest struct {
	NewTableName string `json:"new_table_name"`
	Reason       string `json:"reason"`
}

// Swap godoc
// @Summary      Swap physical master tables (async)
// @Description  Kicks off an atomic master-table swap as a background job. Returns 202 + JobID; poll GET /api/jobs/:id for terminal status. Refuses with 409 if another swap for the same master is still in flight.
// @Tags         Masters
// @Accept       json
// @Produce      json
// @Param        name path string true "Master table name"
// @Param        body body SwapRequest true "Swap payload"
// @Success      202 {object} map[string]interface{}
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

	createdBy := getActor(c)
	correlationID, _ := c.Locals("correlation_id").(string)
	idempotencyKey := c.Get("Idempotency-Key")

	jobID, err := h.swap.SwapAsync(c.Context(), name, req.NewTableName, req.Reason, createdBy, correlationID, idempotencyKey)
	if err != nil {
		if strings.Contains(err.Error(), "master_swap_in_flight") {
			return c.Status(409).JSON(fiber.Map{"error": "master_swap_in_flight", "detail": err.Error()})
		}
		if strings.Contains(err.Error(), "invalid_") {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		h.logger.Error("master swap dispatch failed", zap.String("master", name), zap.String("new_table", req.NewTableName), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "swap dispatch failed: " + err.Error()})
	}
	return c.Status(202).JSON(fiber.Map{
		"status":      "accepted",
		"master_name": name,
		"job_id":      jobID,
	})
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
