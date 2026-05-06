package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/domain/mapping"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type MappingRuleHandler struct {
	repo         *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	natsClient   *natsconn.NatsClient
	bus          ports.CommandBus
	listQuery    *queries.ListMappingRulesHandler
	db           *gorm.DB
}

// NewMappingRuleHandler — Phase 2 v2 / P3: bus drives async dispatch
// (cdc.cmd.backfill / cdc.cmd.alter-column) via the CommandBus port.
// listQuery is the CQRS Q-side adapter for GET /api/mapping-rules.
func NewMappingRuleHandler(repo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, nats *natsconn.NatsClient, bus ports.CommandBus, listQuery *queries.ListMappingRulesHandler, db ...*gorm.DB) *MappingRuleHandler {
	h := &MappingRuleHandler{repo: repo, registryRepo: registryRepo, natsClient: nats, bus: bus, listQuery: listQuery}
	if len(db) > 0 {
		h.db = db[0]
	}
	return h
}

// formatPgOF mirrors the Postgres `TO_CHAR(t, 'YYYY-MM-DD"T"HH24:MI:SSOF')`
// output the previous handler emitted. The DB connection runs at UTC, so
// the original wire contract was always +00; we force UTC here so the
// output stays identical regardless of where the Go process runs.
func formatPgOF(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05") + "+00"
}

// ruleToRow maps the domain mapping.Rule onto the legacy MappingRuleRow
// JSON shape so the wire contract for /api/mapping-rules stays identical.
func ruleToRow(r mapping.Rule) MappingRuleRow {
	return MappingRuleRow{
		ID:              r.ID,
		SourceObjectID:  r.SourceObjectID,
		MasterBindingID: r.MasterBindingID,
		SourceDatabase:  r.SourceDatabase,
		SourceSchema:    r.SourceSchema,
		SourceNamespace: r.SourceNamespace,
		SourceTable:     r.SourceTable,
		ShadowSchema:    r.ShadowSchema,
		ShadowTable:     r.ShadowTable,
		SourceField:     r.SourceField,
		SourcePath:      r.SourcePath,
		TargetColumn:    r.TargetColumn,
		DataType:        r.DataType,
		SourceFormat:    r.SourceFormat,
		TransformFn:     r.TransformFn,
		IsNullable:      r.IsNullable,
		IsActive:        r.IsActive,
		Status:          string(r.Status),
		Notes:           r.Notes,
		CreatedBy:       r.CreatedBy,
		UpdatedBy:       r.UpdatedBy,
		CreatedAt:       formatPgOF(r.CreatedAt),
		UpdatedAt:       formatPgOF(r.UpdatedAt),
		RuleType:        "mapping",
		IsEnriched:      r.IsEnriched,
	}
}

type MappingRuleRow struct {
	ID              int64   `json:"id"`
	SourceObjectID  int64   `json:"source_object_id"`
	MasterBindingID *int64  `json:"master_binding_id,omitempty"`
	SourceDatabase  *string `json:"source_database,omitempty"`
	SourceSchema    *string `json:"source_schema,omitempty"`
	SourceNamespace *string `json:"source_namespace,omitempty"`
	SourceTable     string  `json:"source_table"`
	ShadowSchema    *string `json:"shadow_schema,omitempty"`
	ShadowTable     *string `json:"shadow_table,omitempty"`
	SourceField     string  `json:"source_field"`
	SourcePath      *string `json:"source_path,omitempty"`
	TargetColumn    string  `json:"target_column"`
	DataType        string  `json:"data_type"`
	SourceFormat    string  `json:"source_format"`
	TransformFn     *string `json:"transform_fn,omitempty"`
	IsNullable      bool    `json:"is_nullable"`
	IsActive        bool    `json:"is_active"`
	Status          string  `json:"status"`
	Notes           *string `json:"notes,omitempty"`
	CreatedBy       *string `json:"created_by,omitempty"`
	UpdatedBy       *string `json:"updated_by,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
	RuleType        string  `json:"rule_type"`
	IsEnriched      bool    `json:"is_enriched"`
}

type mappingRuleCreateRequest struct {
	SourceObjectID  *int64  `json:"source_object_id"`
	MasterBindingID *int64  `json:"master_binding_id"`
	SourceDatabase  *string `json:"source_database"`
	SourceSchema    *string `json:"source_schema"`
	SourceNamespace *string `json:"source_namespace"`
	SourceTable     string  `json:"source_table"`
	ShadowSchema    *string `json:"shadow_schema"`
	ShadowTable     *string `json:"shadow_table"`
	SourceField     string  `json:"source_field"`
	SourcePath      *string `json:"source_path"`
	TargetColumn    string  `json:"target_column"`
	DataType        string  `json:"data_type"`
	SourceFormat    string  `json:"source_format"`
	TransformFn     *string `json:"transform_fn"`
	IsNullable      *bool   `json:"is_nullable"`
	IsActive        *bool   `json:"is_active"`
	Status          string  `json:"status"`
	Notes           *string `json:"notes"`
}

type mappingRuleScope struct {
	SourceObjectID int64
	SourceTable    string
	ShadowTable    string
}

func ptrTrim(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	return &s
}

func boolDefault(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}

func (h *MappingRuleHandler) resolveScope(c *fiber.Ctx, sourceObjectID *int64, sourceDatabase, sourceSchema, sourceNamespace *string, sourceTable string, shadowSchema, shadowTable *string) (*mappingRuleScope, error) {
	if sourceObjectID != nil && *sourceObjectID > 0 {
		var row mappingRuleScope
		err := h.db.WithContext(c.Context()).Raw(`
			SELECT
				so.id AS source_object_id,
				so.source_object_name AS source_table,
				COALESCE(sb.shadow_table, so.source_object_name) AS shadow_table
			FROM cdc_system.source_object_registry so
			LEFT JOIN cdc_system.shadow_binding sb
			  ON sb.source_object_id = so.id
			 AND sb.is_active = TRUE
			WHERE so.id = ?
			ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST
			LIMIT 1
		`, *sourceObjectID).Scan(&row).Error
		if err != nil {
			return nil, err
		}
		if row.SourceObjectID == 0 {
			return nil, gorm.ErrRecordNotFound
		}
		return &row, nil
	}

	query := `
		SELECT
			so.id AS source_object_id,
			so.source_object_name AS source_table,
			COALESCE(sb.shadow_table, so.source_object_name) AS shadow_table
		FROM cdc_system.source_object_registry so
		LEFT JOIN cdc_system.shadow_binding sb
		  ON sb.source_object_id = so.id
		 AND sb.is_active = TRUE
		WHERE so.is_active = TRUE
	`
	args := make([]interface{}, 0, 6)
	if trimmed := ptrTrim(sourceDatabase); trimmed != nil {
		query += ` AND so.source_database = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(sourceSchema); trimmed != nil {
		query += ` AND so.source_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(sourceNamespace); trimmed != nil {
		query += ` AND so.source_namespace = ?`
		args = append(args, *trimmed)
	}
	if strings.TrimSpace(sourceTable) != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, strings.TrimSpace(sourceTable))
	}
	if trimmed := ptrTrim(shadowSchema); trimmed != nil {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(shadowTable); trimmed != nil {
		query += ` AND sb.shadow_table = ?`
		args = append(args, *trimmed)
	}
	query += ` ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST LIMIT 2`

	var rows []mappingRuleScope
	if err := h.db.WithContext(c.Context()).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, fmt.Errorf("ambiguous_mapping_scope")
	}
	return &rows[0], nil
}

func (h *MappingRuleHandler) getRuleByID(c *fiber.Ctx, id int64) (*MappingRuleRow, error) {
	var rows []MappingRuleRow
	err := h.db.WithContext(c.Context()).Raw(`
		SELECT
			mr.id,
			mr.source_object_id,
			mr.master_binding_id,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table,
			mr.source_field,
			mr.source_path,
			mr.target_column,
			mr.data_type,
			mr.source_format,
			mr.transform_fn,
			mr.is_nullable,
			mr.is_active,
			mr.status,
			mr.notes,
			mr.created_by,
			mr.updated_by,
			TO_CHAR(mr.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF') AS created_at,
			TO_CHAR(mr.updated_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF') AS updated_at,
			'mapping' AS rule_type,
			false AS is_enriched
		FROM cdc_system.mapping_rule_v2 mr
		JOIN cdc_system.source_object_registry so
		  ON so.id = mr.source_object_id
		LEFT JOIN cdc_system.shadow_binding sb
		  ON sb.source_object_id = mr.source_object_id
		 AND sb.is_active = TRUE
		WHERE mr.id = ?
		ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST
		LIMIT 1
	`, id).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	return &rows[0], nil
}

// List godoc
// @Summary      List mapping rules
// @Description  Returns V2 mapping rules, optionally filtered by source object or source/shadow scope. Legacy table/source_table params are still accepted as fallback.
// @Tags         Mapping Rules
// @Produce      json
// @Param        source_object_id query int false "Filter by source object ID"
// @Param        source_database query string false "Filter by source database"
// @Param        source_table query string false "Filter by source object name"
// @Param        shadow_schema query string false "Filter by shadow schema"
// @Param        shadow_table query string false "Filter by shadow table"
// @Param        status query string false "Filter by status"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [get]
func (h *MappingRuleHandler) List(c *fiber.Ctx) error {
	// Source-table filter accepts three aliases for back-compat with older
	// FE clients: source_table (canonical), table (legacy), table_name.
	filterSourceTable := strings.TrimSpace(c.Query("source_table"))
	if filterSourceTable == "" {
		filterSourceTable = strings.TrimSpace(c.Query("table"))
	}
	if filterSourceTable == "" {
		filterSourceTable = strings.TrimSpace(c.Query("table_name"))
	}

	var sourceObjectID int64
	if s := strings.TrimSpace(c.Query("source_object_id")); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			sourceObjectID = v
		}
	}

	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))

	q := queries.ListMappingRulesQuery{
		Filter: mapping.Filter{
			Status:         mapping.Status(strings.TrimSpace(c.Query("status"))),
			RuleType:       mapping.RuleType(strings.TrimSpace(c.Query("rule_type"))),
			SourceObjectID: sourceObjectID,
			SourceDatabase: strings.TrimSpace(c.Query("source_database")),
			SourceTable:    filterSourceTable,
			ShadowSchema:   strings.TrimSpace(c.Query("shadow_schema")),
			ShadowTable:    strings.TrimSpace(c.Query("shadow_table")),
		},
		Page:     page,
		PageSize: pageSize,
	}

	res, err := h.listQuery.Handle(c.Context(), q)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch mapping rules: " + err.Error()})
	}

	rows := make([]MappingRuleRow, len(res.Data))
	for i, r := range res.Data {
		rows[i] = ruleToRow(r)
	}
	return c.JSON(fiber.Map{
		"data":      rows,
		"count":     len(rows),
		"total":     res.Total,
		"page":      res.Page,
		"page_size": res.PageSize,
	})
}

// Create godoc
// @Summary      Create a mapping rule
// @Description  Creates a V2 mapping rule in cdc_system.mapping_rule_v2. Supports source_object_id or source/shadow scope resolution; legacy source_table is accepted as fallback.
// @Tags         Mapping Rules
// @Accept       json
// @Produce      json
// @Param        body body mappingRuleCreateRequest true "Mapping rule details"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules [post]
func (h *MappingRuleHandler) Create(c *fiber.Ctx) error {
	var req mappingRuleCreateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	req.SourceTable = strings.TrimSpace(req.SourceTable)
	req.SourceField = strings.TrimSpace(req.SourceField)
	req.TargetColumn = strings.TrimSpace(req.TargetColumn)
	req.DataType = strings.TrimSpace(req.DataType)
	req.SourceFormat = strings.TrimSpace(req.SourceFormat)
	req.Status = strings.TrimSpace(req.Status)

	if req.SourceField == "" || req.TargetColumn == "" || req.DataType == "" {
		return c.Status(400).JSON(fiber.Map{"error": "source_field, target_column, data_type are required"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	username := middleware.GetUsername(c)
	cmd := commands.CreateMappingRuleCommand{
		SourceObjectID:  req.SourceObjectID,
		MasterBindingID: req.MasterBindingID,
		SourceDatabase:  req.SourceDatabase,
		SourceSchema:    req.SourceSchema,
		SourceNamespace: req.SourceNamespace,
		SourceTable:     req.SourceTable,
		ShadowSchema:    req.ShadowSchema,
		ShadowTable:     req.ShadowTable,
		SourceField:     req.SourceField,
		SourcePath:      req.SourcePath,
		TargetColumn:    req.TargetColumn,
		DataType:        req.DataType,
		SourceFormat:    req.SourceFormat,
		TransformFn:     req.TransformFn,
		IsNullable:      req.IsNullable,
		IsActive:        req.IsActive,
		Status:          req.Status,
		Notes:           req.Notes,
		UpdatedBy:       username,
	}
	ctx := messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMappingScopeNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "mapping_scope_not_found"})
		case errors.Is(err, commands.ErrMappingScopeAmbiguous):
			return c.Status(409).JSON(fiber.Map{"error": "ambiguous_mapping_scope"})
		case errors.Is(err, commands.ErrMappingRuleAlreadyExists):
			return c.Status(409).JSON(fiber.Map{"error": "mapping_rule_already_exists"})
		case strings.Contains(err.Error(), "required"):
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "failed to create mapping rule: " + err.Error()})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(201).Send(res.ResultBody)
}

// Reload godoc
// @Summary      Reload mapping rules for workers
// @Description  Publishes a NATS message to trigger workers to reload mapping rules from DB. Accepts source_object_id or source/shadow scope; legacy table params are supported as fallback.
// @Tags         Mapping Rules
// @Produce      json
// @Param        source_object_id query int false "Specific source object to reload"
// @Param        source_database query string false "Source database"
// @Param        source_table query string false "Source table"
// @Param        shadow_schema query string false "Shadow schema"
// @Param        shadow_table query string false "Shadow table"
// @Success      200 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules/reload [post]
func (h *MappingRuleHandler) Reload(c *fiber.Ctx) error {
	sourceObjectIDStr := strings.TrimSpace(c.Query("source_object_id"))
	var sourceObjectID *int64
	if sourceObjectIDStr != "" {
		if parsed, err := strconv.ParseInt(sourceObjectIDStr, 10, 64); err == nil {
			sourceObjectID = &parsed
		}
	}
	sourceDatabase := ptrTrim(func() *string { s := c.Query("source_database"); return &s }())
	sourceSchema := ptrTrim(func() *string { s := c.Query("source_schema"); return &s }())
	sourceNamespace := ptrTrim(func() *string { s := c.Query("source_namespace"); return &s }())
	sourceTable := strings.TrimSpace(c.Query("source_table"))
	if sourceTable == "" {
		sourceTable = strings.TrimSpace(c.Query("table"))
	}
	shadowSchema := ptrTrim(func() *string { s := c.Query("shadow_schema"); return &s }())
	shadowTable := ptrTrim(func() *string { s := c.Query("shadow_table"); return &s }())

	target := "*"
	if sourceObjectID != nil || sourceDatabase != nil || sourceSchema != nil || sourceNamespace != nil || sourceTable != "" || shadowSchema != nil || shadowTable != nil {
		scope, err := h.resolveScope(c, sourceObjectID, sourceDatabase, sourceSchema, sourceNamespace, sourceTable, shadowSchema, shadowTable)
		if err != nil {
			switch err.Error() {
			case "ambiguous_mapping_scope":
				return c.Status(409).JSON(fiber.Map{"error": "ambiguous_mapping_scope"})
			default:
				if err == gorm.ErrRecordNotFound {
					return c.Status(404).JSON(fiber.Map{"error": "mapping_scope_not_found"})
				}
				return c.Status(500).JSON(fiber.Map{"error": "failed to resolve mapping scope: " + err.Error()})
			}
		}
		target = scope.ShadowTable
	}

	if err := h.natsClient.PublishReload(target, middleware.GetUsername(c), "reload_mapping", ""); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish reload event"})
	}

	return c.JSON(fiber.Map{"message": "reload signal sent successfully", "target_table": target})
}

// UpdateStatus godoc
// @Summary      Update mapping rule status
// @Description  Updates the status of a V2 mapping rule and publishes reload for the resolved shadow target.
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
	status := strings.TrimSpace(body.Status)
	if status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "status is required"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	username := middleware.GetUsername(c)
	cmd := commands.UpdateMappingRuleCommand{
		ID:        int64(id),
		Status:    status,
		UpdatedBy: username,
	}
	ctx := messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrMappingRuleNotFound):
			return c.Status(404).JSON(fiber.Map{"error": err.Error()})
		case strings.Contains(err.Error(), "required"):
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "failed to update mapping rule: " + err.Error()})
		}
	}
	c.Set("Content-Type", "application/json")
	return c.Status(200).Send(res.ResultBody)
}

// Backfill godoc
// @Summary      Backfill data for a mapping rule
// @Description  Populates the target column from shadow raw payload for existing rows where target column is NULL.
// @Tags         Mapping Rules
// @Produce      json
// @Param        id   path int true "Mapping Rule ID"
// @Success      202 {object} map[string]interface{}
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/mapping-rules/{id}/backfill [post]
func (h *MappingRuleHandler) Backfill(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	rule, err := h.getRuleByID(c, int64(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "mapping rule not found"})
	}
	if rule.ShadowTable == nil || strings.TrimSpace(*rule.ShadowTable) == "" {
		return c.Status(404).JSON(fiber.Map{"error": "shadow_target_not_found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.BackfillCommand{
		TargetTable:  *rule.ShadowTable,
		SourceField:  rule.SourceField,
		TargetColumn: rule.TargetColumn,
		DataType:     rule.DataType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch backfill command: " + derr.Error()})
	}

	return c.Status(202).JSON(fiber.Map{
		"message":       "backfill command accepted",
		"target_table":  *rule.ShadowTable,
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
	body.Status = strings.TrimSpace(body.Status)
	if len(body.IDs) == 0 || body.Status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ids and status required"})
	}

	username := middleware.GetUsername(c)
	updated := 0
	dispatched := 0
	backfilled := 0

	for _, id := range body.IDs {
		rule, err := h.getRuleByID(c, int64(id))
		if err != nil {
			continue
		}
		updates := map[string]interface{}{"status": body.Status, "updated_by": username}
		if body.Status == "rejected" {
			updates["is_active"] = false
		}
		if body.Status == "approved" {
			updates["is_active"] = true
		}
		if err := h.db.WithContext(c.Context()).Table("cdc_system.mapping_rule_v2").Where("id = ?", id).Updates(updates).Error; err != nil {
			continue
		}
		updated++

		if body.Status == "approved" && rule.ShadowTable != nil {
			ctx := messaging.WithMetadata(c.UserContext(), username, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
			alterCmd := commands.AlterColumnCommand{
				TargetTable: *rule.ShadowTable,
				ColumnName:  rule.TargetColumn,
				DataType:    rule.DataType,
				Action:      "add",
			}
			if _, derr := h.bus.Dispatch(ctx, alterCmd); derr == nil {
				dispatched++
			}

			if body.AutoBackfill {
				bfCmd := commands.BackfillCommand{
					TargetTable:  *rule.ShadowTable,
					SourceField:  rule.SourceField,
					TargetColumn: rule.TargetColumn,
					DataType:     rule.DataType,
				}
				if _, derr := h.bus.Dispatch(ctx, bfCmd); derr == nil {
					backfilled++
				}
			}

			h.natsClient.PublishReload(*rule.ShadowTable, username, "batch_update", "")
		}
	}

	if body.Status != "approved" {
		h.natsClient.PublishReload("*", username, "batch_update", "")
	}

	return c.Status(202).JSON(fiber.Map{
		"message":    "batch update accepted — alter-column dispatched per approved rule",
		"updated":    updated,
		"dispatched": dispatched,
		"backfilled": backfilled,
		"total":      len(body.IDs),
	})
}
