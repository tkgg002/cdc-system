package api

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	db           *gorm.DB
}

func NewMappingRuleHandler(repo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, nats *natsconn.NatsClient, db ...*gorm.DB) *MappingRuleHandler {
	h := &MappingRuleHandler{repo: repo, registryRepo: registryRepo, natsClient: nats}
	if len(db) > 0 {
		h.db = db[0]
	}
	return h
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
	status := c.Query("status")
	ruleType := c.Query("rule_type")
	sourceTable := c.Query("source_table")
	legacyTable := c.Query("table")
	tableName := c.Query("table_name")
	sourceDatabase := ptrTrim(func() *string { s := c.Query("source_database"); return &s }())
	shadowSchema := ptrTrim(func() *string { s := c.Query("shadow_schema"); return &s }())
	shadowTable := ptrTrim(func() *string { s := c.Query("shadow_table"); return &s }())
	sourceObjectIDStr := strings.TrimSpace(c.Query("source_object_id"))

	filterSourceTable := strings.TrimSpace(sourceTable)
	if filterSourceTable == "" {
		filterSourceTable = strings.TrimSpace(legacyTable)
	}
	if filterSourceTable == "" {
		filterSourceTable = strings.TrimSpace(tableName)
	}

	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "50"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	query := `
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
			TO_CHAR(mr.created_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS created_at,
			TO_CHAR(mr.updated_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS updated_at,
			'mapping' AS rule_type,
			false AS is_enriched
		FROM cdc_system.mapping_rule_v2 mr
		JOIN cdc_system.source_object_registry so
		  ON so.id = mr.source_object_id
		LEFT JOIN cdc_system.shadow_binding sb
		  ON sb.source_object_id = mr.source_object_id
		 AND sb.is_active = TRUE
		WHERE 1=1
	`
	args := make([]interface{}, 0, 8)
	if sourceObjectIDStr != "" {
		query += ` AND mr.source_object_id = ?`
		args = append(args, sourceObjectIDStr)
	}
	if sourceDatabase != nil {
		query += ` AND so.source_database = ?`
		args = append(args, *sourceDatabase)
	}
	if filterSourceTable != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, filterSourceTable)
	}
	if shadowSchema != nil {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, *shadowSchema)
	}
	if shadowTable != nil {
		query += ` AND sb.shadow_table = ?`
		args = append(args, *shadowTable)
	}
	if status != "" {
		query += ` AND mr.status = ?`
		args = append(args, status)
	}
	if ruleType != "" && ruleType != "mapping" {
		query += ` AND 1=0`
	}

	countQuery := `SELECT COUNT(*) FROM (` + query + `) AS mapping_rules`
	var total int64
	if err := h.db.WithContext(c.Context()).Raw(countQuery, args...).Scan(&total).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to count mapping rules: " + err.Error()})
	}

	query += ` ORDER BY so.source_object_name, mr.source_field OFFSET ? LIMIT ?`
	args = append(args, (page-1)*pageSize, pageSize)

	var rules []MappingRuleRow
	if err := h.db.WithContext(c.Context()).Raw(query, args...).Scan(&rules).Error; err != nil {
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
	if req.SourceFormat == "" {
		req.SourceFormat = "raw"
	}
	if req.Status == "" {
		req.Status = "pending"
	}

	scope, err := h.resolveScope(c, req.SourceObjectID, req.SourceDatabase, req.SourceSchema, req.SourceNamespace, req.SourceTable, req.ShadowSchema, req.ShadowTable)
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

	username := middleware.GetUsername(c)
	isNullable := boolDefault(req.IsNullable, true)
	isActive := boolDefault(req.IsActive, true)
	var insertedID int64
	err = h.db.WithContext(c.Context()).Raw(`
		INSERT INTO cdc_system.mapping_rule_v2 (
			source_object_id,
			master_binding_id,
			source_field,
			source_path,
			target_column,
			data_type,
			source_format,
			transform_fn,
			is_nullable,
			is_active,
			status,
			notes,
			created_by,
			updated_by,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
		RETURNING id
	`,
		scope.SourceObjectID,
		req.MasterBindingID,
		req.SourceField,
		ptrTrim(req.SourcePath),
		req.TargetColumn,
		req.DataType,
		req.SourceFormat,
		ptrTrim(req.TransformFn),
		isNullable,
		isActive,
		req.Status,
		ptrTrim(req.Notes),
		username,
		username,
	).Scan(&insertedID).Error
	if err != nil {
		if strings.Contains(err.Error(), "ux_v2_mapping_rule_identity") || strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			return c.Status(409).JSON(fiber.Map{"error": "mapping_rule_already_exists"})
		}
		return c.Status(500).JSON(fiber.Map{"error": "failed to create mapping rule: " + err.Error()})
	}

	row, err := h.getRuleByID(c, insertedID)
	if err != nil {
		return c.Status(201).JSON(fiber.Map{"message": "mapping rule created", "id": insertedID})
	}
	return c.Status(201).JSON(fiber.Map{"message": "mapping rule created", "data": row})
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
	body.Status = strings.TrimSpace(body.Status)
	if body.Status == "" {
		return c.Status(400).JSON(fiber.Map{"error": "status is required"})
	}

	rule, err := h.getRuleByID(c, int64(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "mapping rule not found"})
	}

	username := middleware.GetUsername(c)
	updates := map[string]interface{}{"status": body.Status, "updated_by": username}
	if body.Status == "rejected" {
		updates["is_active"] = false
	}
	if body.Status == "approved" {
		updates["is_active"] = true
	}
	if err := h.db.WithContext(c.Context()).Table("cdc_system.mapping_rule_v2").Where("id = ?", id).Updates(updates).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to update mapping rule: " + err.Error()})
	}

	if rule.ShadowTable != nil {
		h.natsClient.PublishReload(*rule.ShadowTable, middleware.GetUsername(c), "mapping_status_update", "")
	}

	return c.JSON(fiber.Map{"message": "mapping rule updated", "id": id, "status": body.Status})
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

	payload, _ := json.Marshal(map[string]interface{}{
		"target_table":  *rule.ShadowTable,
		"source_field":  rule.SourceField,
		"target_column": rule.TargetColumn,
		"data_type":     rule.DataType,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.backfill", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch backfill command: " + err.Error()})
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
			alterPayload, _ := json.Marshal(map[string]interface{}{
				"target_table": *rule.ShadowTable,
				"column_name":  rule.TargetColumn,
				"data_type":    rule.DataType,
				"action":       "add",
			})
			if err := h.natsClient.Conn.Publish("cdc.cmd.alter-column", alterPayload); err == nil {
				dispatched++
			}

			if body.AutoBackfill {
				payload, _ := json.Marshal(map[string]interface{}{
					"target_table":  *rule.ShadowTable,
					"source_field":  rule.SourceField,
					"target_column": rule.TargetColumn,
					"data_type":     rule.DataType,
				})
				if err := h.natsClient.Conn.Publish("cdc.cmd.backfill", payload); err == nil {
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
