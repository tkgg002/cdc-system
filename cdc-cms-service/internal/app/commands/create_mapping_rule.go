package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// CreateMappingRuleCommand is POST /api/mapping-rules expressed as a
// sync command. Scope resolution + INSERT + re-fetch all run in-process
// against `cdc_system.mapping_rule_v2` — no NATS RPC.
type CreateMappingRuleCommand struct {
	ports.SyncCommandMixin
	SourceObjectID  *int64  `json:"source_object_id,omitempty"`
	MasterBindingID *int64  `json:"master_binding_id,omitempty"`
	SourceDatabase  *string `json:"source_database,omitempty"`
	SourceSchema    *string `json:"source_schema,omitempty"`
	SourceNamespace *string `json:"source_namespace,omitempty"`
	SourceTable     string  `json:"source_table,omitempty"`
	ShadowSchema    *string `json:"shadow_schema,omitempty"`
	ShadowTable     *string `json:"shadow_table,omitempty"`
	SourceField     string  `json:"source_field"`
	SourcePath      *string `json:"source_path,omitempty"`
	TargetColumn    string  `json:"target_column"`
	DataType        string  `json:"data_type"`
	SourceFormat    string  `json:"source_format,omitempty"`
	TransformFn     *string `json:"transform_fn,omitempty"`
	IsNullable      *bool   `json:"is_nullable,omitempty"`
	IsActive        *bool   `json:"is_active,omitempty"`
	Status          string  `json:"status,omitempty"`
	Notes           *string `json:"notes,omitempty"`
	UpdatedBy       string  `json:"updated_by"`
}

func (CreateMappingRuleCommand) Type() string { return "mapping.create" }

func (c CreateMappingRuleCommand) Validate() error {
	if strings.TrimSpace(c.SourceField) == "" {
		return errors.New("source_field, target_column, data_type are required")
	}
	if strings.TrimSpace(c.TargetColumn) == "" {
		return errors.New("source_field, target_column, data_type are required")
	}
	if strings.TrimSpace(c.DataType) == "" {
		return errors.New("source_field, target_column, data_type are required")
	}
	return nil
}

// Sentinel errors so the API layer can map to status codes without
// string-matching error text.
var (
	ErrMappingScopeNotFound     = errors.New("mapping_scope_not_found")
	ErrMappingScopeAmbiguous    = errors.New("ambiguous_mapping_scope")
	ErrMappingRuleAlreadyExists = errors.New("mapping_rule_already_exists")
)

type CreateMappingRuleHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewCreateMappingRuleHandler(db *gorm.DB, logger *zap.Logger) *CreateMappingRuleHandler {
	return &CreateMappingRuleHandler{db: db, logger: logger}
}

// Internal projection of the post-insert SELECT. Mirrors `MappingRuleRow`
// in the API package field-for-field so the API can stream the response
// body straight to the client without re-shaping.
type mappingRuleRowDTO struct {
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

type mappingRuleScopeDTO struct {
	SourceObjectID int64
	SourceTable    string
	ShadowTable    string
}

func (h *CreateMappingRuleHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateMappingRuleCommand)
	if !ok {
		return nil, errors.New("mapping.create: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("mapping rule store not ready")
	}

	scope, err := h.resolveScope(ctx, &cmd)
	if err != nil {
		return nil, err
	}

	sourceFormat := strings.TrimSpace(cmd.SourceFormat)
	if sourceFormat == "" {
		sourceFormat = "raw"
	}
	status := strings.TrimSpace(cmd.Status)
	if status == "" {
		status = "pending"
	}
	isNullable := boolPtrDefault(cmd.IsNullable, true)
	isActive := boolPtrDefault(cmd.IsActive, true)

	var insertedID int64
	insertErr := h.db.WithContext(ctx).Raw(`
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
		cmd.MasterBindingID,
		strings.TrimSpace(cmd.SourceField),
		ptrTrimCmd(cmd.SourcePath),
		strings.TrimSpace(cmd.TargetColumn),
		strings.TrimSpace(cmd.DataType),
		sourceFormat,
		ptrTrimCmd(cmd.TransformFn),
		isNullable,
		isActive,
		status,
		ptrTrimCmd(cmd.Notes),
		cmd.UpdatedBy,
		cmd.UpdatedBy,
	).Scan(&insertedID).Error
	if insertErr != nil {
		msg := insertErr.Error()
		if strings.Contains(msg, "ux_v2_mapping_rule_identity") || strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique") {
			return nil, ErrMappingRuleAlreadyExists
		}
		return nil, fmt.Errorf("failed to create mapping rule: %w", insertErr)
	}

	row, ferr := h.fetchRule(ctx, insertedID)
	if ferr != nil {
		// Insert succeeded but re-read failed — return id-only success
		// so the FE still gets a 201 with the new ID.
		body, _ := json.Marshal(map[string]interface{}{
			"message": "mapping rule created",
			"id":      insertedID,
		})
		return body, nil
	}
	body, _ := json.Marshal(map[string]interface{}{
		"message": "mapping rule created",
		"data":    row,
	})
	return body, nil
}

func (h *CreateMappingRuleHandler) resolveScope(ctx context.Context, cmd *CreateMappingRuleCommand) (*mappingRuleScopeDTO, error) {
	if cmd.SourceObjectID != nil && *cmd.SourceObjectID > 0 {
		var row mappingRuleScopeDTO
		err := h.db.WithContext(ctx).Raw(`
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
		`, *cmd.SourceObjectID).Scan(&row).Error
		if err != nil {
			return nil, err
		}
		if row.SourceObjectID == 0 {
			return nil, ErrMappingScopeNotFound
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
	if trimmed := ptrTrimCmd(cmd.SourceDatabase); trimmed != nil {
		query += ` AND so.source_database = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrimCmd(cmd.SourceSchema); trimmed != nil {
		query += ` AND so.source_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrimCmd(cmd.SourceNamespace); trimmed != nil {
		query += ` AND so.source_namespace = ?`
		args = append(args, *trimmed)
	}
	if strings.TrimSpace(cmd.SourceTable) != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, strings.TrimSpace(cmd.SourceTable))
	}
	if trimmed := ptrTrimCmd(cmd.ShadowSchema); trimmed != nil {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrimCmd(cmd.ShadowTable); trimmed != nil {
		query += ` AND sb.shadow_table = ?`
		args = append(args, *trimmed)
	}
	query += ` ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST LIMIT 2`

	var rows []mappingRuleScopeDTO
	if err := h.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrMappingScopeNotFound
	}
	if len(rows) > 1 {
		return nil, ErrMappingScopeAmbiguous
	}
	return &rows[0], nil
}

func (h *CreateMappingRuleHandler) fetchRule(ctx context.Context, id int64) (*mappingRuleRowDTO, error) {
	var rows []mappingRuleRowDTO
	err := h.db.WithContext(ctx).Raw(`
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

func ptrTrimCmd(v *string) *string {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(*v)
	if s == "" {
		return nil
	}
	return &s
}

func boolPtrDefault(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}
