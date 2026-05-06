package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
)

// CreateMasterCommand is POST /api/v1/masters expressed as a sync
// command. Resolves shadow_binding + master_connection then INSERTs
// into `cdc_system.master_binding`. All in-process — no NATS RPC.
type CreateMasterCommand struct {
	ports.SyncCommandMixin
	MasterName           string          `json:"master_name"`
	MasterSchema         string          `json:"master_schema"`
	MasterConnectionCode string          `json:"master_connection_code,omitempty"`
	SourceShadow         string          `json:"source_shadow,omitempty"`
	SourceDatabase       string          `json:"source_database,omitempty"`
	SourceSchema         string          `json:"source_schema,omitempty"`
	SourceNamespace      string          `json:"source_namespace,omitempty"`
	SourceTable          string          `json:"source_table,omitempty"`
	ShadowSchema         string          `json:"shadow_schema,omitempty"`
	ShadowTable          string          `json:"shadow_table,omitempty"`
	TransformType        string          `json:"transform_type"`
	Spec                 json.RawMessage `json:"spec,omitempty"`
	Reason               string          `json:"reason"`
	UpdatedBy            string          `json:"updated_by"`
}

func (CreateMasterCommand) Type() string { return "master.create" }

var (
	cmdMasterNameRe  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	cmdNamespaceName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

	cmdValidTransformType = map[string]bool{
		"copy_1_to_1": true, "filter": true, "aggregate": true,
		"group_by": true, "join": true, "custom_sql": true,
	}
)

func (c CreateMasterCommand) Validate() error {
	if !cmdMasterNameRe.MatchString(c.MasterName) {
		return errors.New("invalid_master_name")
	}
	if c.MasterSchema != "" && !cmdNamespaceName.MatchString(c.MasterSchema) {
		return errors.New("invalid_master_schema")
	}
	if !cmdValidTransformType[c.TransformType] {
		return errors.New("invalid_transform_type")
	}
	if len(strings.TrimSpace(c.Reason)) < 10 {
		return errors.New("reason_required_min_10_chars")
	}
	return nil
}

var (
	ErrShadowBindingNotFound      = errors.New("shadow_binding_not_found")
	ErrShadowBindingAmbiguous     = errors.New("ambiguous_shadow_binding")
	ErrMasterConnectionNotFound   = errors.New("master_connection_not_found")
	ErrMasterConnectionAmbiguous  = errors.New("ambiguous_master_connection")
	ErrMasterAlreadyExists        = errors.New("master_already_exists")
)

type CreateMasterHandler struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewCreateMasterHandler(db *gorm.DB, logger *zap.Logger) *CreateMasterHandler {
	return &CreateMasterHandler{db: db, logger: logger}
}

type masterConnectionDTO struct {
	ID              int64
	ConnectionCode  string
	DefaultDatabase *string
	DefaultSchema   *string
}

type shadowBindingDTO struct {
	ShadowBindingID int64
	SourceObjectID  int64
	SourceDatabase  *string
	SourceSchema    *string
	SourceNamespace *string
	SourceTable     *string
	ShadowSchema    string
	ShadowTable     string
}

func (h *CreateMasterHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateMasterCommand)
	if !ok {
		return nil, errors.New("master.create: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("master store not ready")
	}

	masterSchema := strings.TrimSpace(cmd.MasterSchema)
	if masterSchema == "" {
		masterSchema = "public"
	}
	spec := cmd.Spec
	if len(spec) == 0 {
		spec = json.RawMessage("{}")
	}

	shadow, err := h.resolveShadowBinding(ctx, cmd)
	if err != nil {
		return nil, err
	}
	masterConn, err := h.resolveMasterConnection(ctx, cmd)
	if err != nil {
		return nil, err
	}

	if masterSchema == "public" && masterConn.DefaultSchema != nil && strings.TrimSpace(*masterConn.DefaultSchema) != "" {
		masterSchema = strings.TrimSpace(*masterConn.DefaultSchema)
	}

	bindingCode := normalizeBindingCodeCmd("mb", masterSchema, cmd.MasterName, fmt.Sprintf("%d", time.Now().UTC().Unix()))
	physicalTableFQN := masterSchema + "." + cmd.MasterName

	if err := h.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.master_binding
		   (binding_code, source_object_id, shadow_binding_id, master_connection_id,
		    master_database, master_schema, master_table, physical_table_fqn,
		    transform_type, transform_spec, schema_status, is_active, created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, 'pending_review', false, ?, NOW(), NOW())`,
		bindingCode,
		shadow.SourceObjectID,
		shadow.ShadowBindingID,
		masterConn.ID,
		masterConn.DefaultDatabase,
		masterSchema,
		cmd.MasterName,
		physicalTableFQN,
		cmd.TransformType,
		string(spec),
		cmd.UpdatedBy,
	).Error; err != nil {
		msg := err.Error()
		if strings.Contains(msg, "unique") || strings.Contains(msg, "duplicate") {
			return nil, ErrMasterAlreadyExists
		}
		return nil, fmt.Errorf("failed to insert master binding: %w", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"master_name":            cmd.MasterName,
		"master_schema":          masterSchema,
		"master_connection_code": masterConn.ConnectionCode,
		"shadow_schema":          shadow.ShadowSchema,
		"shadow_table":           shadow.ShadowTable,
		"schema_status":          "pending_review",
		"next":                   "POST /api/v1/masters/" + cmd.MasterName + "/approve",
	})
	return body, nil
}

func (h *CreateMasterHandler) resolveMasterConnection(ctx context.Context, cmd CreateMasterCommand) (*masterConnectionDTO, error) {
	code := strings.TrimSpace(cmd.MasterConnectionCode)
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

	var rows []masterConnectionDTO
	if err := h.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrMasterConnectionNotFound
	}
	if len(rows) > 1 && code == "" {
		return nil, ErrMasterConnectionAmbiguous
	}
	return &rows[0], nil
}

func (h *CreateMasterHandler) resolveShadowBinding(ctx context.Context, cmd CreateMasterCommand) (*shadowBindingDTO, error) {
	shadowSchema := strings.TrimSpace(cmd.ShadowSchema)
	shadowTable := strings.TrimSpace(cmd.ShadowTable)
	sourceDatabase := strings.TrimSpace(cmd.SourceDatabase)
	sourceSchema := strings.TrimSpace(cmd.SourceSchema)
	sourceNamespace := strings.TrimSpace(cmd.SourceNamespace)
	sourceTable := strings.TrimSpace(cmd.SourceTable)
	sourceShadow := strings.TrimSpace(cmd.SourceShadow)

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

	var rows []shadowBindingDTO
	if err := h.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrShadowBindingNotFound
	}
	if len(rows) > 1 {
		return nil, ErrShadowBindingAmbiguous
	}
	return &rows[0], nil
}

func normalizeBindingCodeCmd(parts ...string) string {
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
