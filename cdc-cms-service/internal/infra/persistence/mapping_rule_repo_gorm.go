// Package persistence — GORM concrete adapters for ports.* interfaces.
//
// This file implements ports.MappingRuleRepo against the live
// cdc_system.mapping_rule_v2 table with the JOINed source_object_registry
// + shadow_binding context that the existing /api/mapping-rules handler
// returns to the FE.
package persistence

import (
	"context"
	"errors"
	"strings"
	"time"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/mapping"

	"gorm.io/gorm"
)

type mappingRuleRepoGorm struct {
	db *gorm.DB
}

// NewMappingRuleRepo constructs the GORM-backed adapter for
// ports.MappingRuleRepo. Phase 2 v2 / P2: implements ListPaginated only;
// the remaining methods stay stubbed and will be filled in P4 once the
// write-side handlers move into app/commands/.
func NewMappingRuleRepo(db *gorm.DB) ports.MappingRuleRepo {
	return &mappingRuleRepoGorm{db: db}
}

// mappingRuleRow is the flat scan target — Lesson #1253: never scan into
// the domain entity directly because GORM's column → field resolver does
// not respect pointer wrapping or JOIN aliases on associations.
type mappingRuleRow struct {
	ID              int64      `gorm:"column:id"`
	SourceObjectID  int64      `gorm:"column:source_object_id"`
	MasterBindingID *int64     `gorm:"column:master_binding_id"`
	SourceDatabase  *string    `gorm:"column:source_database"`
	SourceSchema    *string    `gorm:"column:source_schema"`
	SourceNamespace *string    `gorm:"column:source_namespace"`
	SourceTable     string     `gorm:"column:source_table"`
	ShadowSchema    *string    `gorm:"column:shadow_schema"`
	ShadowTable     *string    `gorm:"column:shadow_table"`
	SourceField     string     `gorm:"column:source_field"`
	SourcePath      *string    `gorm:"column:source_path"`
	TargetColumn    string     `gorm:"column:target_column"`
	DataType        string     `gorm:"column:data_type"`
	SourceFormat    string     `gorm:"column:source_format"`
	TransformFn     *string    `gorm:"column:transform_fn"`
	IsNullable      bool       `gorm:"column:is_nullable"`
	IsActive        bool       `gorm:"column:is_active"`
	Status          string     `gorm:"column:status"`
	Notes           *string    `gorm:"column:notes"`
	CreatedBy       *string    `gorm:"column:created_by"`
	UpdatedBy       *string    `gorm:"column:updated_by"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	UpdatedAt       time.Time  `gorm:"column:updated_at"`
}

func (r mappingRuleRow) toDomain() mapping.Rule {
	return mapping.Rule{
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
		Status:          mapping.Status(r.Status),
		RuleType:        mapping.RuleTypeMapping,
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       r.UpdatedAt,
		CreatedBy:       r.CreatedBy,
		UpdatedBy:       r.UpdatedBy,
		Notes:           r.Notes,
	}
}

const baseSelect = `
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
		mr.created_at,
		mr.updated_at
	FROM cdc_system.mapping_rule_v2 mr
	JOIN cdc_system.source_object_registry so
	  ON so.id = mr.source_object_id
	LEFT JOIN cdc_system.shadow_binding sb
	  ON sb.source_object_id = mr.source_object_id
	 AND sb.is_active = TRUE
	WHERE 1=1
`

// buildFilter appends WHERE clauses + args to the running query and
// returns the augmented (query, args) pair. Caller appends ORDER BY +
// pagination after.
func buildFilter(q string, args []interface{}, f mapping.Filter) (string, []interface{}) {
	if f.SourceObjectID > 0 {
		q += ` AND mr.source_object_id = ?`
		args = append(args, f.SourceObjectID)
	}
	if s := strings.TrimSpace(f.SourceDatabase); s != "" {
		q += ` AND so.source_database = ?`
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.SourceTable); s != "" {
		q += ` AND so.source_object_name = ?`
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.ShadowSchema); s != "" {
		q += ` AND sb.shadow_schema = ?`
		args = append(args, s)
	}
	if s := strings.TrimSpace(f.ShadowTable); s != "" {
		q += ` AND sb.shadow_table = ?`
		args = append(args, s)
	}
	if f.Status != "" {
		q += ` AND mr.status = ?`
		args = append(args, string(f.Status))
	}
	// rule_type discriminator: only "mapping" matches the V2 table — any
	// other value forces a no-result query (preserves V1 handler semantics).
	if f.RuleType != "" && f.RuleType != mapping.RuleTypeMapping {
		q += ` AND 1=0`
	}
	if f.IsActive != nil {
		q += ` AND mr.is_active = ?`
		args = append(args, *f.IsActive)
	}
	return q, args
}

func (r *mappingRuleRepoGorm) List(ctx context.Context, f mapping.Filter) ([]mapping.Rule, error) {
	q := baseSelect
	args := make([]interface{}, 0, 8)
	q, args = buildFilter(q, args, f)
	q += ` ORDER BY so.source_object_name, mr.source_field`

	var rows []mappingRuleRow
	if err := r.db.WithContext(ctx).Raw(q, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]mapping.Rule, len(rows))
	for i, row := range rows {
		out[i] = row.toDomain()
	}
	return out, nil
}

func (r *mappingRuleRepoGorm) ListPaginated(ctx context.Context, f mapping.Filter, page, pageSize int) ([]mapping.Rule, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 200 {
		pageSize = 50
	}

	q := baseSelect
	args := make([]interface{}, 0, 8)
	q, args = buildFilter(q, args, f)

	// COUNT(*) over the filtered set — wrap inner SELECT to preserve all
	// JOIN context exactly as the data query sees it.
	countQ := `SELECT COUNT(*) FROM (` + q + `) AS mapping_rules_filtered`
	var total int64
	if err := r.db.WithContext(ctx).Raw(countQ, args...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	q += ` ORDER BY so.source_object_name, mr.source_field OFFSET ? LIMIT ?`
	args = append(args, (page-1)*pageSize, pageSize)

	var rows []mappingRuleRow
	if err := r.db.WithContext(ctx).Raw(q, args...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}
	out := make([]mapping.Rule, len(rows))
	for i, row := range rows {
		out[i] = row.toDomain()
	}
	return out, total, nil
}

// Phase 2 v2 / P4 — write-side methods land when api/mapping_rule_handler.Create
// + UpdateStatus + BatchUpdate move into app/commands/.
var errNotImplementedP4 = errors.New("mapping_rule_repo_gorm: write methods not implemented in P2 demo — see Phase 2 v2 / P4")

func (r *mappingRuleRepoGorm) GetByID(ctx context.Context, id int64) (*mapping.Rule, error) {
	return nil, errNotImplementedP4
}

func (r *mappingRuleRepoGorm) Save(ctx context.Context, _ *mapping.Rule) error {
	return errNotImplementedP4
}

func (r *mappingRuleRepoGorm) UpdateStatus(ctx context.Context, _ int64, _ mapping.Status) error {
	return errNotImplementedP4
}

func (r *mappingRuleRepoGorm) BatchUpdateStatus(ctx context.Context, _ []int64, _ mapping.Status) (int64, error) {
	return 0, errNotImplementedP4
}
