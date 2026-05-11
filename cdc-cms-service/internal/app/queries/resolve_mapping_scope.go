package queries

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// MappingRuleScope represents the resolved target scope.
type MappingRuleScope struct {
	SourceObjectID int64
	SourceTable    string
	ShadowTable    string
}

type ResolveMappingScopeQuery struct {
	SourceObjectID  *int64
	SourceDatabase  *string
	SourceSchema    *string
	SourceNamespace *string
	SourceTable     string
	ShadowSchema    *string
	ShadowTable     *string
}

type ResolveMappingScopeHandler struct {
	db *gorm.DB
}

func NewResolveMappingScopeHandler(db *gorm.DB) *ResolveMappingScopeHandler {
	return &ResolveMappingScopeHandler{db: db}
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

func (h *ResolveMappingScopeHandler) Handle(ctx context.Context, q ResolveMappingScopeQuery) (*MappingRuleScope, error) {
	if q.SourceObjectID != nil && *q.SourceObjectID > 0 {
		var row MappingRuleScope
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
		`, *q.SourceObjectID).Scan(&row).Error
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
	if trimmed := ptrTrim(q.SourceDatabase); trimmed != nil {
		query += ` AND so.source_database = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(q.SourceSchema); trimmed != nil {
		query += ` AND so.source_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(q.SourceNamespace); trimmed != nil {
		query += ` AND so.source_namespace = ?`
		args = append(args, *trimmed)
	}
	if strings.TrimSpace(q.SourceTable) != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, strings.TrimSpace(q.SourceTable))
	}
	if trimmed := ptrTrim(q.ShadowSchema); trimmed != nil {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, *trimmed)
	}
	if trimmed := ptrTrim(q.ShadowTable); trimmed != nil {
		query += ` AND sb.shadow_table = ?`
		args = append(args, *trimmed)
	}
	query += ` ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST LIMIT 2`

	var rows []MappingRuleScope
	if err := h.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
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
