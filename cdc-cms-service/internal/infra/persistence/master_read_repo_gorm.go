// Package persistence — GORM concrete adapters for ports.* interfaces
// and the in-package read ports declared in `internal/app/queries/`.
//
// This file implements queries.MasterReader against the live
// `cdc_system.master_binding` table joined with `shadow_binding`,
// `source_object_registry`, and `connection_registry`. The SQL is
// lifted verbatim from `internal/api/master_registry_handler.go::List`.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/queries"

	"gorm.io/gorm"
)

type masterReadRepoGorm struct {
	db *gorm.DB
}

// NewMasterReadRepo constructs the GORM-backed adapter for
// queries.MasterReader.
func NewMasterReadRepo(db *gorm.DB) queries.MasterReader {
	return &masterReadRepoGorm{db: db}
}

func (r *masterReadRepoGorm) ListEnriched(ctx context.Context) ([]queries.MasterListItem, error) {
	const q = `
		SELECT
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
		ORDER BY mb.master_schema, mb.master_table
	`
	var rows []queries.MasterListItem
	if err := r.db.WithContext(ctx).Raw(q).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}
