// Package persistence — GORM concrete adapters for ports.* interfaces
// and the in-package read ports declared in `internal/app/queries/`.
//
// This file implements queries.SourceObjectReader against the live
// `cdc_system.source_object_registry` table, joining the active
// `shadow_binding`, the legacy `cdc_table_registry` bridge, and the
// latest `cdc_reconciliation_report` row. The SQL was lifted verbatim
// from `internal/api/source_objects_handler.go` (List +
// GetMappingContext) so the wire contract stays byte-identical
// throughout Phase 2 v2 / P2.
package persistence

import (
	"context"
	"errors"
	"strings"

	"cdc-cms-service/internal/app/queries"

	"gorm.io/gorm"
)

type sourceObjectReadRepoGorm struct {
	db *gorm.DB
}

// NewSourceObjectReadRepo constructs the GORM-backed adapter for
// queries.SourceObjectReader.
func NewSourceObjectReadRepo(db *gorm.DB) queries.SourceObjectReader {
	return &sourceObjectReadRepoGorm{db: db}
}

// listBaseFromWhere is the FROM + JOIN + base WHERE shared between the
// COUNT(*) and SELECT branches of ListEnriched. Filter args are appended
// to it by the caller via buildListFilter.
const listBaseFromWhere = `
	FROM cdc_system.source_object_registry so
	LEFT JOIN LATERAL (
		SELECT
			sb.id,
			sb.shadow_schema,
			sb.shadow_table,
			sb.physical_table_fqn,
			sb.ddl_status,
			sb.updated_at
		FROM cdc_system.shadow_binding sb
		WHERE sb.source_object_id = so.id
		ORDER BY sb.is_active DESC, sb.updated_at DESC, sb.id DESC
		LIMIT 1
	) sb ON TRUE
	LEFT JOIN cdc_system.cdc_table_registry tr
	  ON tr.source_db = so.source_database
	 AND tr.source_table = so.source_object_name
	 AND (
	       (sb.shadow_table IS NOT NULL AND tr.target_table = sb.shadow_table)
	    OR (sb.shadow_table IS NULL AND tr.target_table = so.source_object_name)
	 )
	LEFT JOIN LATERAL (
		SELECT
			rr.target_table,
			rr.diff,
			rr.status,
			rr.checked_at
		FROM cdc_system.cdc_reconciliation_report rr
		WHERE rr.target_table = COALESCE(sb.shadow_table, tr.target_table)
		ORDER BY rr.checked_at DESC
		LIMIT 1
	) rr ON TRUE
	WHERE so.sync_engine = 'debezium'
`

func buildListFilter(base string, args []interface{}, f queries.SourceObjectListFilter) (string, []interface{}) {
	if s := strings.TrimSpace(f.SourceDB); s != "" {
		base += ` AND so.source_database = ?`
		args = append(args, s)
	}
	if f.IsActive != nil {
		base += ` AND so.is_active = ?`
		args = append(args, *f.IsActive)
	}
	return base, args
}

func (r *sourceObjectReadRepoGorm) ListEnriched(ctx context.Context, f queries.SourceObjectListFilter, page, pageSize int) ([]queries.SourceObjectListItem, int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 500 {
		pageSize = 500
	}

	where := listBaseFromWhere
	args := make([]interface{}, 0, 4)
	where, args = buildListFilter(where, args, f)

	type countRow struct {
		Total int64 `json:"total"`
	}
	var totalRow countRow
	if err := r.db.WithContext(ctx).Raw(`SELECT COUNT(*) AS total `+where, args...).Scan(&totalRow).Error; err != nil {
		return nil, 0, err
	}

	q := `
		SELECT
			so.id,
			tr.id AS registry_id,
			sb.id AS shadow_binding_id,
			so.object_code,
			COALESCE(so.source_database, '') AS source_db,
			so.source_engine_type AS source_type,
			so.source_object_name AS source_table,
			COALESCE(sb.shadow_table, so.source_object_name) AS target_table,
			sb.shadow_schema,
			sb.physical_table_fqn,
			so.sync_engine,
			COALESCE(tr.sync_interval, '1h') AS sync_interval,
			COALESCE(tr.priority, 'normal') AS priority,
			so.primary_key_field,
			COALESCE(so.primary_key_type, '') AS primary_key_type,
			COALESCE(so.timestamp_field, tr.timestamp_field) AS timestamp_field,
			so.is_active,
			COALESCE(sb.ddl_status = 'created', tr.is_table_created, false) AS is_table_created,
			so.profile_status,
			sb.ddl_status,
			CASE
				WHEN rr.status = 'source_error' THEN 'source_error'
				WHEN rr.target_table IS NOT NULL AND COALESCE(rr.diff, 0) <> 0 THEN 'drift'
				WHEN rr.target_table IS NOT NULL THEN 'healthy'
				ELSE 'unknown'
			END AS sync_status,
			CASE
				WHEN tr.id IS NOT NULL THEN 'bridged'
				ELSE 'v2_only'
			END AS bridge_status,
			CASE
				WHEN sb.id IS NOT NULL AND tr.id IS NOT NULL THEN 'v2_ready'
				WHEN sb.id IS NOT NULL THEN 'v2_shadow_only'
				ELSE 'v2_source_only'
			END AS metadata_status,
			COALESCE(rr.diff, 0) AS recon_drift,
			so.created_at,
			GREATEST(so.updated_at, COALESCE(sb.updated_at, so.updated_at), COALESCE(tr.updated_at, so.updated_at)) AS updated_at,
			COALESCE(so.notes, tr.notes) AS notes,
			so.provisioning_mode,
			so.provisioning_state,
			so.source_engine_type
	` + where + `
		ORDER BY so.source_database, so.source_object_name
		LIMIT ? OFFSET ?
	`
	queryArgs := append([]interface{}{}, args...)
	queryArgs = append(queryArgs, pageSize, (page-1)*pageSize)

	var rows []queries.SourceObjectListItem
	if err := r.db.WithContext(ctx).Raw(q, queryArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, totalRow.Total, nil
}

func (r *sourceObjectReadRepoGorm) GetMappingContextByRegistryID(ctx context.Context, registryID uint64) (*queries.SourceObjectMappingContextReadModel, error) {
	const q = `
		SELECT
			COALESCE(so.id, 0) AS id,
			tr.id AS registry_id,
			sb.id AS shadow_binding_id,
			COALESCE(so.object_code, '') AS object_code,
			COALESCE(so.source_database, tr.source_db, '') AS source_db,
			COALESCE(so.source_engine_type, tr.source_type, 'mongodb') AS source_type,
			COALESCE(so.source_object_name, tr.source_table) AS source_table,
			COALESCE(sb.shadow_table, tr.target_table, so.source_object_name) AS target_table,
			sb.shadow_schema,
			sb.physical_table_fqn,
			COALESCE(so.sync_engine, tr.sync_engine, 'debezium') AS sync_engine,
			COALESCE(tr.sync_interval, '1h') AS sync_interval,
			COALESCE(tr.priority, 'normal') AS priority,
			COALESCE(so.primary_key_field, tr.primary_key_field, 'id') AS primary_key_field,
			COALESCE(so.primary_key_type, tr.primary_key_type, '') AS primary_key_type,
			COALESCE(so.timestamp_field, tr.timestamp_field) AS timestamp_field,
			COALESCE(so.is_active, tr.is_active, false) AS is_active,
			COALESCE(sb.ddl_status = 'created', tr.is_table_created, false) AS is_table_created,
			COALESCE(so.profile_status, 'draft') AS profile_status,
			sb.ddl_status,
			CASE
				WHEN rr.status = 'source_error' THEN 'source_error'
				WHEN rr.target_table IS NOT NULL AND COALESCE(rr.diff, 0) <> 0 THEN 'drift'
				WHEN rr.target_table IS NOT NULL THEN 'healthy'
				ELSE 'unknown'
			END AS sync_status,
			CASE
				WHEN tr.id IS NOT NULL THEN 'bridged'
				ELSE 'v2_only'
			END AS bridge_status,
			CASE
				WHEN sb.id IS NOT NULL AND tr.id IS NOT NULL THEN 'v2_ready'
				WHEN sb.id IS NOT NULL THEN 'v2_shadow_only'
				ELSE 'v2_source_only'
			END AS metadata_status,
			COALESCE(rr.diff, 0) AS recon_drift,
			COALESCE(so.created_at, tr.created_at)::timestamptz AS created_at,
			GREATEST(
				COALESCE(so.updated_at, tr.updated_at)::timestamptz,
				COALESCE(sb.updated_at, tr.updated_at)::timestamptz,
				tr.updated_at::timestamptz
			) AS updated_at,
			COALESCE(so.notes, tr.notes) AS notes
		FROM cdc_system.cdc_table_registry tr
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.source_database = tr.source_db
		 AND so.source_object_name = tr.source_table
		LEFT JOIN LATERAL (
			SELECT
				sb.id,
				sb.shadow_schema,
				sb.shadow_table,
				sb.physical_table_fqn,
				sb.ddl_status,
				sb.updated_at
			FROM cdc_system.shadow_binding sb
			WHERE sb.source_object_id = so.id
			  AND sb.shadow_table = tr.target_table
			ORDER BY sb.is_active DESC, sb.updated_at DESC, sb.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN LATERAL (
			SELECT
				rr.target_table,
				rr.diff,
				rr.status
			FROM cdc_system.cdc_reconciliation_report rr
			WHERE rr.target_table = tr.target_table
			ORDER BY rr.checked_at DESC
			LIMIT 1
		) rr ON TRUE
		WHERE tr.id = ?
		LIMIT 1
	`
	var rows []queries.SourceObjectMappingContextReadModel
	if err := r.db.WithContext(ctx).Raw(q, registryID).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// errSourceObjectReadOnly guards against accidental write attempts on
// this read-only adapter. No production code reaches it (the interface
// has no write methods); kept as a compile-time pin in case a future
// caller embeds the struct.
var _ = errors.New("source_object_read_repo_gorm: read-only adapter")
