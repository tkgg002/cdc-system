// Package persistence — GORM-backed read adapters.
//
// activity_log_read_repo_gorm.go implements queries.ActivityLogReader
// against `cdc_activity_log` enriched via LATERAL joins on
// cdc_system.shadow_binding + cdc_system.source_object_registry. The
// SQL is kept byte-identical to the legacy handler so the wire
// contract is preserved.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/queries"

	"gorm.io/gorm"
)

// ActivityLogReadRepo backs queries.ActivityLogReader.
type ActivityLogReadRepo struct {
	db *gorm.DB
}

func NewActivityLogReadRepo(db *gorm.DB) *ActivityLogReadRepo {
	return &ActivityLogReadRepo{db: db}
}

// baseFromClause is the shared FROM + LEFT JOIN LATERAL block. It
// returns a string (not a builder) because the legacy handler used
// raw SQL concat to compose List + Stats queries — keeping the same
// pattern preserves the exact column expressions and join order.
func (r *ActivityLogReadRepo) baseFromClause() string {
	return `
		FROM cdc_activity_log al
		LEFT JOIN LATERAL (
			SELECT
				sb.source_object_id,
				sb.shadow_schema,
				sb.shadow_table
			FROM cdc_system.shadow_binding sb
			WHERE al.target_table IS NOT NULL
			  AND al.target_table <> '*'
			  AND sb.shadow_table = al.target_table
			  AND sb.is_active = TRUE
			ORDER BY sb.updated_at DESC, sb.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS binding_count
			FROM cdc_system.shadow_binding sb
			WHERE al.target_table IS NOT NULL
			  AND al.target_table <> '*'
			  AND sb.shadow_table = al.target_table
			  AND sb.is_active = TRUE
		) scope_counts ON TRUE
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		WHERE 1=1
	`
}

// projectionColumns is the SELECT list shared by List + Stats's
// recent_errors block. It must mirror the legacy handler exactly.
func (r *ActivityLogReadRepo) projectionColumns() string {
	return `
		SELECT
			al.id,
			al.operation,
			al.target_table,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table,
			COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous,
			al.status,
			al.rows_affected,
			al.duration_ms,
			al.details,
			al.error_message,
			al.triggered_by,
			TO_CHAR(al.started_at, 'YYYY-MM-DD"T"HH24:MI:SSOF') AS started_at,
			CASE
				WHEN al.completed_at IS NULL THEN NULL
				ELSE TO_CHAR(al.completed_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')
			END AS completed_at
	`
}

// ListActivity returns paginated rows + a total count. Filter clauses
// are appended in the same order as the legacy handler so the bound
// parameters end up in identical positions.
func (r *ActivityLogReadRepo) ListActivity(ctx context.Context, f queries.ActivityLogFilter, page, pageSize int) ([]queries.ActivityLogRow, int64, error) {
	query := r.projectionColumns() + r.baseFromClause()
	countQuery := `SELECT COUNT(*) ` + r.baseFromClause()
	args := make([]interface{}, 0, 8)
	countArgs := make([]interface{}, 0, 8)

	appendFilter := func(clause, value string) {
		query += clause
		countQuery += clause
		args = append(args, value)
		countArgs = append(countArgs, value)
	}
	if f.Operation != "" {
		appendFilter(` AND al.operation = ?`, f.Operation)
	}
	if f.TargetTable != "" {
		appendFilter(` AND al.target_table = ?`, f.TargetTable)
	}
	if f.Status != "" {
		appendFilter(` AND al.status = ?`, f.Status)
	}
	if f.TriggeredBy != "" {
		appendFilter(` AND al.triggered_by = ?`, f.TriggeredBy)
	}
	if f.SourceDatabase != "" {
		appendFilter(` AND so.source_database = ?`, f.SourceDatabase)
	}
	if f.SourceTable != "" {
		appendFilter(` AND so.source_object_name = ?`, f.SourceTable)
	}
	if f.ShadowSchema != "" {
		appendFilter(` AND sb.shadow_schema = ?`, f.ShadowSchema)
	}
	if f.ShadowTable != "" {
		appendFilter(` AND sb.shadow_table = ?`, f.ShadowTable)
	}

	var total int64
	if err := r.db.WithContext(ctx).Raw(countQuery, countArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	query += ` ORDER BY al.started_at DESC OFFSET ? LIMIT ?`
	args = append(args, (page-1)*pageSize, pageSize)

	var rows []queries.ActivityLogRow
	if err := r.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}

// Stats24h returns the per-operation aggregate (last 24h) and the 10
// most recent error rows.
func (r *ActivityLogReadRepo) Stats24h(ctx context.Context) ([]queries.OpStat, []queries.ActivityLogRow, error) {
	var stats []queries.OpStat
	if err := r.db.WithContext(ctx).Raw(`
		SELECT
			operation,
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'success') as success,
			COUNT(*) FILTER (WHERE status = 'error') as error,
			COUNT(*) FILTER (WHERE status = 'skipped') as skipped
		FROM cdc_activity_log
		WHERE started_at > NOW() - INTERVAL '24 hours'
		GROUP BY operation
		ORDER BY total DESC
	`).Scan(&stats).Error; err != nil {
		return nil, nil, err
	}

	query := r.projectionColumns() + r.baseFromClause() + `
		AND al.status = 'error'
		ORDER BY al.started_at DESC
		LIMIT 10
	`
	var recentErrors []queries.ActivityLogRow
	if err := r.db.WithContext(ctx).Raw(query).Scan(&recentErrors).Error; err != nil {
		return nil, nil, err
	}
	return stats, recentErrors, nil
}
