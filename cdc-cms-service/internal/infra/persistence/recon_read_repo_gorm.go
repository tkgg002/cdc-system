// Package persistence — GORM concrete adapters for ports.* interfaces
// and the in-package read ports declared in `internal/app/queries/`.
//
// This file implements queries.ReconReader. The SQL is lifted verbatim
// from `internal/api/reconciliation_handler.go` (LatestReport,
// TableHistory, ListFailedLogs) so the wire contract is byte-identical.
//
// Migration-017 fallback (LatestReport): the primary SELECT references
// columns introduced by worker migration 017 (timestamp_field_source,
// full_source_count, …). On older envs that SELECT errors; we silently
// fall back to a legacy SELECT that omits those columns. The read-port
// contract returns rows from whichever query succeeded — handlers don't
// see the fallback.
package persistence

import (
	"context"
	"strings"

	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/pkgs/utils"

	"gorm.io/gorm"
)

type reconReadRepoGorm struct {
	db *gorm.DB
}

// NewReconReadRepo constructs the GORM-backed adapter for
// queries.ReconReader.
func NewReconReadRepo(db *gorm.DB) queries.ReconReader {
	return &reconReadRepoGorm{db: db}
}

// listLatestPrimary expects worker migration 017 (new fields). LEFT
// JOIN keeps tables present even if registry row is stale.
const listLatestPrimary = `
	SELECT r.*,
	       r.source_count AS nullable_source_count,
	       reg.sync_engine, reg.source_type, reg.timestamp_field,
	       reg.timestamp_field_source, reg.timestamp_field_confidence,
	       reg.full_source_count, reg.full_dest_count, reg.full_count_at,
	       r.error_code,
	       sb.source_object_id,
	       so.source_object_name AS source_table,
	       sb.shadow_schema,
	       sb.shadow_table,
	       COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
	  FROM (
		SELECT DISTINCT ON (target_table) *
		  FROM cdc_reconciliation_report
		 ORDER BY target_table, checked_at DESC
	  ) r
	  LEFT JOIN cdc_table_registry reg ON reg.target_table = r.target_table
	  LEFT JOIN LATERAL (
		SELECT source_object_id, shadow_schema, shadow_table
		FROM cdc_system.shadow_binding
		WHERE shadow_table = r.target_table
		  AND is_active = TRUE
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	  ) sb ON TRUE
	  LEFT JOIN LATERAL (
		SELECT COUNT(*)::int AS binding_count
		FROM cdc_system.shadow_binding
		WHERE shadow_table = r.target_table
		  AND is_active = TRUE
	  ) scope_counts ON TRUE
	  LEFT JOIN cdc_system.source_object_registry so ON so.id = sb.source_object_id
	 ORDER BY r.target_table
`

// listLatestLegacy is the fallback query when primary errors (older
// envs without migration 017). New columns come back nil/0.
const listLatestLegacy = `
	SELECT r.*, r.source_count AS nullable_source_count,
	       reg.sync_engine, reg.source_type, reg.timestamp_field,
	       sb.source_object_id,
	       so.source_object_name AS source_table,
	       sb.shadow_schema,
	       sb.shadow_table,
	       COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
	  FROM (
		SELECT DISTINCT ON (target_table) *
		  FROM cdc_reconciliation_report
		 ORDER BY target_table, checked_at DESC
	  ) r
	  LEFT JOIN cdc_table_registry reg ON reg.target_table = r.target_table
	  LEFT JOIN LATERAL (
		SELECT source_object_id, shadow_schema, shadow_table
		FROM cdc_system.shadow_binding
		WHERE shadow_table = r.target_table
		  AND is_active = TRUE
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	  ) sb ON TRUE
	  LEFT JOIN LATERAL (
		SELECT COUNT(*)::int AS binding_count
		FROM cdc_system.shadow_binding
		WHERE shadow_table = r.target_table
		  AND is_active = TRUE
	  ) scope_counts ON TRUE
	  LEFT JOIN cdc_system.source_object_registry so ON so.id = sb.source_object_id
	 ORDER BY r.target_table
`

func (r *reconReadRepoGorm) ListLatest(ctx context.Context) ([]queries.LatestReportRow, error) {
	var rows []queries.LatestReportRow
	if err := r.db.WithContext(ctx).Raw(listLatestPrimary).Scan(&rows).Error; err != nil {
		// Migration 017 not applied yet — fall back. Legacy path is
		// best-effort: handler treats `nil` rows as empty list.
		rows = rows[:0]
		_ = r.db.WithContext(ctx).Raw(listLatestLegacy).Scan(&rows).Error
	}
	return rows, nil
}

func (r *reconReadRepoGorm) GetTableHistory(ctx context.Context, table string, page, pageSize int) ([]model.ReconciliationReport, int64, error) {
	var total int64
	if err := r.db.WithContext(ctx).
		Model(&model.ReconciliationReport{}).
		Where("target_table = ?", table).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var reports []model.ReconciliationReport
	if err := r.db.WithContext(ctx).
		Where("target_table = ?", table).
		Order("checked_at DESC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Find(&reports).Error; err != nil {
		return nil, 0, err
	}
	return reports, total, nil
}

const failedLogsBase = `
	SELECT
		f.id,
		f.target_table,
		f.source_table,
		f.source_db,
		f.record_id,
		f.operation,
		f.raw_json,
		f.error_message,
		f.error_type,
		f.kafka_topic,
		f.kafka_partition,
		f.kafka_offset,
		f.retry_count,
		f.max_retries,
		f.status,
		f.created_at,
		f.last_retry_at,
		f.resolved_at,
		f.resolved_by,
		so.source_object_name AS resolved_source_table,
		sb.shadow_schema,
		sb.shadow_table,
		COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
	FROM failed_sync_logs f
	LEFT JOIN LATERAL (
		SELECT source_object_id, shadow_schema, shadow_table
		FROM cdc_system.shadow_binding
		WHERE shadow_table = f.target_table
		  AND is_active = TRUE
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	) sb ON TRUE
	LEFT JOIN LATERAL (
		SELECT COUNT(*)::int AS binding_count
		FROM cdc_system.shadow_binding
		WHERE shadow_table = f.target_table
		  AND is_active = TRUE
	) scope_counts ON TRUE
	LEFT JOIN cdc_system.source_object_registry so ON so.id = sb.source_object_id
	WHERE 1=1
`

// resolveTargetTableQueryBase mirrors the legacy resolveTargetTable
// helper in the recon HTTP handler. The SELECT shape (sb.shadow_table)
// + LIMIT 2 protocol (caller flags >1 row as ambiguous) is preserved
// so the wire contract stays byte-identical.
const resolveTargetTableQueryBase = `
	SELECT sb.shadow_table
	FROM cdc_system.shadow_binding sb
	JOIN cdc_system.source_object_registry so
	  ON so.id = sb.source_object_id
	WHERE sb.is_active = TRUE
`

func (r *reconReadRepoGorm) ResolveTargetTableByScope(ctx context.Context, f queries.ReconScopeFilter) (string, error) {
	query := resolveTargetTableQueryBase
	args := make([]interface{}, 0, 6)
	if v := strings.TrimSpace(f.SourceDatabase); v != "" {
		query += ` AND so.source_database = ?`
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.SourceSchema); v != "" {
		query += ` AND so.source_schema = ?`
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.SourceNamespace); v != "" {
		query += ` AND so.source_namespace = ?`
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.SourceTable); v != "" {
		query += ` AND so.source_object_name = ?`
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.ShadowSchema); v != "" {
		query += ` AND sb.shadow_schema = ?`
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.ShadowTable); v != "" {
		query += ` AND sb.shadow_table = ?`
		args = append(args, v)
	}
	query += ` ORDER BY sb.updated_at DESC, sb.id DESC LIMIT 2`

	var rows []struct {
		ShadowTable string `gorm:"column:shadow_table"`
	}
	if err := r.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return "", queries.ErrAmbiguousScope
	}
	return rows[0].ShadowTable, nil
}

func (r *reconReadRepoGorm) GetFailedLogByID(ctx context.Context, id int64) (*model.FailedSyncLog, error) {
	var log model.FailedSyncLog
	if err := r.db.WithContext(ctx).First(&log, id).Error; err != nil {
		return nil, err
	}
	return &log, nil
}

// retryScopeQuery is the LATERAL-join enrichment lifted verbatim from
// the legacy RetryFailedLog handler. The handler historically swallowed
// the error (`_ = ...Scan(...)`) — keep that behaviour by returning a
// zero-value scope on err so the retry dispatch is never blocked by a
// metadata read failure.
const retryScopeQuery = `
	SELECT
		COALESCE(NULLIF(f.source_db, ''), so.source_database) AS source_database,
		COALESCE(NULLIF(f.source_table, ''), so.source_object_name) AS resolved_source_table,
		sb.shadow_schema,
		sb.shadow_table,
		COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
	FROM failed_sync_logs f
	LEFT JOIN LATERAL (
		SELECT source_object_id, shadow_schema, shadow_table
		FROM cdc_system.shadow_binding
		WHERE shadow_table = f.target_table
		  AND is_active = TRUE
		ORDER BY updated_at DESC, id DESC
		LIMIT 1
	) sb ON TRUE
	LEFT JOIN LATERAL (
		SELECT COUNT(*)::int AS binding_count
		FROM cdc_system.shadow_binding
		WHERE shadow_table = f.target_table
		  AND is_active = TRUE
	) scope_counts ON TRUE
	LEFT JOIN cdc_system.source_object_registry so ON so.id = sb.source_object_id
	WHERE f.id = ?
`

func (r *reconReadRepoGorm) GetRetryScopeByLogID(ctx context.Context, id int64) (queries.FailedLogRetryScope, error) {
	var scope queries.FailedLogRetryScope
	// Match legacy: error swallowed, zero-value scope returned. The
	// retry dispatch must not be blocked by a metadata read miss.
	_ = r.db.WithContext(ctx).Raw(retryScopeQuery, id).Scan(&scope).Error
	return scope, nil
}

func (r *reconReadRepoGorm) ListBackfillRuns(ctx context.Context, table, runID string, limit int) ([]queries.BackfillRunRow, error) {
	if limit <= 0 {
		limit = 30
	}
	q := r.db.WithContext(ctx).
		Table("recon_runs").
		Where("tier = ?", 4).
		Order("started_at DESC").
		Limit(limit)
	if t := strings.TrimSpace(table); t != "" {
		q = q.Where("table_name = ?", t)
	}
	if rid := strings.TrimSpace(runID); rid != "" {
		q = q.Where("instance_id = ?", "backfill:"+rid)
	}
	var rows []queries.BackfillRunRow
	if err := q.Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *reconReadRepoGorm) CountTableRows(ctx context.Context, table string) (int64, int64, error) {
	ident := utils.PgIdent(table)
	var total, nul int64
	// Identifier safety: utils.PgIdent fail-closes to `""` so a
	// malformed name produces a parse error rather than executing.
	if err := r.db.WithContext(ctx).Raw("SELECT COUNT(*) FROM " + ident).Scan(&total).Error; err != nil {
		return 0, 0, err
	}
	if err := r.db.WithContext(ctx).Raw("SELECT COUNT(*) FROM " + ident + " WHERE _source_ts IS NULL").Scan(&nul).Error; err != nil {
		return total, 0, err
	}
	return total, nul, nil
}

func (r *reconReadRepoGorm) ListFailedLogs(ctx context.Context, f queries.FailedLogFilter, page, pageSize int) ([]queries.FailedLogRow, int64, error) {
	query := failedLogsBase
	args := make([]interface{}, 0, 4)
	if f.TargetTable != "" {
		query += ` AND f.target_table = ?`
		args = append(args, f.TargetTable)
	}
	if f.Status != "" {
		query += ` AND f.status = ?`
		args = append(args, f.Status)
	}
	if f.ErrorType != "" {
		query += ` AND f.error_type = ?`
		args = append(args, f.ErrorType)
	}

	var total int64
	countQuery := `SELECT COUNT(*) FROM (` + query + `) AS failed_logs`
	if err := r.db.WithContext(ctx).Raw(countQuery, args...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}

	pagedQuery := query + ` ORDER BY f.created_at DESC OFFSET ? LIMIT ?`
	pagedArgs := append(args, (page-1)*pageSize, pageSize)

	var rows []queries.FailedLogRow
	if err := r.db.WithContext(ctx).Raw(pagedQuery, pagedArgs...).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}
