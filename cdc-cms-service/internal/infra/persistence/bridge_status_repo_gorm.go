// Package persistence — bridge_status_repo_gorm.go is the GORM-backed
// adapter for queries.BridgeStatusReader. The SQL is lifted verbatim
// from the legacy `RegistryHandler.TransformStatus` and
// `SourceObjectActionsHandler.{resolveDispatchScopeBySourceObjectID,
// TransformStatusV2}` helpers so the wire contracts remain
// byte-identical after the API tier stops issuing raw SQL.
package persistence

import (
	"context"
	"strings"
	"time"

	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/pkgs/utils"

	"gorm.io/gorm"
)

type bridgeStatusRepoGorm struct {
	db *gorm.DB
}

// NewBridgeStatusRepo constructs the GORM-backed adapter for
// queries.BridgeStatusReader.
func NewBridgeStatusRepo(db *gorm.DB) queries.BridgeStatusReader {
	return &bridgeStatusRepoGorm{db: db}
}

func (r *bridgeStatusRepoGorm) ProbeBridgeStatus(ctx context.Context, schema, table string) (queries.BridgeStatusProbe, error) {
	var p queries.BridgeStatusProbe
	sch := schema
	if sch == "" {
		sch = "public"
	}

	if err := r.db.WithContext(ctx).Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = ?)",
		table, sch,
	).Scan(&p.Exists).Error; err != nil {
		return p, err
	}
	if !p.Exists {
		return p, nil
	}

	// utils.PgIdent fail-closes to `""` for malformed identifiers so a
	// hostile schema/table name produces a Postgres parse error rather
	// than executing.
	qualified := utils.PgIdent(sch) + "." + utils.PgIdent(table)

	if err := r.db.WithContext(ctx).Raw(
		"SELECT COUNT(*) FROM " + qualified,
	).Scan(&p.TotalRows).Error; err != nil {
		return p, err
	}

	if err := r.db.WithContext(ctx).Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = '_raw_data')",
		sch, table,
	).Scan(&p.HasRawData).Error; err != nil {
		return p, err
	}

	if p.HasRawData {
		if err := r.db.WithContext(ctx).Raw(
			"SELECT COUNT(*) FROM " + qualified + " WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb",
		).Scan(&p.RawDataRows).Error; err != nil {
			return p, err
		}
	}

	return p, nil
}

const dispatchScopeQuery = `
	SELECT
		so.id AS source_object_id,
		sb.shadow_table AS target_table,
		COALESCE(sb.shadow_schema, 'public') AS shadow_schema,
		COALESCE(so.source_database, '') AS source_database,
		so.source_object_name AS source_table,
		so.source_engine_type AS source_type,
		so.primary_key_field AS primary_key_field,
		COALESCE(so.primary_key_type, '') AS primary_key_type
	FROM cdc_system.source_object_registry so
	LEFT JOIN cdc_system.shadow_binding sb
	  ON sb.source_object_id = so.id
	 AND sb.is_active = TRUE
	WHERE so.id = ?
	  AND so.is_active = TRUE
	ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST
	LIMIT 2
`

func (r *bridgeStatusRepoGorm) ResolveDispatchScopeBySourceObjectID(ctx context.Context, id int64) (*queries.DispatchScope, error) {
	var rows []queries.DispatchScope
	if err := r.db.WithContext(ctx).Raw(dispatchScopeQuery, id).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, queries.ErrAmbiguousDispatchScope
	}
	if strings.TrimSpace(rows[0].TargetTable) == "" {
		return nil, queries.ErrSourceObjectNoActiveShadow
	}
	return &rows[0], nil
}

func (r *bridgeStatusRepoGorm) ListDispatchActivity(ctx context.Context, table, operation string, since time.Time) ([]map[string]interface{}, error) {
	q := r.db.WithContext(ctx).
		Table("cdc_system.cdc_activity_log").
		Where("target_table = ?", table)
	if operation != "" {
		q = q.Where("operation = ?", operation)
	}
	if !since.IsZero() {
		q = q.Where("started_at >= ?", since)
	}
	var entries []map[string]interface{}
	if err := q.Order("started_at DESC").Limit(50).Find(&entries).Error; err != nil {
		return nil, err
	}
	return entries, nil
}
