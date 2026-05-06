// Package persistence — GORM-backed read adapters.
//
// worker_schedule_read_repo_gorm.go implements
// queries.WorkerScheduleReader against cdc_system.cdc_worker_schedule
// enriched via LATERAL joins on shadow_binding + source_object_registry.
// SQL is byte-identical to the legacy handler.
package persistence

import (
	"context"
	"time"

	"cdc-cms-service/internal/app/queries"

	"gorm.io/gorm"
)

// WorkerScheduleReadRepo backs queries.WorkerScheduleReader.
type WorkerScheduleReadRepo struct {
	db *gorm.DB
}

func NewWorkerScheduleReadRepo(db *gorm.DB) *WorkerScheduleReadRepo {
	return &WorkerScheduleReadRepo{db: db}
}

// scanRow is the flat shape GORM unmarshals into; we project it
// into the nested WorkerScheduleResponse.Scope shape after loading.
type scanRow struct {
	ID               uint       `gorm:"column:id"`
	Operation        string     `gorm:"column:operation"`
	TargetTable      *string    `gorm:"column:target_table"`
	IntervalMinutes  int        `gorm:"column:interval_minutes"`
	IsEnabled        bool       `gorm:"column:is_enabled"`
	LastRunAt        *time.Time `gorm:"column:last_run_at"`
	NextRunAt        *time.Time `gorm:"column:next_run_at"`
	RunCount         int64      `gorm:"column:run_count"`
	LastError        *string    `gorm:"column:last_error"`
	Notes            *string    `gorm:"column:notes"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
	UpdatedAt        time.Time  `gorm:"column:updated_at"`
	SourceObjectID   *int64     `gorm:"column:source_object_id"`
	SourceDatabase   *string    `gorm:"column:source_database"`
	SourceSchema     *string    `gorm:"column:source_schema"`
	SourceNamespace  *string    `gorm:"column:source_namespace"`
	SourceTable      *string    `gorm:"column:source_table"`
	ShadowBindingID  *int64     `gorm:"column:shadow_binding_id"`
	ShadowSchema     *string    `gorm:"column:shadow_schema"`
	ShadowTable      *string    `gorm:"column:shadow_table"`
	PhysicalTableFQN *string    `gorm:"column:physical_table_fqn"`
	ScopeAmbiguous   bool       `gorm:"column:scope_ambiguous"`
}

// ListResponses pulls every worker_schedule row joined with the V2
// metadata for the FE table. Order: operation, target_table NULLS
// FIRST, id (legacy parity).
func (r *WorkerScheduleReadRepo) ListResponses(ctx context.Context) ([]queries.WorkerScheduleResponse, error) {
	var scan []scanRow
	err := r.db.WithContext(ctx).Raw(`
		SELECT
			ws.id,
			ws.operation,
			ws.target_table,
			ws.interval_minutes,
			ws.is_enabled,
			ws.last_run_at,
			ws.next_run_at,
			ws.run_count,
			ws.last_error,
			ws.notes,
			ws.created_at,
			ws.updated_at,
			sb.source_object_id,
			so.source_database,
			so.source_schema,
			so.source_namespace,
			so.source_object_name AS source_table,
			sb.shadow_binding_id,
			sb.shadow_schema,
			sb.shadow_table,
			sb.physical_table_fqn,
			COALESCE(scope_counts.binding_count, 0) > 1 AS scope_ambiguous
		FROM cdc_system.cdc_worker_schedule ws
		LEFT JOIN LATERAL (
			SELECT
				s.id AS shadow_binding_id,
				s.source_object_id,
				s.shadow_schema,
				s.shadow_table,
				s.physical_table_fqn
			FROM cdc_system.shadow_binding s
			WHERE ws.target_table IS NOT NULL
			  AND s.shadow_table = ws.target_table
			  AND s.is_active = TRUE
			ORDER BY s.updated_at DESC, s.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS binding_count
			FROM cdc_system.shadow_binding s
			WHERE ws.target_table IS NOT NULL
			  AND s.shadow_table = ws.target_table
			  AND s.is_active = TRUE
		) scope_counts ON TRUE
		LEFT JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		ORDER BY ws.operation, ws.target_table NULLS FIRST, ws.id
	`).Scan(&scan).Error
	if err != nil {
		return nil, err
	}
	rows := make([]queries.WorkerScheduleResponse, 0, len(scan))
	for _, s := range scan {
		rows = append(rows, queries.WorkerScheduleResponse{
			ID:              s.ID,
			Operation:       s.Operation,
			TargetTable:     s.TargetTable,
			IntervalMinutes: s.IntervalMinutes,
			IsEnabled:       s.IsEnabled,
			LastRunAt:       s.LastRunAt,
			NextRunAt:       s.NextRunAt,
			RunCount:        s.RunCount,
			LastError:       s.LastError,
			Notes:           s.Notes,
			CreatedAt:       s.CreatedAt,
			UpdatedAt:       s.UpdatedAt,
			Scope: queries.WorkerScheduleScope{
				SourceObjectID:   s.SourceObjectID,
				SourceDatabase:   s.SourceDatabase,
				SourceSchema:     s.SourceSchema,
				SourceNamespace:  s.SourceNamespace,
				SourceTable:      s.SourceTable,
				ShadowBindingID:  s.ShadowBindingID,
				ShadowSchema:     s.ShadowSchema,
				ShadowTable:      s.ShadowTable,
				PhysicalTableFQN: s.PhysicalTableFQN,
				ScopeAmbiguous:   s.ScopeAmbiguous,
			},
		})
	}
	return rows, nil
}

// GetResponseByID returns one schedule row by id. Implementation
// matches the legacy handler exactly: list-then-filter (the join
// projection is the same).
func (r *WorkerScheduleReadRepo) GetResponseByID(ctx context.Context, id uint) (*queries.WorkerScheduleResponse, error) {
	rows, err := r.ListResponses(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].ID == id {
			return &rows[i], nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}
