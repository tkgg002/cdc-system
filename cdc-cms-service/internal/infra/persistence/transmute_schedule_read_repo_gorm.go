// Package persistence — GORM-backed read adapters.
//
// transmute_schedule_read_repo_gorm.go implements
// queries.TransmuteScheduleReader against cdc_system.transmute_schedule
// joined with cdc_system.master_binding. SQL is byte-identical to the
// legacy handler.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/queries"

	"gorm.io/gorm"
)

// TransmuteScheduleReadRepo backs queries.TransmuteScheduleReader.
type TransmuteScheduleReadRepo struct {
	db *gorm.DB
}

func NewTransmuteScheduleReadRepo(db *gorm.DB) *TransmuteScheduleReadRepo {
	return &TransmuteScheduleReadRepo{db: db}
}

// ListSchedules returns every schedule row, joined with the master
// binding so the FE can render `master_table`. NULLS LAST keeps
// orphaned schedules at the bottom of the list (legacy parity).
func (r *TransmuteScheduleReadRepo) ListSchedules(ctx context.Context) ([]queries.TransmuteScheduleRow, error) {
	var rows []queries.TransmuteScheduleRow
	err := r.db.WithContext(ctx).Raw(`
		SELECT
			ts.id,
			mb.master_table        AS master_table,
			ts.mode,
			ts.cron_expr,
			ts.last_run_at,
			ts.next_run_at,
			ts.last_status,
			ts.last_error,
			ts.last_stats,
			ts.is_enabled,
			ts.created_by,
			ts.created_at,
			ts.updated_at
		FROM cdc_system.transmute_schedule ts
		LEFT JOIN cdc_system.master_binding mb ON mb.id = ts.master_binding_id
		ORDER BY mb.master_table NULLS LAST, ts.mode
	`).Scan(&rows).Error
	return rows, err
}
