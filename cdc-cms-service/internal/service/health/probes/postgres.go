package probes

import (
	"context"
	"time"

	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

// Postgres probes the CMS metadata DB by counting registry rows
// (cheap GORM call) plus a pg_stat_user_tables sum. The model.Count
// query is the liveness gate — its success implies the connection,
// the schema, and the migrations are all healthy.
func Postgres(ctx context.Context, db *gorm.DB, probeTimeout time.Duration) map[string]any {
	start := time.Now()
	ctxQ, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	var tableCount, rowCount int64
	gdb := db.WithContext(ctxQ)
	if err := gdb.Model(&model.TableRegistry{}).Count(&tableCount).Error; err != nil {
		return map[string]any{"status": StatusDown, "error": SanitizeErr(err), "latency_ms": time.Since(start).Milliseconds()}
	}
	gdb.Raw("SELECT COALESCE(SUM(n_live_tup),0) FROM pg_stat_user_tables WHERE schemaname='public'").Scan(&rowCount)

	return map[string]any{
		"status":            StatusUp,
		"tables_registered": tableCount,
		"total_rows":        rowCount,
		"latency_ms":        time.Since(start).Milliseconds(),
	}
}
