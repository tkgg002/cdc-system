// system_health_queries.go — DB-derived snapshot sections.
//
// Lives next to the collector (same package) because it touches
// Collector internals (db, logger, cfg.ProbeTimeout) but is logically
// independent of the external HTTP probes that already moved to
// internal/infra/observability/probes. Keeping these here means the probes
// package stays a leaf with no inbound dependency on service.
package observability

import (
	"context"
	"fmt"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
)

// queryReconciliation pulls the latest report per target table. The
// DISTINCT ON pattern + (target_table, checked_at DESC) order picks
// the freshest row per table in one scan; postgres uses index
// idx_recon_target_checked.
func (c *Collector) queryReconciliation(ctx context.Context) []map[string]any {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	var reports []model.ReconciliationReport
	err := c.db.WithContext(ctxQ).Raw(
		`SELECT DISTINCT ON (target_table) * FROM cdc_reconciliation_report ORDER BY target_table, checked_at DESC`,
	).Scan(&reports).Error
	if err != nil {
		c.logger.Debug("query reconciliation", zap.Error(err))
		return nil
	}

	result := make([]map[string]any, 0, len(reports))
	for _, r := range reports {
		driftPct := float64(0)
		if r.SourceCount > 0 {
			driftPct = float64(r.Diff) / float64(r.SourceCount) * 100
		}
		result = append(result, map[string]any{
			"table":        r.TargetTable,
			"source_count": r.SourceCount,
			"dest_count":   r.DestCount,
			"drift_pct":    fmt.Sprintf("%.2f", driftPct),
			"status":       r.Status,
			"last_check":   r.CheckedAt,
		})
	}
	return result
}

// queryFailedCount aggregates 24h / 1h failed-sync rows. The bounded
// `> lower AND <= NOW()` ranges enable runtime partition pruning on
// failed_sync_logs (migration 010) — without the upper bound Postgres
// opens every partition (300ms+ planning overhead in production).
func (c *Collector) queryFailedCount(ctx context.Context) map[string]any {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	var count24h, count1h int64
	c.db.WithContext(ctxQ).Model(&model.FailedSyncLog{}).
		Where("created_at > NOW() - INTERVAL '24 hours' AND created_at <= NOW()").Count(&count24h)
	c.db.WithContext(ctxQ).Model(&model.FailedSyncLog{}).
		Where("created_at > NOW() - INTERVAL '1 hour' AND created_at <= NOW()").Count(&count1h)
	return map[string]any{"count_24h": count24h, "count_1h": count1h}
}

// queryRecentEvents returns the 10 most recent activity-log rows.
// The created_at>NOW()-1day predicate bounds the scan to 1–2
// partitions of cdc_activity_log (daily partition); idx_act_new_started
// (started_at DESC) serves the ORDER BY + LIMIT within each partition.
func (c *Collector) queryRecentEvents(ctx context.Context) []map[string]any {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	var logs []model.ActivityLog
	c.db.WithContext(ctxQ).
		Where("created_at > NOW() - INTERVAL '1 day' AND created_at <= NOW()").
		Order("started_at DESC").Limit(10).Find(&logs)

	result := make([]map[string]any, 0, len(logs))
	for _, l := range logs {
		result = append(result, map[string]any{
			"time":      l.StartedAt,
			"operation": l.Operation,
			"table":     l.TargetTable,
			"status":    l.Status,
			"details":   string(l.Details),
		})
	}
	return result
}
