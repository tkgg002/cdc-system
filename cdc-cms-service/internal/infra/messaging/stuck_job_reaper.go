// stuck_job_reaper.go — periodic sweep of cdc_jobs rows stuck in
// 'running' beyond their type-specific timeout.
//
// Boss G1 REVISE (P3.T3.12): a flat 30s reaper false-positives recon.check
// on a 50GB shadow (legitimate >5min runtime) and master.swap (already
// bounded to 30s by the goroutine ctx). The map below tunes per type;
// unknown types fall back to defaultTO. Tune via NewStuckJobReaper —
// no schema change needed.
package messaging

import (
	"context"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// StuckJobReaper sweeps cdc_jobs rows stuck in 'running' beyond a
// per-type timeout.
type StuckJobReaper struct {
	db        *gorm.DB
	logger    *zap.Logger
	interval  time.Duration
	timeouts  map[string]time.Duration
	defaultTO time.Duration
}

// DefaultJobTimeouts is the spec map per boss G1 verdict. Add new
// command types here when their handler is registered.
func DefaultJobTimeouts() map[string]time.Duration {
	return map[string]time.Duration{
		"master.swap":    60 * time.Second,
		"master.create":  120 * time.Second,
		"recon.check":    10 * time.Minute,
		"recon.heal":     5 * time.Minute,
		"recon.retry-failed":          2 * time.Minute,
		"recon.backfill-source-ts":    15 * time.Minute,
		"transmute":                   15 * time.Minute,
		"source.standardize":          5 * time.Minute,
		"source.scan-fields":          5 * time.Minute,
		"source.detect-timestamp-field": 5 * time.Minute,
		"source.create-default-columns": 2 * time.Minute,
		"mapping.alter-column":          2 * time.Minute,
		"mapping.backfill":              30 * time.Minute,
		"debezium.signal":               1 * time.Minute,
		"debezium.snapshot":             10 * time.Minute,
		"debezium.restart":              2 * time.Minute,
	}
}

// NewStuckJobReaper builds the reaper. Pass nil timeouts to use the
// DefaultJobTimeouts map; pass nil interval (or zero) to use 30s.
func NewStuckJobReaper(db *gorm.DB, logger *zap.Logger, interval time.Duration, timeouts map[string]time.Duration) *StuckJobReaper {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if timeouts == nil {
		timeouts = DefaultJobTimeouts()
	}
	return &StuckJobReaper{
		db:        db,
		logger:    logger,
		interval:  interval,
		timeouts:  timeouts,
		defaultTO: 30 * time.Second,
	}
}

// Run blocks on a ticker until ctx is canceled. Designed to live in a
// goroutine started at server boot, paired with a CancelFunc on shutdown.
func (r *StuckJobReaper) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	r.logger.Info("stuck job reaper started",
		zap.Duration("interval", r.interval),
		zap.Int("type_count", len(r.timeouts)),
		zap.Duration("default_timeout", r.defaultTO))
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("stuck job reaper stopped")
			return
		case <-t.C:
			if err := r.reapOnce(ctx); err != nil {
				r.logger.Warn("reaper sweep failed", zap.Error(err))
			}
		}
	}
}

// reapOnce flips every running row whose started_at + per-type-timeout
// has elapsed. Returns the row count flipped (visible via metrics later).
//
// SQL emits a single UPDATE so the sweep is one round-trip regardless of
// the type-count. The CASE expression is built deterministically (sorted
// keys) so the prepared statement plan stays cache-friendly.
func (r *StuckJobReaper) reapOnce(ctx context.Context) error {
	keys := make([]string, 0, len(r.timeouts))
	for k := range r.timeouts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var caseExpr strings.Builder
	caseExpr.WriteString("CASE type ")
	args := make([]interface{}, 0, len(keys)*2+1)
	for _, k := range keys {
		caseExpr.WriteString("WHEN ? THEN ?::int ")
		args = append(args, k, int64(r.timeouts[k].Seconds()))
	}
	caseExpr.WriteString("ELSE ?::int END")
	args = append(args, int64(r.defaultTO.Seconds()))

	sql := `
        UPDATE cdc_system.cdc_jobs
           SET status = 'failed',
               error_message = COALESCE(NULLIF(error_message, ''), 'reaper: timeout exceeded'),
               finished_at = NOW()
         WHERE status = 'running'
           AND started_at IS NOT NULL
           AND started_at + (interval '1 second' * (` + caseExpr.String() + `)::int) < NOW()
    `

	res := r.db.WithContext(ctx).Exec(sql, args...)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		r.logger.Info("reaped stuck jobs", zap.Int64("count", res.RowsAffected))
	}
	return nil
}
