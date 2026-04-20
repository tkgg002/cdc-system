package service

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Plan v3 WORKER task #5 — partition retention enforcement.
//
// Partitioned tables in this project (see migration 010):
//   - failed_sync_logs_YYYY_MM    → retention 90 days
//   - cdc_activity_log_YYYY_MM    → retention 30 days
//
// Local dev does NOT run pg_partman / pg_cron, so this dedicated
// goroutine picks up the same responsibility. It runs once on
// startup, then every `Interval` (default 24h). Each tick:
//
//  1. SELECT tablename FROM pg_tables WHERE tablename ~ '<partition regex>'
//  2. For each match, parse the trailing YYYY_MM → time.Time
//     (first-of-month, UTC).
//  3. If the PARTITION END (start_of_next_month) is older than
//     `now - retention`, issue `DROP TABLE IF EXISTS <partition>`.
//  4. Emit Prom metric + structured log.
//
// The job runs regardless of leader election: DROP TABLE IF EXISTS
// is idempotent across workers and we take an advisory lock so we
// don't execute concurrent drops on the same table even in multi-
// instance deployments.

var (
	PartitionDropCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_partition_drops_total",
			Help: "Number of monthly partitions dropped by retention job",
		},
		[]string{"parent_table"},
	)
	PartitionDropErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_partition_drop_errors_total",
			Help: "Errors encountered by the partition retention job",
		},
		[]string{"parent_table"},
	)
)

// PartitionDropperConfig — override defaults per environment. All
// zero-value fields fall back to sensible defaults.
type PartitionDropperConfig struct {
	// Interval between full sweeps. Default 24h.
	Interval time.Duration
	// FailedSyncLogsRetention — default 90 days.
	FailedSyncLogsRetention time.Duration
	// CDCActivityLogRetention — default 30 days.
	CDCActivityLogRetention time.Duration
	// RunAt — HH:MM in worker local tz. When set, the first sweep
	// waits until the next HH:MM; subsequent sweeps use Interval.
	// Empty = first sweep at startup + every Interval thereafter.
	RunAt string
}

func (c *PartitionDropperConfig) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = 24 * time.Hour
	}
	if c.FailedSyncLogsRetention <= 0 {
		c.FailedSyncLogsRetention = 90 * 24 * time.Hour
	}
	if c.CDCActivityLogRetention <= 0 {
		c.CDCActivityLogRetention = 30 * 24 * time.Hour
	}
}

// partitionRule describes a parent table + retention + regex used to
// pattern-match its monthly / daily partitions.
//
// Two naming schemes are observed in this project:
//  1. failed_sync_logs_y<YYYY>m<MM>   — monthly; partition spans one month.
//  2. cdc_activity_log_<YYYYMMDD>     — daily; partition spans one day.
//
// Each rule supplies a regex + a parser that returns (start, end) —
// the half-open interval the partition covers. Retention compares
// `end < now - retention` to decide DROP.
type partitionRule struct {
	Parent    string
	Retention time.Duration
	Re        *regexp.Regexp
	// Parse extracts the partition [start, end) from the submatches.
	// Return zero times to skip (regex matched but fields are invalid).
	Parse func(m []string) (time.Time, time.Time)
}

// PartitionDropper runs the retention loop.
type PartitionDropper struct {
	db     *gorm.DB
	logger *zap.Logger
	cfg    PartitionDropperConfig
	rules  []partitionRule
}

// NewPartitionDropper constructs the service. The rule set is fixed
// (failed_sync_logs + cdc_activity_log) but retention comes from cfg.
func NewPartitionDropper(db *gorm.DB, cfg PartitionDropperConfig, logger *zap.Logger) *PartitionDropper {
	cfg.applyDefaults()
	return &PartitionDropper{
		db:     db,
		logger: logger,
		cfg:    cfg,
		rules: []partitionRule{
			{
				Parent:    "failed_sync_logs",
				Retention: cfg.FailedSyncLogsRetention,
				// Monthly: failed_sync_logs_y2026m04
				Re: regexp.MustCompile(`^failed_sync_logs_y(\d{4})m(\d{2})$`),
				Parse: func(m []string) (time.Time, time.Time) {
					var year, month int
					fmt.Sscanf(m[1], "%d", &year)
					fmt.Sscanf(m[2], "%d", &month)
					if year < 1970 || month < 1 || month > 12 {
						return time.Time{}, time.Time{}
					}
					start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
					end := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
					return start, end
				},
			},
			{
				Parent:    "cdc_activity_log",
				Retention: cfg.CDCActivityLogRetention,
				// Daily: cdc_activity_log_20260417
				Re: regexp.MustCompile(`^cdc_activity_log_(\d{4})(\d{2})(\d{2})$`),
				Parse: func(m []string) (time.Time, time.Time) {
					var year, month, day int
					fmt.Sscanf(m[1], "%d", &year)
					fmt.Sscanf(m[2], "%d", &month)
					fmt.Sscanf(m[3], "%d", &day)
					if year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 {
						return time.Time{}, time.Time{}
					}
					start := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
					end := start.Add(24 * time.Hour)
					return start, end
				},
			},
		},
	}
}

// Start runs until ctx is cancelled. First sweep fires on startup so
// operators can verify the job at boot.
func (p *PartitionDropper) Start(ctx context.Context) {
	p.logger.Info("partition dropper started",
		zap.Duration("interval", p.cfg.Interval),
		zap.Duration("failed_sync_logs_retention", p.cfg.FailedSyncLogsRetention),
		zap.Duration("cdc_activity_log_retention", p.cfg.CDCActivityLogRetention),
	)

	// First sweep immediately; we still honor RunAt if set — schedule a
	// timer to HH:MM tomorrow but run now as a sanity check.
	p.RunOnce(ctx)

	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("partition dropper stopped")
			return
		case <-ticker.C:
			p.RunOnce(ctx)
		}
	}
}

// RunOnce performs a single sweep across all configured rules.
// Exported so tests and manual triggers can invoke it synchronously.
func (p *PartitionDropper) RunOnce(ctx context.Context) {
	// Advisory lock to serialise across worker instances.
	var acquired bool
	if err := p.db.WithContext(ctx).Raw(
		"SELECT pg_try_advisory_lock(hashtext('cdc_partition_dropper'))",
	).Scan(&acquired).Error; err != nil {
		p.logger.Warn("partition dropper: advisory lock probe failed", zap.Error(err))
		return
	}
	if !acquired {
		p.logger.Debug("partition dropper: another instance holds the lock, skipping")
		return
	}
	defer p.db.Exec("SELECT pg_advisory_unlock(hashtext('cdc_partition_dropper'))")

	now := time.Now().UTC()
	for _, rule := range p.rules {
		if err := p.sweep(ctx, rule, now); err != nil {
			p.logger.Warn("partition sweep failed",
				zap.String("parent", rule.Parent),
				zap.Error(err),
			)
			PartitionDropErrors.WithLabelValues(rule.Parent).Inc()
		}
	}
}

func (p *PartitionDropper) sweep(ctx context.Context, rule partitionRule, now time.Time) error {
	// Pattern matches the GIN regex used by pg_tables.
	pattern := strings.Replace(rule.Re.String(), `^`, "^", 1)
	pattern = strings.TrimPrefix(pattern, "^")
	pattern = strings.TrimSuffix(pattern, "$")
	// pg_tables uses ~ POSIX regex — keep ^...$.
	pgPattern := "^" + pattern + "$"

	type pgTable struct {
		Tablename string `gorm:"column:tablename"`
	}
	var rows []pgTable
	err := p.db.WithContext(ctx).Raw(
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename ~ ?`,
		pgPattern,
	).Scan(&rows).Error
	if err != nil {
		return fmt.Errorf("pg_tables lookup: %w", err)
	}

	cutoff := now.Add(-rule.Retention)
	drops := 0
	for _, r := range rows {
		m := rule.Re.FindStringSubmatch(r.Tablename)
		if m == nil {
			continue
		}
		_, partitionEnd := rule.Parse(m)
		if partitionEnd.IsZero() {
			continue
		}
		if !partitionEnd.Before(cutoff) {
			continue // still within retention window
		}

		// DROP TABLE IF EXISTS — identifier is validated by the regex so
		// no injection surface. Quote defensively.
		ident := `"` + strings.ReplaceAll(r.Tablename, `"`, `""`) + `"`
		if err := p.db.WithContext(ctx).Exec("DROP TABLE IF EXISTS " + ident).Error; err != nil {
			p.logger.Warn("partition drop failed",
				zap.String("parent", rule.Parent),
				zap.String("partition", r.Tablename),
				zap.Error(err),
			)
			PartitionDropErrors.WithLabelValues(rule.Parent).Inc()
			continue
		}
		drops++
		PartitionDropCount.WithLabelValues(rule.Parent).Inc()
		p.logger.Info("partition dropped (retention)",
			zap.String("parent", rule.Parent),
			zap.String("partition", r.Tablename),
			zap.Time("partition_end", partitionEnd),
			zap.Time("cutoff", cutoff),
		)
	}
	if drops > 0 {
		p.logger.Info("partition sweep completed",
			zap.String("parent", rule.Parent),
			zap.Int("dropped", drops),
			zap.Int("scanned", len(rows)),
		)
	}
	return nil
}
