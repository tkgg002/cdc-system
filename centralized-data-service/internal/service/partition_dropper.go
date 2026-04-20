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
	// PartitionBackfillCount — child partitions materialised to absorb
	// orphan rows that landed in the *_default catch-all. See
	// EnsureBackfillPartitions.
	PartitionBackfillCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_partition_backfill_total",
			Help: "Child partitions created by the backfill job to absorb orphan rows from *_default",
		},
		[]string{"parent_table"},
	)
	// PartitionBackfillErrors — errors during backfill (lookup, CREATE, row move).
	PartitionBackfillErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_partition_backfill_errors_total",
			Help: "Errors encountered by the partition backfill job",
		},
		[]string{"parent_table"},
	)
)

// maxBackfillPartitionsPerRun caps the number of child partitions created in
// a single sweep, so a runaway *_default (e.g. months of orphan data) cannot
// hold the advisory lock indefinitely. The next sweep picks up the remainder.
const maxBackfillPartitionsPerRun = 31

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
//
// For backfill (EnsureBackfillPartitions), each rule also knows:
//   - DefaultTable: name of the catch-all partition to scan for orphans.
//   - Granularity:  "daily" | "monthly" — dictates partition naming +
//                   [start, end) range when materialising a new partition.
//   - NameForDay:   given a UTC day, return the child partition name.
//   - RangeForDay:  given a UTC day, return the partition's [start, end).
type partitionRule struct {
	Parent    string
	Retention time.Duration
	Re        *regexp.Regexp
	// Parse extracts the partition [start, end) from the submatches.
	// Return zero times to skip (regex matched but fields are invalid).
	Parse func(m []string) (time.Time, time.Time)

	// Backfill fields (see EnsureBackfillPartitions).
	DefaultTable string
	Granularity  string // "daily" or "monthly"
	NameForDay   func(day time.Time) string
	RangeForDay  func(day time.Time) (time.Time, time.Time)
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
				Parent:       "failed_sync_logs",
				Retention:    cfg.FailedSyncLogsRetention,
				DefaultTable: "failed_sync_logs_default",
				Granularity:  "monthly",
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
				NameForDay: func(day time.Time) string {
					return fmt.Sprintf("failed_sync_logs_y%04dm%02d", day.Year(), int(day.Month()))
				},
				RangeForDay: func(day time.Time) (time.Time, time.Time) {
					start := time.Date(day.Year(), day.Month(), 1, 0, 0, 0, 0, time.UTC)
					end := time.Date(day.Year(), day.Month()+1, 1, 0, 0, 0, 0, time.UTC)
					return start, end
				},
			},
			{
				Parent:       "cdc_activity_log",
				Retention:    cfg.CDCActivityLogRetention,
				DefaultTable: "cdc_activity_log_default",
				Granularity:  "daily",
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
				NameForDay: func(day time.Time) string {
					return fmt.Sprintf("cdc_activity_log_%04d%02d%02d", day.Year(), int(day.Month()), day.Day())
				},
				RangeForDay: func(day time.Time) (time.Time, time.Time) {
					start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
					return start, start.Add(24 * time.Hour)
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
		if err := p.backfillFromDefault(ctx, rule, now); err != nil {
			p.logger.Warn("partition backfill failed",
				zap.String("parent", rule.Parent),
				zap.Error(err),
			)
			PartitionBackfillErrors.WithLabelValues(rule.Parent).Inc()
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

// backfillFromDefault detects rows stuck in the `<parent>_default` catch-all
// partition and materialises proper daily/monthly child partitions to absorb
// them. Solves the class of SLOW SQL caused by a non-empty _default forcing
// the planner to keep it in the Append (the planner cannot prune _default
// because it has no range bound, only a NOT-IN constraint synthesized from
// siblings; a non-empty _default adds buffer reads to every query).
//
// Invariants:
//   - Idempotent: CREATE TABLE IF NOT EXISTS + INSERT ... SELECT ... DELETE
//     inside a single transaction.
//   - Bounded: max `maxBackfillPartitionsPerRun` child partitions per sweep.
//   - Skip days older than `retention`: if the orphan window pre-dates the
//     retention cutoff we still materialise the child (the next sweep will
//     drop it). Keeping the row-move path uniform is safer than a "DELETE
//     orphan" branch.
//   - Guard against `DefaultTable` nil-rule or missing closure: rule without
//     backfill metadata is skipped (future-proof for non-partitioned rules).
func (p *PartitionDropper) backfillFromDefault(ctx context.Context, rule partitionRule, now time.Time) error {
	if rule.DefaultTable == "" || rule.NameForDay == nil || rule.RangeForDay == nil {
		return nil
	}

	// Probe existence of the default table first. In fresh envs / tests the
	// partitioned parent may not exist yet; silently skip.
	var exists bool
	if err := p.db.WithContext(ctx).Raw(
		`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = ?)`,
		rule.DefaultTable,
	).Scan(&exists).Error; err != nil {
		return fmt.Errorf("probe default partition: %w", err)
	}
	if !exists {
		return nil
	}

	// Group orphan rows by day (daily) or month-start (monthly) to know
	// which child partitions to materialise. We intentionally project via
	// DATE_TRUNC so both granularities share the query; the bucket is then
	// collapsed to day-level for daily or month-start for monthly.
	truncUnit := "day"
	if rule.Granularity == "monthly" {
		truncUnit = "month"
	}
	type bucket struct {
		Bucket time.Time `gorm:"column:bucket"`
		Cnt    int64     `gorm:"column:cnt"`
	}
	var buckets []bucket
	//nolint:gosec // table name comes from internal rule, not user input
	bucketSQL := fmt.Sprintf(
		`SELECT DATE_TRUNC('%s', created_at) AS bucket, COUNT(*) AS cnt
		   FROM %q
		  WHERE created_at IS NOT NULL
		  GROUP BY 1
		  ORDER BY 1`, truncUnit, rule.DefaultTable,
	)
	if err := p.db.WithContext(ctx).Raw(bucketSQL).Scan(&buckets).Error; err != nil {
		return fmt.Errorf("bucket orphan rows: %w", err)
	}
	if len(buckets) == 0 {
		return nil // default partition is empty — healthy state
	}

	created := 0
	moved := int64(0)
	for _, b := range buckets {
		if created >= maxBackfillPartitionsPerRun {
			p.logger.Info("partition backfill: per-run cap reached, deferring remainder",
				zap.String("parent", rule.Parent),
				zap.Int("cap", maxBackfillPartitionsPerRun),
			)
			break
		}

		day := b.Bucket.UTC()
		partitionName := rule.NameForDay(day)
		rangeStart, rangeEnd := rule.RangeForDay(day)
		if rangeStart.IsZero() || rangeEnd.IsZero() || !rangeStart.Before(rangeEnd) {
			p.logger.Warn("partition backfill: invalid range, skipping",
				zap.String("parent", rule.Parent),
				zap.Time("day", day),
			)
			continue
		}

		// Single transaction. Ordering is load-bearing on PostgreSQL 11+:
		//   1. DELETE rows in range from _default into a RETURNING CTE;
		//      stash them in a TEMP table (session-local, dropped at COMMIT).
		//   2. CREATE TABLE ... PARTITION OF parent FOR VALUES ... — now
		//      safe because _default no longer contains rows that would
		//      violate the new child's range constraint.
		//   3. INSERT rows from temp into PARENT — they route to the new
		//      child via the partition key.
		//
		// Doing CREATE first (intuitive order) fails with SQLSTATE 23514
		// ("updated partition constraint for default partition would be
		// violated") because PG re-validates _default against sibling
		// ranges at DDL time.
		err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Validate identifiers against the rule's own regex before
			// splicing into DDL. Defence-in-depth: the name comes from
			// NameForDay but an accidental typo elsewhere must still fail
			// closed.
			if rule.Re != nil && !rule.Re.MatchString(partitionName) {
				return fmt.Errorf("partition name %q failed regex validation", partitionName)
			}

			// Step 1: drain _default into a session-local temp table.
			drainSQL := fmt.Sprintf(
				`CREATE TEMP TABLE _backfill_staging ON COMMIT DROP AS
				 WITH deleted AS (
					DELETE FROM %q
					 WHERE created_at >= ?
					   AND created_at <  ?
					RETURNING *
				 )
				 SELECT * FROM deleted`,
				rule.DefaultTable,
			)
			drainRes := tx.Exec(drainSQL, rangeStart, rangeEnd)
			if drainRes.Error != nil {
				return fmt.Errorf("drain default into staging: %w", drainRes.Error)
			}
			// RowsAffected from CREATE TABLE AS is the row count on PG.
			drained := drainRes.RowsAffected

			// Step 2: create the child partition. _default is now empty in
			// this range so the constraint check passes.
			createSQL := fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %q PARTITION OF %q FOR VALUES FROM (%s) TO (%s)`,
				partitionName, rule.Parent,
				"'"+rangeStart.Format("2006-01-02 15:04:05")+"+00'",
				"'"+rangeEnd.Format("2006-01-02 15:04:05")+"+00'",
			)
			if err := tx.Exec(createSQL).Error; err != nil {
				return fmt.Errorf("create partition %s: %w", partitionName, err)
			}

			// Step 3: re-insert via parent (routes to new child). If the
			// partition already existed (IF NOT EXISTS branch) and was
			// empty for this range, this is still correct.
			insertSQL := fmt.Sprintf(
				`INSERT INTO %q SELECT * FROM _backfill_staging`,
				rule.Parent,
			)
			res := tx.Exec(insertSQL)
			if res.Error != nil {
				return fmt.Errorf("re-insert into parent for %s: %w", partitionName, res.Error)
			}
			if res.RowsAffected != drained {
				return fmt.Errorf(
					"row count mismatch for %s: drained=%d reinserted=%d",
					partitionName, drained, res.RowsAffected,
				)
			}
			moved += res.RowsAffected
			return nil
		})
		if err != nil {
			p.logger.Warn("partition backfill: txn failed",
				zap.String("parent", rule.Parent),
				zap.String("partition", partitionName),
				zap.Time("range_start", rangeStart),
				zap.Error(err),
			)
			PartitionBackfillErrors.WithLabelValues(rule.Parent).Inc()
			continue
		}

		created++
		PartitionBackfillCount.WithLabelValues(rule.Parent).Inc()
		p.logger.Info("partition backfilled from default",
			zap.String("parent", rule.Parent),
			zap.String("partition", partitionName),
			zap.Time("range_start", rangeStart),
			zap.Time("range_end", rangeEnd),
			zap.Int64("bucket_rows", b.Cnt),
		)
	}

	if created > 0 {
		p.logger.Info("partition backfill completed",
			zap.String("parent", rule.Parent),
			zap.Int("buckets", len(buckets)),
			zap.Int("partitions_created", created),
			zap.Int64("rows_moved", moved),
		)
	}
	return nil
}
