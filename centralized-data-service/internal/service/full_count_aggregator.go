package service

import (
	"context"
	"fmt"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// FullCountAggregator runs a daily pass over every active registry entry
// and captures two "absolute truth" counts per table:
//
//  1. Mongo EstimatedDocumentCount — pulls from the collection metadata
//     on the secondary, typically ~1ms regardless of N.
//  2. Postgres SELECT COUNT(*) on the destination table, with a
//     pg_class.reltuples fallback when the table has >10M rows (full
//     COUNT on a 50M-row partitioned table blocks for minutes).
//
// These full counts drive the FE "Total Source / Total Dest" columns
// (ADR v4 §2.7) and give the recon pipeline a drift-truth datum that
// doesn't depend on window semantics (which can be asymmetric between
// Mongo updated_at and Debezium source.ts_ms).
type FullCountAggregator struct {
	db           *gorm.DB
	dbReplica    *gorm.DB
	mongoClient  *mongo.Client
	registryRepo *repository.RegistryRepo
	cfg          FullCountAggregatorConfig
	logger       *zap.Logger
}

// FullCountAggregatorConfig groups the tunables; zero-value works fine
// for production (daily at 03:00 UTC with a 10M-row PG fast-path cutoff).
type FullCountAggregatorConfig struct {
	RunAt              string        // HH:MM UTC, default "03:00"
	PGFastPathCutoff   int64         // rows above this → use reltuples, default 10_000_000
	MongoCountTimeout  time.Duration // default 10s
	PGCountTimeout     time.Duration // default 2m (COUNT on partitioned tables)
	PerTableGap        time.Duration // sleep between tables, default 500ms
}

func (c *FullCountAggregatorConfig) applyDefaults() {
	if c.RunAt == "" {
		c.RunAt = "03:00"
	}
	if c.PGFastPathCutoff <= 0 {
		c.PGFastPathCutoff = 10_000_000
	}
	if c.MongoCountTimeout <= 0 {
		c.MongoCountTimeout = 10 * time.Second
	}
	if c.PGCountTimeout <= 0 {
		c.PGCountTimeout = 2 * time.Minute
	}
	if c.PerTableGap <= 0 {
		c.PerTableGap = 500 * time.Millisecond
	}
}

// NewFullCountAggregator wires the aggregator with primary DB (for
// writes to registry) + replica DB (for heavy COUNT(*) reads) + Mongo
// client. Pass `db` twice when no replica is configured — the aggregator
// writes are lightweight either way.
func NewFullCountAggregator(
	db, dbReplica *gorm.DB,
	mongoClient *mongo.Client,
	registryRepo *repository.RegistryRepo,
	cfg FullCountAggregatorConfig,
	logger *zap.Logger,
) *FullCountAggregator {
	cfg.applyDefaults()
	return &FullCountAggregator{
		db:           db,
		dbReplica:    dbReplica,
		mongoClient:  mongoClient,
		registryRepo: registryRepo,
		cfg:          cfg,
		logger:       logger,
	}
}

// Start spins the daily ticker. Blocks on ctx.Done(). Safe to call in a
// goroutine — the scheduler waits until the next `cfg.RunAt` wallclock
// slot before firing, so crash-restart cycles don't flood Mongo with
// redundant counts.
func (fa *FullCountAggregator) Start(ctx context.Context) {
	if fa == nil {
		return
	}
	fa.logger.Info("full-count aggregator starting",
		zap.String("run_at_utc", fa.cfg.RunAt),
		zap.Int64("pg_fast_path_cutoff", fa.cfg.PGFastPathCutoff),
	)

	// First tick: wait until the next cfg.RunAt. We sleep in 60s chunks
	// so ctx cancellation is responsive on shutdown.
	for {
		wait := durationUntilNext(fa.cfg.RunAt, time.Now().UTC())
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		fa.RunOnce(ctx)
	}
}

// RunOnce iterates all active registry entries and updates their
// full_source_count / full_dest_count / full_count_at. Exposed so
// operators can trigger a one-off via admin API if needed.
func (fa *FullCountAggregator) RunOnce(ctx context.Context) {
	started := time.Now()
	entries, err := fa.registryRepo.GetAllActive(ctx)
	if err != nil {
		fa.logger.Warn("full-count aggregator: registry load failed", zap.Error(err))
		return
	}
	fa.logger.Info("full-count aggregator pass starting", zap.Int("tables", len(entries)))
	var done, errs int
	for _, e := range entries {
		if err := fa.countOne(ctx, e); err != nil {
			errs++
			fa.logger.Warn("full-count aggregator: table failed",
				zap.String("target_table", e.TargetTable), zap.Error(err))
		} else {
			done++
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(fa.cfg.PerTableGap):
		}
	}
	fa.logger.Info("full-count aggregator pass complete",
		zap.Int("tables_counted", done),
		zap.Int("tables_failed", errs),
		zap.Duration("elapsed", time.Since(started)),
	)
}

// countOne executes Mongo + PG counts for a single registry entry and
// writes the result. Errors on either side are non-fatal for the other —
// we still persist the side that succeeded so partial data is useful.
func (fa *FullCountAggregator) countOne(ctx context.Context, e model.TableRegistry) error {
	var srcCount *int64
	var dstCount *int64

	// Mongo side — EstimatedDocumentCount is a ~1ms metadata read.
	if fa.mongoClient != nil {
		mctx, cancel := context.WithTimeout(ctx, fa.cfg.MongoCountTimeout)
		coll := fa.mongoClient.
			Database(e.SourceDB).
			Collection(e.SourceTable, options.Collection().SetReadPreference(readpref.Secondary()))
		n, err := coll.EstimatedDocumentCount(mctx)
		cancel()
		if err == nil {
			srcCount = &n
		} else {
			fa.logger.Warn("full-count: mongo EstimatedDocumentCount failed",
				zap.String("db", e.SourceDB),
				zap.String("collection", e.SourceTable),
				zap.Error(err))
		}
	}

	// PG side — use replica when configured. Fast-path via pg_class.reltuples
	// for tables expected to exceed the cutoff (we re-check reltuples on
	// every pass so the estimate tracks truth).
	if fa.dbReplica != nil {
		pctx, cancel := context.WithTimeout(ctx, fa.cfg.PGCountTimeout)
		var reltuples float64
		if err := fa.dbReplica.WithContext(pctx).Raw(
			"SELECT reltuples::bigint FROM pg_class WHERE relname = ?", e.TargetTable,
		).Scan(&reltuples).Error; err == nil && reltuples > float64(fa.cfg.PGFastPathCutoff) {
			est := int64(reltuples)
			dstCount = &est
			fa.logger.Debug("full-count: using reltuples fast path",
				zap.String("target_table", e.TargetTable),
				zap.Int64("estimated_rows", est))
		} else {
			// Exact count. Partitioned tables can block — the 2m timeout
			// above bounds the worst case; we log the error and move on.
			var exact int64
			q := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(e.TargetTable))
			if err := fa.dbReplica.WithContext(pctx).Raw(q).Scan(&exact).Error; err == nil {
				dstCount = &exact
			} else {
				fa.logger.Warn("full-count: PG COUNT(*) failed",
					zap.String("target_table", e.TargetTable), zap.Error(err))
			}
		}
		cancel()
	}

	// Write whatever we have. When both sides failed, skip the write so
	// we don't overwrite stale-but-useful values with NULLs.
	if srcCount == nil && dstCount == nil {
		return fmt.Errorf("both counts failed")
	}
	updates := map[string]interface{}{
		"full_count_at": time.Now().UTC(),
	}
	if srcCount != nil {
		updates["full_source_count"] = *srcCount
	}
	if dstCount != nil {
		updates["full_dest_count"] = *dstCount
	}
	if err := fa.db.WithContext(ctx).Model(&model.TableRegistry{}).
		Where("id = ?", e.ID).Updates(updates).Error; err != nil {
		return fmt.Errorf("registry update: %w", err)
	}
	return nil
}

// durationUntilNext computes the wall-clock wait until the next HH:MM
// slot (UTC). If the slot is earlier today → schedule for tomorrow.
// Malformed config falls through to 24h so the aggregator still runs
// once per day even when the operator typos `RunAt`.
func durationUntilNext(hhmm string, now time.Time) time.Duration {
	var h, m int
	if _, err := fmt.Sscanf(hhmm, "%d:%d", &h, &m); err != nil {
		return 24 * time.Hour
	}
	next := time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, time.UTC)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next.Sub(now)
}
