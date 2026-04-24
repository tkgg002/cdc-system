package service

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand"
	"os"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/pkgs/metrics"
	"centralized-data-service/pkgs/rediscache"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ReconCoreConfig groups tunables for the v3 window-based orchestrator.
// All fields optional — sensible defaults applied at construction.
type ReconCoreConfig struct {
	// Window sizing
	WindowSize         time.Duration // default 15m
	WindowLookback     time.Duration // default 7 * 24h
	WindowFreezeMargin time.Duration // default 5m (don't scan t > now - margin)

	// Thresholds
	CountDriftThreshold int64 // default 1 document

	// Tier 3 budget
	Tier3MaxDocsPerRun int64  // default 10_000_000
	Tier3OffPeakStart  string // default "02:00"
	Tier3OffPeakEnd    string // default "05:00"

	// Scheduling
	JitterMaxSeconds int // default 30

	// Leader election (Redis). When nil — scheduled recon runs on every
	// instance. Still per-table advisory-locked so there is no duplicate
	// work, but allocating work across instances is more efficient.
	LeaderLockTTL   time.Duration // default 60s
	LeaderHeartbeat time.Duration // default 20s
	LeaderLockKey   string        // default "recon:leader"

	// Instance identity for recon_runs.instance_id.
	InstanceID string
}

func (c *ReconCoreConfig) applyDefaults() {
	if c.WindowSize <= 0 {
		c.WindowSize = 15 * time.Minute
	}
	if c.WindowLookback <= 0 {
		c.WindowLookback = 7 * 24 * time.Hour
	}
	if c.WindowFreezeMargin <= 0 {
		c.WindowFreezeMargin = 5 * time.Minute
	}
	if c.CountDriftThreshold <= 0 {
		c.CountDriftThreshold = 1
	}
	if c.Tier3MaxDocsPerRun <= 0 {
		c.Tier3MaxDocsPerRun = 10_000_000
	}
	if c.Tier3OffPeakStart == "" {
		c.Tier3OffPeakStart = "02:00"
	}
	if c.Tier3OffPeakEnd == "" {
		c.Tier3OffPeakEnd = "05:00"
	}
	if c.JitterMaxSeconds <= 0 {
		c.JitterMaxSeconds = 30
	}
	if c.LeaderLockTTL <= 0 {
		c.LeaderLockTTL = 60 * time.Second
	}
	if c.LeaderHeartbeat <= 0 {
		c.LeaderHeartbeat = 20 * time.Second
	}
	if c.LeaderLockKey == "" {
		c.LeaderLockKey = "recon:leader"
	}
	if c.InstanceID == "" {
		host, _ := os.Hostname()
		c.InstanceID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}
}

// ReconCore orchestrates tiered reconciliation.
//
// v3 design (plan_data_integrity_v3 §2-§10):
//   - Tier 1: per-window count compare. No full-table COUNT.
//   - Tier 2: XOR-hash compare only on windows drifted by Tier 1.
//   - Tier 3: 256-bucket whole-table hash, budget-gated off-peak.
//   - Advisory lock `pg_try_advisory_lock(hashtext('recon_'||table))`
//     prevents double-runs within a single worker instance.
//   - Optional Redis leader election — scheduled recon only runs on
//     the leader; NATS-command-triggered recon runs on any instance
//     because the advisory lock still serialises per table.
//   - Every run is tracked in `recon_runs` (migration 011) with state
//     transitions running→success|failed|cancelled, plus metrics.
type ReconCore struct {
	sourceAgent   *ReconSourceAgent
	destAgent     *ReconDestAgent
	db            *gorm.DB
	mongoClient   *mongo.Client
	schemaAdapter *SchemaAdapter
	registryRepo  *repository.RegistryRepo
	redis         *rediscache.RedisCache
	cfg           ReconCoreConfig
	logger        *zap.Logger
}

// NewReconCore keeps the original constructor signature for backward
// compatibility. Uses default config and no Redis leader election.
func NewReconCore(
	sourceAgent *ReconSourceAgent,
	destAgent *ReconDestAgent,
	db *gorm.DB,
	mongoClient *mongo.Client,
	schemaAdapter *SchemaAdapter,
	registryRepo *repository.RegistryRepo,
	logger *zap.Logger,
) *ReconCore {
	return NewReconCoreWithConfig(sourceAgent, destAgent, db, mongoClient, schemaAdapter, registryRepo, nil, ReconCoreConfig{}, logger)
}

// NewReconCoreWithConfig gives callers full control — used by the
// worker_server wiring when Redis + explicit instance ID are available.
func NewReconCoreWithConfig(
	sourceAgent *ReconSourceAgent,
	destAgent *ReconDestAgent,
	db *gorm.DB,
	mongoClient *mongo.Client,
	schemaAdapter *SchemaAdapter,
	registryRepo *repository.RegistryRepo,
	redis *rediscache.RedisCache,
	cfg ReconCoreConfig,
	logger *zap.Logger,
) *ReconCore {
	cfg.applyDefaults()
	return &ReconCore{
		sourceAgent:   sourceAgent,
		destAgent:     destAgent,
		db:            db,
		mongoClient:   mongoClient,
		schemaAdapter: schemaAdapter,
		registryRepo:  registryRepo,
		redis:         redis,
		cfg:           cfg,
		logger:        logger,
	}
}

// ============================================================
// Per-table locking
// ============================================================

// withTableLock acquires `pg_try_advisory_lock(key)` with a stable
// CRC32-derived 32-bit key. This avoids per-cluster hashtext drift and
// reduces accidental collisions between different table names.
// Returns (acquired, unlock). Caller MUST call unlock() whether acquired
// or not (unlock is a no-op when not acquired). A non-acquired lock
// means a previous run is still in flight; the caller should skip.
func (rc *ReconCore) withTableLock(ctx context.Context, table string) (bool, func()) {
	key := advisoryLockKey("recon_" + table)
	var acquired bool
	if err := rc.db.WithContext(ctx).Raw(
		"SELECT pg_try_advisory_lock(?)", key,
	).Scan(&acquired).Error; err != nil {
		rc.logger.Warn("advisory lock acquire failed — assuming NOT acquired",
			zap.String("table", table), zap.Error(err))
		return false, func() {}
	}
	if !acquired {
		return false, func() {}
	}
	unlock := func() {
		rc.db.Exec("SELECT pg_advisory_unlock(?)", key)
	}
	return true, unlock
}

// ============================================================
// Leader election
// ============================================================

// AcquireLeader attempts to become the scheduled-recon leader. Returns a
// cancel func that releases the lock + stops the heartbeat. Safe to call
// when Redis is not configured — in that case it always returns
// (true, noop) so single-instance deployments work unchanged.
func (rc *ReconCore) AcquireLeader(ctx context.Context) (bool, func()) {
	if rc.redis == nil {
		return true, func() {}
	}
	client := rc.redis.RawClient()
	if client == nil {
		return true, func() {}
	}

	ok, err := client.SetNX(ctx, rc.cfg.LeaderLockKey, rc.cfg.InstanceID, rc.cfg.LeaderLockTTL).Result()
	if err != nil {
		rc.logger.Warn("leader acquire failed — skipping scheduled run",
			zap.Error(err))
		return false, func() {}
	}
	if !ok {
		return false, func() {}
	}

	hbCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(rc.cfg.LeaderHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				// Refresh TTL using an ownership-guarded Lua script so we
				// don't accidentally extend a stolen lock.
				script := `if redis.call("get", KEYS[1]) == ARGV[1] then
					return redis.call("pexpire", KEYS[1], ARGV[2])
				else return 0 end`
				_, _ = client.Eval(hbCtx, script,
					[]string{rc.cfg.LeaderLockKey},
					rc.cfg.InstanceID, int64(rc.cfg.LeaderLockTTL/time.Millisecond),
				).Result()
			}
		}
	}()

	release := func() {
		cancel()
		// Release lock only if still ours.
		script := `if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else return 0 end`
		_, _ = client.Eval(context.Background(), script,
			[]string{rc.cfg.LeaderLockKey}, rc.cfg.InstanceID).Result()
	}
	return true, release
}

// ============================================================
// Run-state tracking
// ============================================================

type reconRunHandle struct {
	id           string
	table        string
	tier         int
	started      time.Time
	docsScanned  int64
	windowsCount int
	mismatches   int
	healActions  int
}

// beginRun inserts a running row into recon_runs. The unique index
// `recon_runs_one_running` (table_name WHERE status='running') enforces
// a single in-flight row per table; the advisory lock above is the
// Go-side gate, this is the DB-side safety net.
func (rc *ReconCore) beginRun(ctx context.Context, table string, tier int) (*reconRunHandle, error) {
	h := &reconRunHandle{
		id:      uuid.NewString(),
		table:   table,
		tier:    tier,
		started: time.Now().UTC(),
	}
	err := rc.db.WithContext(ctx).Exec(
		`INSERT INTO recon_runs
			(id, table_name, tier, status, started_at, instance_id)
		 VALUES (?, ?, ?, 'running', ?, ?)`,
		h.id, table, tier, h.started, rc.cfg.InstanceID,
	).Error
	if err != nil {
		return nil, err
	}
	return h, nil
}

// finishRun marks the row terminal (success|failed|cancelled) and
// writes the Prometheus metrics. errMsg may be empty for success.
func (rc *ReconCore) finishRun(ctx context.Context, h *reconRunHandle, status, errMsg string) {
	finished := time.Now().UTC()
	dur := finished.Sub(h.started).Seconds()
	errMsg = SanitizeFreeformText(errMsg, 2000)

	updates := map[string]interface{}{
		"status":           status,
		"finished_at":      finished,
		"docs_scanned":     h.docsScanned,
		"windows_checked":  h.windowsCount,
		"mismatches_found": h.mismatches,
		"heal_actions":     h.healActions,
	}
	if errMsg != "" {
		updates["error_message"] = errMsg
	}
	if err := rc.db.WithContext(ctx).Exec(
		`UPDATE recon_runs
		 SET status=?, finished_at=?, docs_scanned=?, windows_checked=?,
		     mismatches_found=?, heal_actions=?, error_message=?
		 WHERE id=?`,
		updates["status"], updates["finished_at"], updates["docs_scanned"],
		updates["windows_checked"], updates["mismatches_found"],
		updates["heal_actions"], errMsg, h.id,
	).Error; err != nil {
		rc.logger.Warn("finish recon_runs row failed", zap.Error(err))
	}

	// Prometheus emit. Tier 3 tables can be huge → bucket by
	// `table_group` (prefix before first underscore or "other") to
	// avoid cardinality blow-up. Top-level `table` label kept only
	// for a manually curated allow-list — here we always emit
	// `table_group` and the explicit `table` is left for the
	// legacy ReconDrift gauge which already has per-table cardinality.
	group := tableGroup(h.table)
	metrics.ReconRunDuration.WithLabelValues(group, fmt.Sprintf("%d", h.tier)).Observe(dur)
	metrics.ReconMismatchCount.WithLabelValues(h.table, fmt.Sprintf("%d", h.tier)).Set(float64(h.mismatches))
	if status == "success" {
		metrics.ReconLastSuccessTs.WithLabelValues(h.table, fmt.Sprintf("%d", h.tier)).Set(float64(finished.Unix()))
	}
}

// tableGroup returns a low-cardinality group label for a table name.
// Strategy: take the portion before the first underscore; if absent,
// "other". Callers that want per-table metrics should use the
// explicit `table` label on mismatch gauges instead.
func tableGroup(table string) string {
	idx := strings.Index(table, "_")
	if idx <= 0 {
		return "other"
	}
	return table[:idx]
}

// ============================================================
// Windowing
// ============================================================

// window is a half-open interval [Lo, Hi).
type window struct {
	Lo time.Time
	Hi time.Time
}

// buildWindows slices [from, to) into N windows of size cfg.WindowSize.
// If the range is not a clean multiple of WindowSize the last window
// ends exactly at `to`.
func (rc *ReconCore) buildWindows(from, to time.Time) []window {
	var ws []window
	if to.Before(from) {
		return ws
	}
	cur := from
	for cur.Before(to) {
		next := cur.Add(rc.cfg.WindowSize)
		if next.After(to) {
			next = to
		}
		ws = append(ws, window{Lo: cur, Hi: next})
		cur = next
	}
	return ws
}

// pickScanRange resolves the upper watermark (min of both sides' max
// source ts, clamped to now − freeze margin) and the lower watermark
// (upper − lookback).
// tsField returns the entry's Mongo timestamp field (Bug B). Helper
// keeps the nil-pointer dereference in one place + lets us unit-test
// the default fallback behaviour when registry value is not set.
func tsField(entry model.TableRegistry) string {
	if entry.TimestampField == nil {
		return ""
	}
	return *entry.TimestampField
}

func (rc *ReconCore) pickScanRange(ctx context.Context, entry model.TableRegistry) (time.Time, time.Time, error) {
	srcMax, err := rc.sourceAgent.MaxWindowTs(ctx, entry.SourceURL, entry.SourceDB, entry.SourceTable, tsField(entry))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("source max ts: %w", err)
	}
	dstMax, err := rc.destAgent.MaxWindowTs(ctx, entry.TargetTable)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("dest max ts: %w", err)
	}

	// Upper watermark = min(max(src), max(dst), now - freezeMargin)
	nowFreeze := time.Now().UTC().Add(-rc.cfg.WindowFreezeMargin)
	upper := nowFreeze
	if !srcMax.IsZero() && srcMax.Before(upper) {
		upper = srcMax
	}
	if !dstMax.IsZero() && dstMax.Before(upper) {
		upper = dstMax
	}
	lower := upper.Add(-rc.cfg.WindowLookback)
	return lower, upper, nil
}

// ============================================================
// Tier 1 — count per window
// ============================================================

// RunTier1 performs window-based count comparison for a single table.
// No full-table COUNT is issued — we count each 15-minute slice in a
// range-indexed query on both sides. Returns a drift snapshot that
// Tier 2 can drill into.
func (rc *ReconCore) RunTier1(ctx context.Context, entry model.TableRegistry) *model.ReconciliationReport {
	acquired, unlock := rc.withTableLock(ctx, entry.TargetTable)
	defer unlock()
	if !acquired {
		rc.logger.Info("tier1 skipped — previous run ongoing",
			zap.String("table", entry.TargetTable))
		return rc.errorReport(entry, "count", 1, fmt.Errorf("previous run ongoing"))
	}

	handle, err := rc.beginRun(ctx, entry.TargetTable, 1)
	if err != nil {
		rc.logger.Error("tier1 beginRun failed", zap.Error(err))
		return rc.errorReport(entry, "count", 1, err)
	}

	status := "success"
	defer func() {
		rc.finishRun(ctx, handle, status, "")
	}()

	lo, hi, err := rc.pickScanRange(ctx, entry)
	if err != nil {
		status = "failed"
		return rc.errorReport(entry, "count", 1, err)
	}
	windows := rc.buildWindows(lo, hi)
	handle.windowsCount = len(windows)

	type winDrift struct {
		Lo          time.Time `json:"lo"`
		Hi          time.Time `json:"hi"`
		SourceCount int64     `json:"source_count"`
		DestCount   int64     `json:"dest_count"`
	}
	var drifted []winDrift
	var totalSrc, totalDst int64

	// Migration 017: CountInWindowWithFallback probes candidate fields
	// when the primary returns 0 — prevents silent drift reports when
	// registry `timestamp_field` is stale relative to actual collection
	// schema (discovered runtime, auto-correcting).
	fallbacks := entry.GetCandidates()
	for _, w := range windows {
		srcCount, fieldUsed, err := rc.sourceAgent.CountInWindowWithFallback(
			ctx, entry.SourceURL, entry.SourceDB, entry.SourceTable,
			tsField(entry), w.Lo, w.Hi, fallbacks,
		)
		if err != nil {
			status = "failed"
			return rc.errorReport(entry, "count", 1, fmt.Errorf("src count window %v: %w", w.Lo, err))
		}
		_ = fieldUsed // future: persist per-window fieldUsed for audit
		dstCount, err := rc.destAgent.CountInWindow(ctx, entry.TargetTable, w.Lo, w.Hi)
		if err != nil {
			status = "failed"
			return rc.errorReport(entry, "count", 1, fmt.Errorf("dst count window %v: %w", w.Lo, err))
		}
		totalSrc += srcCount
		totalDst += dstCount
		handle.docsScanned += srcCount + dstCount

		if abs(srcCount-dstCount) >= rc.cfg.CountDriftThreshold {
			drifted = append(drifted, winDrift{Lo: w.Lo, Hi: w.Hi, SourceCount: srcCount, DestCount: dstCount})
		}
	}

	handle.mismatches = len(drifted)

	statusStr := "ok"
	if len(drifted) > 0 {
		statusStr = "drift"
	}

	driftJSON, _ := json.Marshal(drifted)
	duration := int(time.Since(handle.started).Milliseconds())
	srcTotal := totalSrc
	report := &model.ReconciliationReport{
		TargetTable: entry.TargetTable,
		SourceDB:    entry.SourceDB,
		SourceCount: &srcTotal,
		DestCount:   totalDst,
		Diff:        totalSrc - totalDst,
		StaleCount:  len(drifted),
		StaleIDs:    driftJSON, // reuse stale_ids column as drifted-windows payload for tier 1/2
		CheckType:   "count_windowed",
		Status:      statusStr,
		Tier:        1,
		DurationMs:  &duration,
		CheckedAt:   time.Now(),
	}
	rc.db.Create(report)

	metrics.ReconDrift.WithLabelValues(entry.TargetTable, "1").Set(float64(len(drifted)))

	rc.logger.Info("tier1 count_windowed",
		zap.String("table", entry.TargetTable),
		zap.Int("windows", len(windows)),
		zap.Int("drifted_windows", len(drifted)),
		zap.Int64("total_src", totalSrc),
		zap.Int64("total_dst", totalDst),
	)
	return report
}

// ============================================================
// Tier 2 — XOR-hash per drifted window + drill-down ID diff
// ============================================================

// RunTier2 picks up drifted windows detected by Tier 1 (or scans the
// same range if Tier 1 has not run yet) and for each such window:
//  1. HashWindow on both sides (count + xor)
//  2. If count / xor mismatch → ListIDsInWindow on both sides, diff.
//
// Writes mismatches to cdc_reconciliation_report with status=drift.
func (rc *ReconCore) RunTier2(ctx context.Context, entry model.TableRegistry) *model.ReconciliationReport {
	acquired, unlock := rc.withTableLock(ctx, entry.TargetTable)
	defer unlock()
	if !acquired {
		rc.logger.Info("tier2 skipped — previous run ongoing",
			zap.String("table", entry.TargetTable))
		return rc.errorReport(entry, "hash_window", 2, fmt.Errorf("previous run ongoing"))
	}

	handle, err := rc.beginRun(ctx, entry.TargetTable, 2)
	if err != nil {
		return rc.errorReport(entry, "hash_window", 2, err)
	}
	status := "success"
	defer func() { rc.finishRun(ctx, handle, status, "") }()

	lo, hi, err := rc.pickScanRange(ctx, entry)
	if err != nil {
		status = "failed"
		return rc.errorReport(entry, "hash_window", 2, err)
	}
	windows := rc.buildWindows(lo, hi)
	handle.windowsCount = len(windows)

	var missingFromDest []string
	var missingFromSrc []string
	var driftedWindows int

	for _, w := range windows {
		srcRes, err := rc.sourceAgent.HashWindow(ctx, entry.SourceURL, entry.SourceDB, entry.SourceTable, tsField(entry), w.Lo, w.Hi)
		if err != nil {
			status = "failed"
			return rc.errorReport(entry, "hash_window", 2, fmt.Errorf("src hash window %v: %w", w.Lo, err))
		}
		dstRes, err := rc.destAgent.HashWindow(ctx, entry.TargetTable, entry.PrimaryKeyField, w.Lo, w.Hi)
		if err != nil {
			status = "failed"
			return rc.errorReport(entry, "hash_window", 2, fmt.Errorf("dst hash window %v: %w", w.Lo, err))
		}
		handle.docsScanned += srcRes.Count + dstRes.Count

		if srcRes.Count == dstRes.Count && srcRes.XorHash == dstRes.XorHash {
			continue // window clean
		}
		driftedWindows++

		// Drill-down: collect IDs on both sides.
		srcIDs, err := rc.sourceAgent.ListIDsInWindow(ctx, entry.SourceURL, entry.SourceDB, entry.SourceTable, tsField(entry), w.Lo, w.Hi)
		if err != nil {
			rc.logger.Warn("src list ids failed", zap.Error(err))
			continue
		}
		dstIDs, err := rc.destAgent.ListIDsInWindow(ctx, entry.TargetTable, entry.PrimaryKeyField, w.Lo, w.Hi)
		if err != nil {
			rc.logger.Warn("dst list ids failed", zap.Error(err))
			continue
		}
		mFromDst, mFromSrc := diffIDs(srcIDs, dstIDs)
		missingFromDest = append(missingFromDest, mFromDst...)
		missingFromSrc = append(missingFromSrc, mFromSrc...)
	}

	handle.mismatches = len(missingFromDest) + len(missingFromSrc)

	missingJSON, _ := json.Marshal(missingFromDest)
	staleJSON, _ := json.Marshal(map[string][]string{
		"missing_from_dest": missingFromDest,
		"missing_from_src":  missingFromSrc,
	})

	statusStr := "ok"
	if driftedWindows > 0 {
		statusStr = "drift"
	}

	duration := int(time.Since(handle.started).Milliseconds())
	report := &model.ReconciliationReport{
		TargetTable:  entry.TargetTable,
		SourceDB:     entry.SourceDB,
		MissingCount: len(missingFromDest),
		MissingIDs:   missingJSON,
		StaleCount:   driftedWindows,
		StaleIDs:     staleJSON,
		CheckType:    "hash_window",
		Status:       statusStr,
		Tier:         2,
		DurationMs:   &duration,
		CheckedAt:    time.Now(),
	}
	rc.db.Create(report)

	metrics.ReconDrift.WithLabelValues(entry.TargetTable, "2").Set(float64(handle.mismatches))

	rc.logger.Info("tier2 hash_window",
		zap.String("table", entry.TargetTable),
		zap.Int("windows", len(windows)),
		zap.Int("drifted_windows", driftedWindows),
		zap.Int("missing_from_dest", len(missingFromDest)),
		zap.Int("missing_from_src", len(missingFromSrc)),
	)
	return report
}

// ============================================================
// Tier 3 — 256-bucket whole-table hash with budget + off-peak
// ============================================================

// RunTier3 computes the 256-bucket fingerprint on both sides and diffs
// them. Budget: skip historical (full scan) when estimated rows exceed
// Tier3MaxDocsPerRun; in that case fall back to the 7-day window scan
// (delegates to RunTier2 for the drifted buckets). Respects off-peak
// window [Tier3OffPeakStart, Tier3OffPeakEnd] unless the caller passes
// force=true via the Force field in the handle.
func (rc *ReconCore) RunTier3(ctx context.Context, entry model.TableRegistry) *model.ReconciliationReport {
	acquired, unlock := rc.withTableLock(ctx, entry.TargetTable)
	defer unlock()
	if !acquired {
		return rc.errorReport(entry, "bucket_hash", 3, fmt.Errorf("previous run ongoing"))
	}

	handle, err := rc.beginRun(ctx, entry.TargetTable, 3)
	if err != nil {
		return rc.errorReport(entry, "bucket_hash", 3, err)
	}
	status := "success"
	defer func() { rc.finishRun(ctx, handle, status, "") }()

	if !withinOffPeak(time.Now().UTC(), rc.cfg.Tier3OffPeakStart, rc.cfg.Tier3OffPeakEnd) {
		rc.logger.Info("tier3 skipped — outside off-peak window",
			zap.String("table", entry.TargetTable),
			zap.String("off_peak", rc.cfg.Tier3OffPeakStart+"-"+rc.cfg.Tier3OffPeakEnd),
		)
		status = "cancelled"
		return rc.errorReport(entry, "bucket_hash", 3, fmt.Errorf("outside off-peak window"))
	}

	// Budget check: peek at dest count to estimate cost.
	destCount, err := rc.destAgent.CountRows(ctx, entry.TargetTable, entry.PrimaryKeyField)
	if err != nil {
		status = "failed"
		return rc.errorReport(entry, "bucket_hash", 3, err)
	}
	if destCount > rc.cfg.Tier3MaxDocsPerRun {
		rc.logger.Warn("tier3 budget exceeded — falling back to 7-day window scan",
			zap.String("table", entry.TargetTable),
			zap.Int64("dest_count", destCount),
			zap.Int64("budget", rc.cfg.Tier3MaxDocsPerRun),
		)
		// Degrade gracefully into tier 2 for recent windows — this keeps
		// the budget while still catching fresh drift.
		handle.mismatches = 0
		return rc.RunTier2(ctx, entry)
	}

	srcBuckets, err := rc.sourceAgent.BucketHash(ctx, entry.SourceURL, entry.SourceDB, entry.SourceTable, tsField(entry))
	if err != nil {
		status = "failed"
		return rc.errorReport(entry, "bucket_hash", 3, err)
	}
	dstBuckets, err := rc.destAgent.BucketHash(ctx, entry.TargetTable, entry.PrimaryKeyField)
	if err != nil {
		status = "failed"
		return rc.errorReport(entry, "bucket_hash", 3, err)
	}
	handle.docsScanned = srcBuckets.Total + dstBuckets.Total

	var driftedBuckets []int
	for i := 0; i < 256; i++ {
		if srcBuckets.Buckets[i] != dstBuckets.Buckets[i] {
			driftedBuckets = append(driftedBuckets, i)
		}
	}
	handle.mismatches = len(driftedBuckets)

	statusStr := "ok"
	if len(driftedBuckets) > 0 {
		statusStr = "drift"
	}

	payload := map[string]interface{}{
		"drifted_buckets": driftedBuckets,
		"src_total":       srcBuckets.Total,
		"dst_total":       dstBuckets.Total,
	}
	staleJSON, _ := json.Marshal(payload)
	duration := int(time.Since(handle.started).Milliseconds())
	srcBucketTotal := srcBuckets.Total
	report := &model.ReconciliationReport{
		TargetTable: entry.TargetTable,
		SourceDB:    entry.SourceDB,
		SourceCount: &srcBucketTotal,
		DestCount:   dstBuckets.Total,
		Diff:        srcBuckets.Total - dstBuckets.Total,
		StaleCount:  len(driftedBuckets),
		StaleIDs:    staleJSON,
		CheckType:   "bucket_hash",
		Status:      statusStr,
		Tier:        3,
		DurationMs:  &duration,
		CheckedAt:   time.Now(),
	}
	rc.db.Create(report)

	metrics.ReconDrift.WithLabelValues(entry.TargetTable, "3").Set(float64(len(driftedBuckets)))

	rc.logger.Info("tier3 bucket_hash",
		zap.String("table", entry.TargetTable),
		zap.Int("drifted_buckets", len(driftedBuckets)),
		zap.Int64("src_total", srcBuckets.Total),
		zap.Int64("dst_total", dstBuckets.Total),
	)
	return report
}

// withinOffPeak returns true when `now` falls inside the [start, end]
// clock window, parsed as HH:MM. Cross-midnight windows supported
// (e.g., 22:00-05:00).
func withinOffPeak(now time.Time, start, end string) bool {
	hhmm := func(s string) (int, int, bool) {
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			return 0, 0, false
		}
		var h, m int
		if _, err := fmt.Sscanf(parts[0], "%d", &h); err != nil {
			return 0, 0, false
		}
		if _, err := fmt.Sscanf(parts[1], "%d", &m); err != nil {
			return 0, 0, false
		}
		return h, m, true
	}
	sh, sm, ok1 := hhmm(start)
	eh, em, ok2 := hhmm(end)
	if !ok1 || !ok2 {
		return true // fail-open: if config malformed, allow the run
	}
	curM := now.Hour()*60 + now.Minute()
	startM := sh*60 + sm
	endM := eh*60 + em
	if startM <= endM {
		return curM >= startM && curM < endM
	}
	// cross-midnight
	return curM >= startM || curM < endM
}

// ============================================================
// CheckAll — scheduled entry point
// ============================================================

// CheckAll iterates every active registry entry, applies jitter +
// staggered spacing, and runs Tier 1 per table. Returns the list of
// produced reports. Leader election is applied at the top level: if
// another worker holds the leader lock, this call returns nil.
func (rc *ReconCore) CheckAll(ctx context.Context) []*model.ReconciliationReport {
	isLeader, release := rc.AcquireLeader(ctx)
	defer release()
	if !isLeader {
		rc.logger.Info("recon CheckAll — not leader, skipping")
		return nil
	}

	entries, err := rc.registryRepo.GetAllActive(ctx)
	if err != nil {
		rc.logger.Error("recon CheckAll: registry load failed", zap.Error(err))
		return nil
	}

	// Stagger 200 tables over 5 minutes → ~1.5s apart. Plus per-table
	// random jitter up to cfg.JitterMaxSeconds.
	spread := 5 * time.Minute
	step := time.Duration(0)
	if len(entries) > 0 {
		step = spread / time.Duration(len(entries))
	}

	var reports []*model.ReconciliationReport
	now := time.Now()
	for i, entry := range entries {
		if rc.schemaAdapter.GetSchema(entry.TargetTable) == nil {
			continue // table not materialised yet
		}
		// Stagger + jitter
		if i > 0 && step > 0 {
			time.Sleep(step)
		}
		jitter := time.Duration(rand.Intn(rc.cfg.JitterMaxSeconds+1)) * time.Second
		time.Sleep(jitter)

		report := rc.RunTier1(ctx, entry)
		if report == nil {
			continue
		}
		reports = append(reports, report)

		// Feedback loop — update registry sync_status.
		updates := map[string]interface{}{"last_recon_at": now}
		switch report.Status {
		case "ok":
			updates["sync_status"] = "healthy"
			updates["recon_drift"] = 0
		case "drift":
			updates["sync_status"] = "drift"
			updates["recon_drift"] = report.Diff
		case "error":
			updates["sync_status"] = "source_error"
		}
		rc.db.Model(&model.TableRegistry{}).Where("target_table = ?", report.TargetTable).Updates(updates)
	}
	return reports
}

// ============================================================
// Heal — REMOVED in v3 root-cause fix (2026-04-17)
// ============================================================
//
// The legacy `ReconCore.Heal(ctx, entry, missingIDs)` walked `missingIDs`
// one row at a time, issued a Mongo FindOne + PG upsert per record AND
// wrote one `cdc_activity_log` row per record. On a corrupted Tier 2
// report that listed e.g. 1,713 rows the audit table exploded by 1,713
// rows per heal trigger, which made the CMS audit view unreadable.
//
// The v3 `ReconHealer.HealWindow` pathway supersedes it entirely:
//   - Audit batching (run_started + run_completed = 2 rows, regardless
//     of record count; plus at most a sample of per-record rows when
//     the `AuditSampleRate` is set).
//   - Batched `$in` Mongo reads instead of FindOne loops.
//   - OCC guard on `_source_ts` + per-table sensitive-field masking.
//   - Optional Debezium incremental snapshot signal (Phase A).
//
// Callsites audited on 2026-04-17: only `recon_handler.HandleReconHeal`
// referenced `rc.Heal`, and that fallback was deleted in the same
// commit. `rc.mongoClient` remains on the struct for future uses (e.g.
// source sampling) and to preserve the NewReconCore constructor
// signature.

// ============================================================
// Helpers
// ============================================================

// errorReport writes a `status='error'` row with structured ErrorCode
// classification (Migration 017 / ADR v4 §2.3). SourceCount is left NULL
// so FE can distinguish "query failed" from "query OK, 0 rows". Legacy
// error rows created before migration 017 will have NULL error_code
// and continue to render as generic "Lỗi nguồn" — intentional, no
// backfill needed.
func (rc *ReconCore) errorReport(entry model.TableRegistry, checkType string, tier int, err error) *model.ReconciliationReport {
	errMsg := SanitizeFreeformText(err.Error(), 2000)
	code := classifyMongoError(err)
	if code == "" {
		code = ErrCodeUnknown
	}
	report := &model.ReconciliationReport{
		TargetTable:  entry.TargetTable,
		SourceDB:     entry.SourceDB,
		CheckType:    checkType,
		Status:       "error",
		Tier:         tier,
		ErrorMessage: &errMsg,
		ErrorCode:    code,
		CheckedAt:    time.Now(),
	}
	rc.db.Create(report)
	return report
}

// diffIDs returns (missingFromB, missingFromA).
func diffIDs(a, b []string) ([]string, []string) {
	setA := make(map[string]struct{}, len(a))
	for _, s := range a {
		setA[s] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, s := range b {
		setB[s] = struct{}{}
	}
	var fromB, fromA []string
	for s := range setA {
		if _, ok := setB[s]; !ok {
			fromB = append(fromB, s)
		}
	}
	for s := range setB {
		if _, ok := setA[s]; !ok {
			fromA = append(fromA, s)
		}
	}
	return fromB, fromA
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func md5Hex(data []byte) string {
	return fmt.Sprintf("%x", md5.Sum(data))
}

// unwrapBSONValue converts BSON native types to Go/JSON-friendly types.
func unwrapBSONValue(v interface{}) interface{} {
	switch val := v.(type) {
	case primitive.ObjectID:
		return val.Hex()
	case primitive.DateTime:
		return val.Time().Format(time.RFC3339Nano)
	case primitive.A:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = unwrapBSONValue(item)
		}
		return result
	case bson.M:
		result := make(map[string]interface{}, len(val))
		for k, item := range val {
			result[k] = unwrapBSONValue(item)
		}
		return result
	case bson.D:
		result := make(map[string]interface{}, len(val))
		for _, elem := range val {
			result[elem.Key] = unwrapBSONValue(elem.Value)
		}
		return result
	case int32:
		return int64(val)
	case primitive.Decimal128:
		return val.String()
	default:
		return v
	}
}

// fnvHash32 — utility used by a legacy helper that needs a deterministic
// small fingerprint; kept for completeness.
func fnvHash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func advisoryLockKey(name string) int64 {
	return int64(crc32.ChecksumIEEE([]byte(name)))
}

// ensure we use the helper (prevents goimports from removing the import
// in the tight loop above).
var _ = fnvHash32
