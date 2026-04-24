package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/pkgs/metrics"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Heal v3 implementation (plan v3 §6 + §7).
//
// Goals:
//  1. Batched Mongo fetch ($in size 500) → O(fetch/batch) rather than
//     O(N) single-doc roundtrips.
//  2. Version-aware OCC via SchemaAdapter.BuildUpsertSQL — the
//     `_source_ts` guard ensures heal NEVER overwrites a row whose PG
//     copy is newer than the Mongo doc we just fetched.
//  3. Audit log batched — one INSERT per 100 heal actions, not one per
//     record. Reuses `cdc_activity_log` (operation="recon-heal") so no
//     new migration is required.
//  4. Prefer Debezium incremental snapshot signal when a connector is
//     healthy; fall back to direct `$in` heal otherwise.
//
// All heal events emit `cdc_recon_heal_actions_total{table,action}`
// with action ∈ upsert|skip|error.

// HealResult captures the outcome of a single HealMissingIDs call.
type HealResult struct {
	Table         string
	Requested     int
	Upserted      int
	Deleted       int
	Skipped       int // OCC guard rejected, or doc missing in source
	Errored       int
	UsedSignal    bool // true when Debezium signal took the lead path
	DurationMs    int64
	SignalID      string
	AuditFlushCnt int
	RunID         string // heal run correlation id — echoes into audit rows
}

// healAuditBatcher buffers per-record audit entries and flushes them in
// multi-row INSERT batches. Critical for prod scale (50M records → used
// to be 50M audit rows; now capped at ≤ 102 rows per heal run via the
// sampling + summary strategy below).
//
// Strategy (plan v3 §6, re-implemented after per-record spam regression):
//  1. "skip"   — counted in memory only, NO per-record row. Skip carries
//     no diagnostic value (destination already newer); the aggregate
//     count lands in the run_completed summary.
//  2. "upsert" — buffered up to MaxSampleUpsert rows per run (default
//     100). Additional upserts are counted but NOT buffered. Operators
//     get a representative sample; auditors get exact counts.
//  3. "error"  — ALWAYS buffered. Errors are rare and each one is
//     actionable; sampling them defeats observability.
//  4. Buffer flushes whenever it reaches MaxBatch (default 100), via a
//     single GORM CreateInBatches call. Final flush happens in End().
//  5. Begin() writes a `run_started` row immediately; End() writes a
//     `run_completed` row with aggregate counters and duration.
//
// Total rows per run ≤ 1 (run_started) + MaxSampleUpsert + errorCount
// + 1 (run_completed). For a healthy 10K-upsert heal: ≈ 102 rows.
type healAuditBatcher struct {
	db       *gorm.DB
	logger   *zap.Logger
	mu       sync.Mutex
	buf      []model.ActivityLog
	maxBatch int
	// maxSampleUpsert caps how many "upsert" rows we persist per run.
	// Errors are always persisted (no sampling).
	maxSampleUpsert int

	table        string
	runID        string
	startedAt    time.Time
	upsertCount  int
	skipCount    int
	errorCount   int
	upsertLogged int // sampled-through count
	flushCount   int
}

func newHealAuditBatcher(db *gorm.DB, logger *zap.Logger, maxBatch, maxSampleUpsert int) *healAuditBatcher {
	if maxBatch <= 0 {
		maxBatch = 100
	}
	if maxSampleUpsert <= 0 {
		maxSampleUpsert = 100
	}
	return &healAuditBatcher{
		db:              db,
		logger:          logger,
		maxBatch:        maxBatch,
		maxSampleUpsert: maxSampleUpsert,
		buf:             make([]model.ActivityLog, 0, maxBatch),
	}
}

// Begin writes the run_started summary row. Not batched — one row per
// heal run is cheap and gives operators a head marker.
func (b *healAuditBatcher) Begin(ctx context.Context, runID, table string) {
	b.mu.Lock()
	b.runID = runID
	b.table = table
	b.startedAt = time.Now()
	b.mu.Unlock()

	details, _ := json.Marshal(map[string]interface{}{
		"action": "run_started",
		"run_id": runID,
	})
	now := time.Now()
	row := model.ActivityLog{
		Operation:   "recon-heal",
		TargetTable: table,
		Status:      "running",
		Details:     details,
		TriggeredBy: "recon-healer",
		StartedAt:   now,
		CompletedAt: &now,
	}
	if err := b.db.WithContext(ctx).Create(&row).Error; err != nil && b.logger != nil {
		b.logger.Warn("heal audit run_started insert failed",
			zap.String("table", table), zap.String("run_id", runID), zap.Error(err))
	}
}

// Record classifies one heal action. Skips never produce rows; upserts
// are sampled; errors are always persisted. Counters update
// unconditionally.
func (b *healAuditBatcher) Record(ctx context.Context, action, recordID string, sourceTsMs int64, errMsg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch action {
	case "skip":
		b.skipCount++
		return // aggregate only — NO per-record row
	case "upsert":
		b.upsertCount++
		if b.upsertLogged >= b.maxSampleUpsert {
			return // sampled out — counter keeps growing, row suppressed
		}
		b.upsertLogged++
	case "error":
		b.errorCount++
	default:
		// Unknown action — log it anyway; cheap and rare.
	}

	detailsMap := map[string]interface{}{
		"action":    action,
		"record_id": recordID,
		"run_id":    b.runID,
	}
	if sourceTsMs > 0 {
		detailsMap["source_ts_ms"] = sourceTsMs
	}
	if errMsg != "" {
		detailsMap["error"] = errMsg
	}
	details, _ := json.Marshal(detailsMap)

	status := "success"
	if action == "error" {
		status = "error"
	}
	now := time.Now()
	b.buf = append(b.buf, model.ActivityLog{
		Operation:   "recon-heal",
		TargetTable: b.table,
		Status:      status,
		Details:     details,
		TriggeredBy: "recon-healer",
		StartedAt:   now,
		CompletedAt: &now,
	})

	if len(b.buf) >= b.maxBatch {
		b.flushLocked(ctx)
	}
}

// Flush drains whatever is buffered. Safe to call at any point.
func (b *healAuditBatcher) Flush(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked(ctx)
}

func (b *healAuditBatcher) flushLocked(ctx context.Context) {
	if len(b.buf) == 0 {
		return
	}
	// GORM CreateInBatches emits multi-row INSERTs; with the whole buffer
	// sized ≤ maxBatch this is a single round-trip.
	if err := b.db.WithContext(ctx).CreateInBatches(&b.buf, b.maxBatch).Error; err != nil && b.logger != nil {
		b.logger.Warn("heal audit batch flush failed",
			zap.Int("batch", len(b.buf)),
			zap.String("table", b.table),
			zap.Error(err),
		)
	}
	b.flushCount++
	b.buf = b.buf[:0]
}

// End drains the buffer and writes the run_completed summary. Must be
// called exactly once per Begin.
func (b *healAuditBatcher) End(ctx context.Context, status string, runErr error, usedSignal bool, signalID string) {
	b.Flush(ctx)

	b.mu.Lock()
	startedAt := b.startedAt
	runID := b.runID
	table := b.table
	upsertCount := b.upsertCount
	skipCount := b.skipCount
	errorCount := b.errorCount
	flushCount := b.flushCount
	b.mu.Unlock()

	errMsg := ""
	if runErr != nil {
		errMsg = runErr.Error()
	}
	duration := time.Since(startedAt)
	durMs := duration.Milliseconds()

	detailsMap := map[string]interface{}{
		"action":         "run_completed",
		"run_id":         runID,
		"status":         status,
		"upserted_count": upsertCount,
		"skipped_count":  skipCount,
		"errored_count":  errorCount,
		"duration_ms":    durMs,
		"audit_flushes":  flushCount,
		"used_signal":    usedSignal,
	}
	if signalID != "" {
		detailsMap["signal_id"] = signalID
	}
	if errMsg != "" {
		detailsMap["error"] = errMsg
	}
	details, _ := json.Marshal(detailsMap)

	durInt := int(durMs)
	now := time.Now()
	row := model.ActivityLog{
		Operation:   "recon-heal",
		TargetTable: table,
		Status:      status,
		Details:     details,
		TriggeredBy: "recon-healer",
		StartedAt:   startedAt,
		CompletedAt: &now,
		DurationMs:  &durInt,
	}
	if errMsg != "" {
		row.ErrorMessage = &errMsg
	}
	if err := b.db.WithContext(ctx).Create(&row).Error; err != nil && b.logger != nil {
		b.logger.Warn("heal audit run_completed insert failed",
			zap.String("table", table), zap.String("run_id", runID), zap.Error(err))
	}
}

// FlushCount returns the number of batch inserts performed — exposed
// for HealResult.AuditFlushCnt book-keeping.
func (b *healAuditBatcher) FlushCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushCount
}

// ReconHealer orchestrates the heal phases. It is decoupled from
// ReconCore so CMS / NATS handlers / unit tests can invoke it without
// pulling in the full recon orchestration.
type ReconHealer struct {
	mongoClient   *mongo.Client
	db            *gorm.DB
	schemaAdapter *SchemaAdapter
	signal        *DebeziumSignalClient // optional — nil disables Signal path
	masking       *MaskingService
	logger        *zap.Logger
	cfg           ReconHealerConfig
}

// ReconHealerConfig exposes the handful of tunables we need. All
// optional.
type ReconHealerConfig struct {
	BatchSize      int           // default 500 (plan v3 §6)
	AuditFlushSize int           // default 100 (plan v3 §6)
	QueryTimeout   time.Duration // per-chunk ctx timeout, default 60s
	// ForceDirect — when true, skip the Signal path even when a healthy
	// connector is available. Useful for tests and targeted re-heal.
	ForceDirect bool
	// SensitiveFieldMask provides fallback default keywords only when a
	// shared MaskingService instance is not injected by the caller.
	SensitiveFieldMask []string
}

func (c *ReconHealerConfig) applyDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.AuditFlushSize <= 0 {
		c.AuditFlushSize = 100
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 60 * time.Second
	}
}

// NewReconHealer wires the heal pipeline. `signal` may be nil; when nil,
// all heal calls go through the direct $in path.
func NewReconHealer(
	db *gorm.DB,
	mongoClient *mongo.Client,
	schemaAdapter *SchemaAdapter,
	signal *DebeziumSignalClient,
	masking *MaskingService,
	cfg ReconHealerConfig,
	logger *zap.Logger,
) *ReconHealer {
	cfg.applyDefaults()
	if masking == nil {
		masking = NewMaskingService(db, logger, cfg.SensitiveFieldMask...)
	}
	return &ReconHealer{
		db:            db,
		mongoClient:   mongoClient,
		schemaAdapter: schemaAdapter,
		signal:        signal,
		masking:       masking,
		logger:        logger,
		cfg:           cfg,
	}
}

// InvalidateMaskCache drops the per-table sensitive-field cache so the
// next call reloads from cdc_table_registry through the shared
// MaskingService.
func (rh *ReconHealer) InvalidateMaskCache() {
	if rh.masking != nil {
		rh.masking.Invalidate("")
	}
}

// HealMissingIDs performs the v3 heal pipeline on a batch of missing
// primary-key strings. `entry` carries source/target identifiers.
//
// Pipeline:
//  1. Optionally request a Debezium incremental snapshot for the ID
//     range (when Signal path available + connector healthy).
//  2. Fall through to direct heal:
//     for chunk of BatchSize in ids:
//     docs = coll.Find({_id: {$in: chunk}})
//     for each doc:
//     build OCC UPSERT via SchemaAdapter (_source_ts from updated_at
//     or ObjectID timestamp)
//     exec UPSERT — OCC clause decides apply vs skip
//     buffer audit entry; flush every AuditFlushSize
//     flush remaining audit buffer at end.
//
// The Signal path + direct path are NOT mutually exclusive: if the
// caller supplied a tLo/tHi via `HealWindow`, Signal covers the whole
// range on the best-effort path while direct heal resolves the specific
// IDs immediately (faster convergence). `HealMissingIDs` only runs the
// direct path; `HealWindow` wraps Signal + optional direct.
func (rh *ReconHealer) HealMissingIDs(
	ctx context.Context,
	entry model.TableRegistry,
	ids []string,
) (*HealResult, error) {
	// Standalone heal (no wrapping HealWindow) — allocate a one-shot
	// batcher here so summary rows still land.
	batcher := newHealAuditBatcher(rh.db, rh.logger, rh.cfg.AuditFlushSize, rh.cfg.AuditFlushSize)
	runID := newHealRunID()
	batcher.Begin(ctx, runID, entry.TargetTable)

	res, err := rh.healMissingIDsWithBatcher(ctx, entry, ids, batcher, runID)
	if res != nil {
		res.RunID = runID
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	batcher.End(ctx, status, err, false, "")
	if res != nil {
		res.AuditFlushCnt = batcher.FlushCount()
	}
	return res, err
}

// healMissingIDsWithBatcher is the inner heal worker. Caller supplies
// the audit batcher + runID so multiple calls (e.g. HealWindow Phase B)
// can share a single audit run.
func (rh *ReconHealer) healMissingIDsWithBatcher(
	ctx context.Context,
	entry model.TableRegistry,
	ids []string,
	batcher *healAuditBatcher,
	runID string,
) (*HealResult, error) {
	if len(ids) == 0 {
		return &HealResult{Table: entry.TargetTable, RunID: runID}, nil
	}
	if rh.mongoClient == nil {
		return nil, fmt.Errorf("heal: mongo client not configured")
	}
	if rh.schemaAdapter == nil {
		return nil, fmt.Errorf("heal: schema adapter not configured")
	}

	schema := rh.schemaAdapter.GetSchema(entry.TargetTable)
	if schema == nil {
		return nil, fmt.Errorf("heal: schema not found for %s", entry.TargetTable)
	}

	start := time.Now()
	res := &HealResult{Table: entry.TargetTable, Requested: len(ids), RunID: runID}

	// Healing must read from primary so deletes/updates are not missed
	// under replica lag.
	coll := rh.mongoClient.
		Database(entry.SourceDB).
		Collection(
			entry.SourceTable,
			options.Collection().SetReadPreference(readpref.Primary()),
		)

	for _, chunk := range chunkStrings(ids, rh.cfg.BatchSize) {
		chunkCtx, cancel := context.WithTimeout(ctx, rh.cfg.QueryTimeout)

		// Build $in using a mix of ObjectID + raw string values —
		// Mongo handles both within the same $in operand.
		inVals := make([]interface{}, 0, len(chunk))
		for _, s := range chunk {
			if oid, err := primitive.ObjectIDFromHex(s); err == nil {
				inVals = append(inVals, oid)
			} else {
				inVals = append(inVals, s)
			}
		}

		cursor, err := coll.Find(chunkCtx, bson.M{"_id": bson.M{"$in": inVals}})
		if err != nil {
			cancel()
			res.Errored += len(chunk)
			metrics.ReconHealActions.
				WithLabelValues(entry.TargetTable, "error").
				Add(float64(len(chunk)))
			rh.logger.Warn("heal: mongo find chunk failed",
				zap.String("table", entry.TargetTable),
				zap.Int("chunk_size", len(chunk)),
				zap.Error(err),
			)
			continue
		}

		// Stream docs; decouple fetch from PG upsert so Mongo doesn't
		// wait on PG write latency.
		var docs []bson.M
		for cursor.Next(chunkCtx) {
			var d bson.M
			if err := cursor.Decode(&d); err != nil {
				rh.logger.Warn("heal: decode failed", zap.Error(err))
				continue
			}
			docs = append(docs, d)
		}
		cursor.Close(chunkCtx)
		cancel()

		// Build a presence map to count docs we fetched vs. requested.
		fetched := make(map[string]bool, len(docs))

		for _, doc := range docs {
			idStr := extractDocIDString(doc)
			if idStr == "" {
				res.Errored++
				metrics.ReconHealActions.
					WithLabelValues(entry.TargetTable, "error").
					Inc()
				continue
			}
			fetched[idStr] = true

			action, err := rh.applyOne(ctx, schema, entry, doc, idStr)
			if err != nil {
				res.Errored++
				metrics.ReconHealActions.
					WithLabelValues(entry.TargetTable, "error").
					Inc()
				rh.logger.Warn("heal: upsert failed",
					zap.String("table", entry.TargetTable),
					zap.String("id", idStr),
					zap.Error(err),
				)
				// Errors are always persisted — one row per failure.
				batcher.Record(ctx, "error", idStr, extractSourceTsFromDoc(doc), err.Error())
				continue
			}

			switch action {
			case "upsert":
				res.Upserted++
			case "skip":
				res.Skipped++
			}
			metrics.ReconHealActions.
				WithLabelValues(entry.TargetTable, action).
				Inc()

			// Batcher drops "skip" in memory and samples "upsert".
			batcher.Record(ctx, action, idStr, extractSourceTsFromDoc(doc), "")
		}

		// IDs requested but not returned by Mongo = doc missing in source
		// too (Mongo deleted, PG stale). Count as skip so operators see
		// the discrepancy.
		for _, s := range chunk {
			if !fetched[s] {
				res.Skipped++
				metrics.ReconHealActions.
					WithLabelValues(entry.TargetTable, "skip").
					Inc()
				batcher.Record(ctx, "skip", s, 0, "")
			}
		}
	}

	res.DurationMs = time.Since(start).Milliseconds()

	rh.logger.Info("heal batch completed",
		zap.String("table", entry.TargetTable),
		zap.Int("requested", res.Requested),
		zap.Int("upserted", res.Upserted),
		zap.Int("skipped", res.Skipped),
		zap.Int("errored", res.Errored),
		zap.Int64("duration_ms", res.DurationMs),
	)
	return res, nil
}

// HealOrphanedIDs removes destination rows that no longer exist in the
// Mongo source. Caller should pass the `missing_from_src` list emitted
// by Tier 2 (`ReconciliationReport.StaleIDs` payload).
func (rh *ReconHealer) HealOrphanedIDs(
	ctx context.Context,
	entry model.TableRegistry,
	missingFromSrc []string,
) (*HealResult, error) {
	if len(missingFromSrc) == 0 {
		return &HealResult{Table: entry.TargetTable}, nil
	}
	if err := validateIdent(entry.TargetTable); err != nil {
		return nil, err
	}
	if err := validateIdent(entry.PrimaryKeyField); err != nil {
		return nil, err
	}

	start := time.Now()
	res := &HealResult{
		Table:     entry.TargetTable,
		Requested: len(missingFromSrc),
	}

	for _, chunk := range chunkStrings(missingFromSrc, rh.cfg.BatchSize) {
		args := make([]interface{}, 0, len(chunk)+1)
		placeholders := make([]string, 0, len(chunk))
		for i, id := range chunk {
			placeholders = append(placeholders, "?")
			args = append(args, id)
			_ = i
		}
		query := fmt.Sprintf(
			`DELETE FROM %s WHERE %s IN (%s)`,
			quoteIdent(entry.TargetTable),
			quoteIdent(entry.PrimaryKeyField),
			strings.Join(placeholders, ", "),
		)
		execRes := rh.db.WithContext(ctx).Exec(query, args...)
		if execRes.Error != nil {
			res.Errored += len(chunk)
			rh.logger.Warn("heal orphaned delete failed",
				zap.String("table", entry.TargetTable),
				zap.Int("chunk_size", len(chunk)),
				zap.Error(execRes.Error),
			)
			continue
		}
		res.Deleted += int(execRes.RowsAffected)
	}

	res.DurationMs = time.Since(start).Milliseconds()
	rh.logger.Info("heal orphaned IDs completed",
		zap.String("table", entry.TargetTable),
		zap.Int("requested", res.Requested),
		zap.Int("deleted", res.Deleted),
		zap.Int("errored", res.Errored),
		zap.Int64("duration_ms", res.DurationMs),
	)
	return res, nil
}

// HealWindow is the orchestrated heal flow (plan v3 §7):
//
//  1. Phase A (preferred): Debezium incremental snapshot with range
//     filter on updated_at. Relies on the worker's kafka consumer to
//     apply the records — no direct PG write from here.
//  2. Phase B (fallback): direct $in heal via HealMissingIDs for the
//     pre-computed missing ID list.
//
// Caller supplies `missingIDs` from Tier 2 output; pass nil / empty to
// request "snapshot-only" behavior (Signal alone). When signal is NOT
// configured or the connector is unhealthy, we go straight to Phase B.
func (rh *ReconHealer) HealWindow(
	ctx context.Context,
	entry model.TableRegistry,
	tLo, tHi time.Time,
	missingIDs []string,
) (*HealResult, error) {
	runID := newHealRunID()
	res := &HealResult{Table: entry.TargetTable, Requested: len(missingIDs), RunID: runID}
	start := time.Now()

	// ONE audit batcher for the whole run — Phase A + Phase B share it.
	// This guarantees exactly 1 run_started + 1 run_completed per
	// HealWindow invocation regardless of phases taken.
	batcher := newHealAuditBatcher(rh.db, rh.logger, rh.cfg.AuditFlushSize, rh.cfg.AuditFlushSize)
	batcher.Begin(ctx, runID, entry.TargetTable)

	var runErr error
	defer func() {
		status := "success"
		if runErr != nil || res.Errored > 0 {
			status = "error"
			if runErr == nil && res.Errored > 0 {
				// Preserve the fact of per-record errors in the summary.
				runErr = fmt.Errorf("%d record(s) errored during heal", res.Errored)
			}
		}
		res.DurationMs = time.Since(start).Milliseconds()
		res.AuditFlushCnt = batcher.FlushCount()
		batcher.End(ctx, status, runErr, res.UsedSignal, res.SignalID)
	}()

	// Phase A — Debezium signal, best-effort.
	if rh.signal != nil && rh.signal.IsConfigured() && !rh.cfg.ForceDirect {
		healthy, err := rh.signal.IsConnectorHealthy(ctx)
		if err != nil {
			rh.logger.Warn("heal: connector status probe failed, skipping signal path",
				zap.Error(err),
			)
		} else if healthy {
			filter := BuildUpdatedAtRangeFilter(tLo, tHi)
			signalID, err := rh.signal.TriggerIncrementalSnapshot(
				ctx, entry.SourceDB, entry.SourceTable, filter,
			)
			if err != nil {
				rh.logger.Warn("heal: debezium signal insert failed, falling back to direct",
					zap.Error(err),
				)
			} else {
				res.UsedSignal = true
				res.SignalID = signalID
				rh.logger.Info("heal: debezium incremental snapshot requested",
					zap.String("table", entry.TargetTable),
					zap.String("filter", filter),
					zap.String("signal_id", signalID),
				)
			}
		} else {
			rh.logger.Info("heal: connector NOT running, skipping signal path",
				zap.String("table", entry.TargetTable),
			)
		}
	}

	// Phase B — direct heal for the exact list. Always runs when IDs
	// were supplied (even if Phase A succeeded): faster convergence for
	// known-missing rows than waiting for the connector to stream.
	if len(missingIDs) > 0 {
		direct, err := rh.healMissingIDsWithBatcher(ctx, entry, missingIDs, batcher, runID)
		if err != nil {
			runErr = err
			return res, err
		}
		if direct != nil {
			res.Upserted += direct.Upserted
			res.Skipped += direct.Skipped
			res.Errored += direct.Errored
		}
	}

	return res, nil
}

// applyOne builds + executes the OCC upsert for a single Mongo doc.
// Returns the action taken for metrics/audit: "upsert" when a row was
// written, "skip" when the OCC guard rejected (RowsAffected==0).
func (rh *ReconHealer) applyOne(
	ctx context.Context,
	schema *TableSchema,
	entry model.TableRegistry,
	doc bson.M,
	idStr string,
) (string, error) {
	// Prepare mapped data (BSON → Go/JSON).
	data := make(map[string]interface{}, len(doc))
	for k, v := range doc {
		data[k] = unwrapBSONValue(v)
	}

	rawJSON := rh.buildMaskedRawJSON(entry.TargetTable, data)

	// _source_ts preference order:
	//  1. doc.updated_at (time.Time or primitive.DateTime)
	//  2. ObjectID embedded timestamp
	//  3. 0 → OCC guard skipped, row upserted unconditionally
	srcTsMs := extractSourceTsFromDoc(doc)

	query, values := rh.schemaAdapter.BuildUpsertSQL(
		schema,
		entry.TargetTable,
		entry.PrimaryKeyField,
		idStr,
		data,
		string(rawJSON),
		"recon-heal",
		md5Hex(rawJSON),
		srcTsMs,
	)

	execRes := rh.db.WithContext(ctx).Exec(query, values...)
	if err := execRes.Error; err != nil {
		return "error", err
	}
	if execRes.RowsAffected == 0 {
		// OCC guard rejected — destination already has a newer ts.
		return "skip", nil
	}
	return "upsert", nil
}

func (rh *ReconHealer) buildMaskedRawJSON(targetTable string, data map[string]interface{}) []byte {
	maskedData := data
	if rh.masking != nil {
		maskedData = rh.masking.MaskTableData(targetTable, data)
	}
	rawJSON, _ := json.Marshal(maskedData)
	return rawJSON
}

// newHealRunID produces a short, sortable correlation ID used to tie
// every audit row emitted during a single HealWindow / HealMissingIDs
// invocation together. Not a UUID (keeps the code dep-free and the ID
// compact); timestamp-nanos + 6 hex chars is ample for local ordering
// plus collision safety inside one process.
func newHealRunID() string {
	now := time.Now().UTC()
	// Suffix entropy from UnixNano's low bits — cheap, collision-safe
	// enough for a single-process heal loop.
	return fmt.Sprintf("heal-%s-%06x",
		now.Format("20060102T150405.000"),
		uint32(now.UnixNano())&0xFFFFFF,
	)
}

// ============================================================
// Helpers
// ============================================================

// chunkStrings slices ids into chunks of at most size. Returned slices
// alias the input — do not mutate.
func chunkStrings(ids []string, size int) [][]string {
	if size <= 0 {
		return [][]string{ids}
	}
	// Sort so chunks are deterministic across retries — helps audit
	// diffing and unit tests.
	s := make([]string, len(ids))
	copy(s, ids)
	sort.Strings(s)

	out := make([][]string, 0, (len(s)+size-1)/size)
	for i := 0; i < len(s); i += size {
		j := i + size
		if j > len(s) {
			j = len(s)
		}
		out = append(out, s[i:j])
	}
	return out
}

// extractDocIDString returns the string representation of doc["_id"].
func extractDocIDString(doc bson.M) string {
	v, ok := doc["_id"]
	if !ok {
		return ""
	}
	switch vv := v.(type) {
	case primitive.ObjectID:
		return vv.Hex()
	case string:
		return vv
	case int32:
		return fmt.Sprintf("%d", vv)
	case int64:
		return fmt.Sprintf("%d", vv)
	default:
		return fmt.Sprintf("%v", vv)
	}
}

// extractSourceTsFromDoc — preference:
//  1. doc["updated_at"] (time.Time or primitive.DateTime)
//  2. doc["_id"].Timestamp() when ObjectID
//  3. 0 (unknown)
func extractSourceTsFromDoc(doc bson.M) int64 {
	if v, ok := doc["updated_at"]; ok {
		switch tv := v.(type) {
		case time.Time:
			return tv.UnixMilli()
		case primitive.DateTime:
			return tv.Time().UnixMilli()
		}
	}
	if v, ok := doc["_id"]; ok {
		if oid, ok := v.(primitive.ObjectID); ok {
			return oid.Timestamp().UnixMilli()
		}
	}
	return 0
}
