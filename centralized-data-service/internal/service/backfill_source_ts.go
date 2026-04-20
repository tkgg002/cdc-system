package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/pkgs/metrics"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// BackfillSourceTsConfig — tunables for the backfill job.
// BatchSize controls how many PG rows (and the resulting Mongo $in
// operand) we process per iteration. MaxTotalScan caps the total per
// table per run to avoid runaway loops; defaults to 10_000_000 rows.
type BackfillSourceTsConfig struct {
	BatchSize    int
	MaxTotalScan int
	QueryTimeout time.Duration
}

func (c *BackfillSourceTsConfig) applyDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.MaxTotalScan <= 0 {
		c.MaxTotalScan = 10_000_000
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 60 * time.Second
	}
}

// BackfillSourceTsService populates the `_source_ts` column for rows
// that were ingested before the OCC guard was introduced. It derives
// the source timestamp from the Mongo document's `updated_at`
// (preferred) or the ObjectID embedded timestamp (fallback). All
// progress is tracked through `recon_runs` with tier=4 — a dedicated
// tier so this backfill does NOT mix with the legitimate Tier 1/2/3
// reconciliation schedules.
type BackfillSourceTsService struct {
	db           *gorm.DB
	mongoClient  *mongo.Client
	registryRepo *repository.RegistryRepo
	cfg          BackfillSourceTsConfig
	logger       *zap.Logger
}

func NewBackfillSourceTsService(
	db *gorm.DB,
	mongoClient *mongo.Client,
	registryRepo *repository.RegistryRepo,
	cfg BackfillSourceTsConfig,
	logger *zap.Logger,
) *BackfillSourceTsService {
	cfg.applyDefaults()
	return &BackfillSourceTsService{
		db:           db,
		mongoClient:  mongoClient,
		registryRepo: registryRepo,
		cfg:          cfg,
		logger:       logger,
	}
}

// BackfillResult aggregates per-table outcome for one run.
type BackfillResult struct {
	Table      string `json:"table"`
	Scanned    int64  `json:"scanned"`
	Updated    int64  `json:"updated"`
	Skipped    int64  `json:"skipped"`
	Errored    int64  `json:"errored"`
	Remaining  int64  `json:"remaining"`
	DurationMs int64  `json:"duration_ms"`
}

// BackfillAll — drive the loop across a set of tables (empty list =
// all active tables from the registry). Returns per-table results.
func (b *BackfillSourceTsService) BackfillAll(
	ctx context.Context,
	runID string,
	tables []string,
) ([]BackfillResult, error) {
	if len(tables) == 0 {
		entries, err := b.registryRepo.GetAllActive(ctx)
		if err != nil {
			return nil, fmt.Errorf("backfill: load registry: %w", err)
		}
		for _, e := range entries {
			tables = append(tables, e.TargetTable)
		}
	}

	results := make([]BackfillResult, 0, len(tables))
	for _, t := range tables {
		res, err := b.BackfillOne(ctx, runID, t)
		if err != nil {
			b.logger.Warn("backfill: table failed — continuing",
				zap.String("table", t), zap.Error(err))
			continue
		}
		results = append(results, res)
	}
	return results, nil
}

// BackfillOne — iterate rows with _source_ts IS NULL in batches of
// cfg.BatchSize, derive source_ts_ms from Mongo, parameterized UPDATE
// on PG. Emits progress metrics `cdc_recon_backfill_progress{table}`
// (percent 0..100) and writes a recon_runs row with tier=4 for full
// visibility on the CMS monitoring page.
func (b *BackfillSourceTsService) BackfillOne(
	ctx context.Context,
	runID string,
	table string,
) (BackfillResult, error) {
	res := BackfillResult{Table: table}
	start := time.Now()

	entry, err := b.lookupRegistry(ctx, table)
	if err != nil {
		return res, err
	}

	// Begin a dedicated recon_runs row (tier=4 = backfill).
	runRowID := uuid.NewString()
	handle, err := b.beginRun(ctx, runRowID, table, runID)
	if err != nil {
		return res, fmt.Errorf("backfill: beginRun: %w", err)
	}

	// Upper bound — total rows NULL at start of run. Used for progress %.
	var totalNull int64
	_ = b.db.WithContext(ctx).
		Raw(fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE _source_ts IS NULL`, table)).
		Scan(&totalNull).Error
	if totalNull == 0 {
		b.finishRun(ctx, handle, "success", "no rows with NULL _source_ts")
		res.DurationMs = time.Since(start).Milliseconds()
		return res, nil
	}

	// For _source_ts backfill, always use Mongo _id column since we need
	// $in lookup in the Mongo source collection by ObjectID. Registry's
	// primary_key_field may be a business code (e.g. "id" in
	// refund_requests = human-readable business code, NOT ObjectID) which
	// would not match Mongo _id.
	pk := "_id"

	coll := b.mongoClient.
		Database(entry.SourceDB).
		Collection(entry.SourceTable)

	// Loop with safety cap MaxTotalScan.
	// Cursor-based pagination on `pk` so batches advance even when Mongo
	// has no matching doc for the batch (otherwise a batch of unmatched
	// ids would loop forever because _source_ts stays NULL and the same
	// top-N rows are re-selected on each iteration).
	finalStatus := "success"
	finalErr := ""
	var cursor string
	for iter := 0; iter < 100_000; iter++ {
		if res.Scanned >= int64(b.cfg.MaxTotalScan) {
			finalStatus = "cancelled"
			finalErr = "hit MaxTotalScan cap"
			break
		}

		batchCtx, cancel := context.WithTimeout(ctx, b.cfg.QueryTimeout)
		ids, err := b.fetchNullBatch(batchCtx, table, pk, cursor, b.cfg.BatchSize)
		cancel()
		if err != nil {
			finalStatus = "failed"
			finalErr = err.Error()
			break
		}
		if len(ids) == 0 {
			break // all done
		}
		// Advance cursor to the last id of this batch.
		cursor = ids[len(ids)-1]

		// Mongo fetch — project only _id + updated_at to keep payload tiny.
		sourceTsMap, fetchErr := b.fetchSourceTsMap(ctx, coll, ids)
		if fetchErr != nil {
			b.logger.Warn("backfill: mongo fetch failed, continuing to next batch",
				zap.String("table", table), zap.Int("batch", len(ids)),
				zap.Error(fetchErr),
			)
			res.Errored += int64(len(ids))
			continue
		}

		// UPDATE per id, parameterized. Use a single tx for the batch
		// so a mid-batch failure rolls back cleanly.
		updated, skipped := b.applyBatch(ctx, table, pk, ids, sourceTsMap)
		res.Updated += updated
		res.Skipped += skipped
		res.Scanned += int64(len(ids))

		// Update handle for recon_runs persistence.
		handle.docsScanned = res.Scanned
		handle.healActions = int(res.Updated)

		// Progress %. Clamp to 0..100.
		pct := 100.0
		if totalNull > 0 {
			pct = float64(res.Updated) / float64(totalNull) * 100.0
			if pct > 100 {
				pct = 100
			}
		}
		metrics.ReconBackfillProgress.WithLabelValues(table).Set(pct)

		// Periodic mid-run progress write so CMS polling sees progress.
		if iter%5 == 0 {
			b.touchRunProgress(ctx, handle)
		}
	}

	// Final remaining count for visibility.
	var remaining int64
	_ = b.db.WithContext(ctx).
		Raw(fmt.Sprintf(`SELECT COUNT(*) FROM %q WHERE _source_ts IS NULL`, table)).
		Scan(&remaining).Error
	res.Remaining = remaining
	if remaining == 0 {
		metrics.ReconBackfillProgress.WithLabelValues(table).Set(100)
	}

	b.finishRun(ctx, handle, finalStatus, finalErr)
	res.DurationMs = time.Since(start).Milliseconds()

	b.logger.Info("backfill: table done",
		zap.String("table", table),
		zap.Int64("scanned", res.Scanned),
		zap.Int64("updated", res.Updated),
		zap.Int64("skipped", res.Skipped),
		zap.Int64("errored", res.Errored),
		zap.Int64("remaining", res.Remaining),
		zap.String("status", finalStatus),
	)
	return res, nil
}

// lookupRegistry resolves the Mongo source for a target_table.
func (b *BackfillSourceTsService) lookupRegistry(
	ctx context.Context,
	table string,
) (model.TableRegistry, error) {
	var entry model.TableRegistry
	if err := b.db.WithContext(ctx).
		Where("target_table = ?", table).First(&entry).Error; err != nil {
		return entry, fmt.Errorf("registry not found for %q: %w", table, err)
	}
	return entry, nil
}

// fetchNullBatch returns up to `limit` primary-key values with NULL
// _source_ts whose pk is strictly greater than `after` (cursor-based
// pagination). Uses the table's PK field so we can drive the Mongo
// $in + the PG UPDATE WHERE clause with the same value. An empty
// `after` starts from the beginning.
func (b *BackfillSourceTsService) fetchNullBatch(
	ctx context.Context,
	table, pk, after string,
	limit int,
) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if after == "" {
		rows, err = b.db.WithContext(ctx).Raw(
			fmt.Sprintf(
				`SELECT %q FROM %q WHERE _source_ts IS NULL ORDER BY %q LIMIT ?`,
				pk, table, pk,
			),
			limit,
		).Rows()
	} else {
		rows, err = b.db.WithContext(ctx).Raw(
			fmt.Sprintf(
				`SELECT %q FROM %q WHERE _source_ts IS NULL AND %q > ? ORDER BY %q LIMIT ?`,
				pk, table, pk, pk,
			),
			after, limit,
		).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, scanErr
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// fetchSourceTsMap fetches { _id -> source_ts_ms } for a chunk.
func (b *BackfillSourceTsService) fetchSourceTsMap(
	ctx context.Context,
	coll *mongo.Collection,
	ids []string,
) (map[string]int64, error) {
	inVals := make([]interface{}, 0, len(ids))
	for _, s := range ids {
		if oid, err := primitive.ObjectIDFromHex(s); err == nil {
			inVals = append(inVals, oid)
		} else {
			inVals = append(inVals, s)
		}
	}

	cur, err := coll.Find(ctx,
		bson.M{"_id": bson.M{"$in": inVals}},
		options.Find().SetProjection(bson.M{"_id": 1, "updated_at": 1}),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := make(map[string]int64, len(ids))
	for cur.Next(ctx) {
		var d bson.M
		if err := cur.Decode(&d); err != nil {
			continue
		}
		idStr := extractDocIDString(d)
		if idStr == "" {
			continue
		}
		ts := extractSourceTsFromDoc(d)
		if ts == 0 {
			// No updated_at + non-ObjectID _id → skip; caller counts as "skipped"
			continue
		}
		out[idStr] = ts
	}
	return out, nil
}

// applyBatch issues per-row UPDATEs inside a single transaction.
// Returns (updatedCount, skippedCount). "skipped" = row not in the
// map (no source ts derivable) OR RowsAffected 0 (someone else filled
// it first).
func (b *BackfillSourceTsService) applyBatch(
	ctx context.Context,
	table, pk string,
	ids []string,
	tsMap map[string]int64,
) (int64, int64) {
	if len(ids) == 0 {
		return 0, 0
	}
	var updated, skipped int64

	tx := b.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return 0, int64(len(ids))
	}
	for _, id := range ids {
		ms, ok := tsMap[id]
		if !ok {
			skipped++
			continue
		}
		stmt := fmt.Sprintf(
			`UPDATE %q SET _source_ts = ? WHERE %q = ? AND _source_ts IS NULL`,
			table, pk,
		)
		execRes := tx.Exec(stmt, ms, id)
		if execRes.Error != nil {
			skipped++
			continue
		}
		if execRes.RowsAffected == 0 {
			skipped++
			continue
		}
		updated += execRes.RowsAffected
	}
	if err := tx.Commit().Error; err != nil {
		return 0, int64(len(ids))
	}
	return updated, skipped
}

// ============================================================
// recon_runs lifecycle helpers (tier=4 backfill scope)
// ============================================================

type backfillRunHandle struct {
	id          string
	runID       string // correlation id from CMS (same for all tables of one trigger)
	table       string
	started     time.Time
	docsScanned int64
	healActions int
}

func (b *BackfillSourceTsService) beginRun(
	ctx context.Context,
	runRowID, table, correlationID string,
) (*backfillRunHandle, error) {
	h := &backfillRunHandle{
		id:      runRowID,
		runID:   correlationID,
		table:   table,
		started: time.Now().UTC(),
	}
	instance := fmt.Sprintf("backfill:%s", correlationID)
	err := b.db.WithContext(ctx).Exec(
		`INSERT INTO recon_runs
			(id, table_name, tier, status, started_at, instance_id)
		 VALUES (?, ?, 4, 'running', ?, ?)`,
		h.id, table, h.started, instance,
	).Error
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (b *BackfillSourceTsService) touchRunProgress(
	ctx context.Context,
	h *backfillRunHandle,
) {
	_ = b.db.WithContext(ctx).Exec(
		`UPDATE recon_runs
		   SET docs_scanned=?, heal_actions=?
		 WHERE id=?`,
		h.docsScanned, h.healActions, h.id,
	).Error
}

func (b *BackfillSourceTsService) finishRun(
	ctx context.Context,
	h *backfillRunHandle,
	status, errMsg string,
) {
	finished := time.Now().UTC()
	_ = b.db.WithContext(ctx).Exec(
		`UPDATE recon_runs
		   SET status=?, finished_at=?, docs_scanned=?, heal_actions=?, error_message=?
		 WHERE id=?`,
		status, finished, h.docsScanned, h.healActions, errMsg, h.id,
	).Error
}

// ============================================================
// Activity log entry for audit trail
// ============================================================

// WriteActivity writes a single cdc_activity_log row summarising the
// backfill for downstream dashboards. Non-blocking failure — the
// backfill result is already persisted in recon_runs.
func (b *BackfillSourceTsService) WriteActivity(
	ctx context.Context,
	runID string,
	results []BackfillResult,
	outcome string,
) {
	now := time.Now()
	details, _ := json.Marshal(map[string]interface{}{
		"run_id":  runID,
		"results": results,
	})
	_ = b.db.WithContext(ctx).Create(&model.ActivityLog{
		Operation:   "recon-backfill-source-ts",
		TargetTable: "*",
		Status:      outcome,
		Details:     details,
		TriggeredBy: "nats-command",
		StartedAt:   now,
		CompletedAt: &now,
	}).Error
}
