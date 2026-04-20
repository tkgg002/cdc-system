package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/pkgs/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DLQWorker periodically scans `failed_sync_logs` and retries rows
// that are due, with exponential backoff and a hard cap at MaxRetries.
//
// State machine (plan v3 §9):
//   pending | failed  →  retrying
//   retrying          →  resolved  | dead_letter
//
// Retry pickup query (partial-index-optimised, see migration 012):
//   WHERE (status IN ('pending','failed','retrying'))
//     AND (next_retry_at IS NULL OR next_retry_at < NOW())
//     AND retry_count < MaxRetries
//   ORDER BY next_retry_at NULLS FIRST, id
//   LIMIT BatchSize
//
// Backoff schedule (0-indexed by retry_count):
//   0 → +1m, 1 → +5m, 2 → +30m, 3 → +2h, 4 → +6h, ≥5 → dead_letter
//
// When retry_count reaches MaxRetries (default 5), the row transitions
// to `dead_letter`. The `cdc_dlq_stuck_records_total{reason}` gauge
// (exposed via Prometheus) reflects the current count of rows stuck in
// dead_letter so operators can wire an alert.

// DLQRetryMetrics wraps the retry-specific gauges we expose in
// addition to the existing reconciliation metrics. Kept local to the
// service so the main metrics pkg stays stable; registers to the
// default prometheus registry.
var (
	DLQStuckRecords = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cdc_dlq_stuck_records_total",
			Help: "Count of failed_sync_logs rows stuck in a given terminal/pending state",
		},
		[]string{"status"},
	)

	DLQRetryAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cdc_dlq_retry_attempts_total",
			Help: "Total DLQ retry attempts by outcome",
		},
		[]string{"table", "outcome"}, // outcome: resolved|retrying|dead_letter|error
	)

	DLQWriteFail = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "cdc_dlq_write_failures_total",
			Help: "DLQ insert failures — offset kept (message will be redelivered)",
		},
	)
)

// DLQWorkerConfig — tunables for the retry loop.
type DLQWorkerConfig struct {
	PollInterval time.Duration // default 5m
	BatchSize    int           // default 100
	MaxRetries   int           // default 5
	QueryTimeout time.Duration // default 30s
}

func (c *DLQWorkerConfig) applyDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Minute
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 30 * time.Second
	}
}

// DLQWorker runs the retry loop. Construct once, call Start in a
// goroutine, cancel via the ctx passed to Start.
type DLQWorker struct {
	db            *gorm.DB
	mongoClient   *mongo.Client
	schemaAdapter *SchemaAdapter
	logger        *zap.Logger
	cfg           DLQWorkerConfig
}

// NewDLQWorker — mongoClient is optional; when nil, retries that need
// to re-fetch from Mongo (i.e. row has no raw_json payload) are moved
// to dead_letter instead of being retried.
func NewDLQWorker(
	db *gorm.DB,
	mongoClient *mongo.Client,
	schemaAdapter *SchemaAdapter,
	cfg DLQWorkerConfig,
	logger *zap.Logger,
) *DLQWorker {
	cfg.applyDefaults()
	return &DLQWorker{
		db:            db,
		mongoClient:   mongoClient,
		schemaAdapter: schemaAdapter,
		cfg:           cfg,
		logger:        logger,
	}
}

// Start runs the poll loop until ctx is cancelled. First iteration
// fires immediately so operators can verify pickup at worker start.
func (w *DLQWorker) Start(ctx context.Context) {
	w.logger.Info("dlq retry worker started",
		zap.Duration("poll_interval", w.cfg.PollInterval),
		zap.Int("batch_size", w.cfg.BatchSize),
		zap.Int("max_retries", w.cfg.MaxRetries),
	)

	// Fire once at startup.
	w.RunOnce(ctx)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("dlq retry worker stopped")
			return
		case <-ticker.C:
			w.RunOnce(ctx)
		}
	}
}

// RunOnce executes a single poll + retry cycle. Exported so integration
// tests can trigger without the goroutine.
func (w *DLQWorker) RunOnce(ctx context.Context) {
	// Refresh the stuck-records gauge every cycle regardless of work done.
	defer w.refreshStuckGauge(ctx)

	cycleCtx, cancel := context.WithTimeout(ctx, w.cfg.QueryTimeout)
	defer cancel()

	var rows []model.FailedSyncLog
	err := w.db.WithContext(cycleCtx).
		Raw(`SELECT * FROM failed_sync_logs
			 WHERE status IN ('pending','failed','retrying')
			   AND (next_retry_at IS NULL OR next_retry_at < NOW())
			   AND retry_count < ?
			 ORDER BY next_retry_at NULLS FIRST, id
			 LIMIT ?`,
			w.cfg.MaxRetries, w.cfg.BatchSize,
		).Scan(&rows).Error

	if err != nil {
		w.logger.Warn("dlq worker: poll failed", zap.Error(err))
		return
	}
	if len(rows) == 0 {
		return
	}

	w.logger.Info("dlq worker picked up rows",
		zap.Int("count", len(rows)),
	)

	for _, r := range rows {
		w.retryOne(ctx, r)
	}
}

// retryOne runs the retry path for a single DLQ row. Transitions the
// row into terminal status when appropriate.
func (w *DLQWorker) retryOne(ctx context.Context, r model.FailedSyncLog) {
	now := time.Now().UTC()
	// Always mark retrying BEFORE the attempt — if the worker crashes
	// mid-flight the next cycle sees retrying + next_retry_at due and
	// will try again (with incremented retry_count after successful
	// UPDATE below; the crash-before-UPDATE case is idempotent).
	if err := w.db.WithContext(ctx).Exec(
		`UPDATE failed_sync_logs SET status='retrying', last_retry_at=? WHERE id=?`,
		now, r.ID,
	).Error; err != nil {
		w.logger.Warn("dlq worker: mark retrying failed", zap.Uint64("id", r.ID), zap.Error(err))
		return
	}

	err := w.tryApply(ctx, r)
	retryCount := r.RetryCount + 1

	if err == nil {
		// resolved
		w.db.WithContext(ctx).Exec(
			`UPDATE failed_sync_logs
			   SET status='resolved', resolved_at=?, retry_count=?, next_retry_at=NULL
			 WHERE id=?`,
			now, retryCount, r.ID,
		)
		DLQRetryAttempts.WithLabelValues(r.TargetTable, "resolved").Inc()
		w.logger.Info("dlq retry resolved",
			zap.Uint64("id", r.ID),
			zap.String("table", r.TargetTable),
			zap.Int("retries", retryCount),
		)
		return
	}

	// Fail path.
	if retryCount >= w.cfg.MaxRetries {
		errMsg := truncate(err.Error(), 2000)
		w.db.WithContext(ctx).Exec(
			`UPDATE failed_sync_logs
			   SET status='dead_letter', retry_count=?, last_error=?, next_retry_at=NULL
			 WHERE id=?`,
			retryCount, errMsg, r.ID,
		)
		DLQRetryAttempts.WithLabelValues(r.TargetTable, "dead_letter").Inc()
		w.logger.Warn("dlq retry exhausted → dead_letter",
			zap.Uint64("id", r.ID),
			zap.String("table", r.TargetTable),
			zap.Int("retries", retryCount),
			zap.Error(err),
		)
		return
	}

	next := now.Add(BackoffDelay(retryCount))
	errMsg := truncate(err.Error(), 2000)
	w.db.WithContext(ctx).Exec(
		`UPDATE failed_sync_logs
		   SET status='retrying', retry_count=?, next_retry_at=?, last_error=?
		 WHERE id=?`,
		retryCount, next, errMsg, r.ID,
	)
	DLQRetryAttempts.WithLabelValues(r.TargetTable, "retrying").Inc()
	w.logger.Info("dlq retry scheduled",
		zap.Uint64("id", r.ID),
		zap.String("table", r.TargetTable),
		zap.Int("retries", retryCount),
		zap.Time("next_retry_at", next),
	)
}

// tryApply attempts the actual re-insert into the target table. Fetch
// strategy:
//  1. If raw_json column is populated, use it directly — cheapest path.
//  2. Else, if mongoClient + record_id available, re-fetch from Mongo.
//  3. Else, return error → caller schedules backoff or dead-letters.
func (w *DLQWorker) tryApply(ctx context.Context, r model.FailedSyncLog) error {
	if w.schemaAdapter == nil {
		return fmt.Errorf("schema adapter not configured")
	}

	var registry model.TableRegistry
	if err := w.db.WithContext(ctx).
		Where("target_table = ?", r.TargetTable).
		First(&registry).Error; err != nil {
		return fmt.Errorf("registry lookup: %w", err)
	}

	schema := w.schemaAdapter.GetSchema(r.TargetTable)
	if schema == nil {
		return fmt.Errorf("schema not found for %s", r.TargetTable)
	}

	// Decode raw_json first.
	var payload map[string]interface{}
	if len(r.RawJSON) > 0 {
		if err := json.Unmarshal(r.RawJSON, &payload); err != nil {
			return fmt.Errorf("parse raw_json: %w", err)
		}
	}

	// Mongo re-fetch fallback.
	srcTsMs := int64(0)
	if payload == nil && w.mongoClient != nil && r.RecordID != "" && registry.SourceDB != "" && registry.SourceTable != "" {
		coll := w.mongoClient.
			Database(registry.SourceDB).
			Collection(registry.SourceTable)

		var doc bson.M
		query := bson.M{"_id": r.RecordID}
		if oid, err := primitive.ObjectIDFromHex(r.RecordID); err == nil {
			query = bson.M{"_id": oid}
		}
		if err := coll.FindOne(ctx, query).Decode(&doc); err != nil {
			return fmt.Errorf("mongo re-fetch: %w", err)
		}
		payload = make(map[string]interface{}, len(doc))
		for k, v := range doc {
			payload[k] = unwrapBSONValue(v)
		}
		srcTsMs = extractSourceTsFromDoc(doc)
	}

	if payload == nil {
		return fmt.Errorf("no payload to retry (empty raw_json + no mongo fallback)")
	}

	if srcTsMs == 0 {
		// Use updated_at embedded in the payload when possible.
		if v, ok := payload["updated_at"]; ok {
			if s, ok := v.(string); ok {
				if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
					srcTsMs = t.UnixMilli()
				}
			}
		}
	}

	pkValue := r.RecordID
	if pkValue == "" {
		// Best-effort fallback from payload.
		if v, ok := payload[registry.PrimaryKeyField]; ok {
			pkValue = fmt.Sprintf("%v", v)
		}
	}
	if pkValue == "" {
		return fmt.Errorf("missing primary key")
	}

	rawJSON, _ := json.Marshal(payload)
	query, values := w.schemaAdapter.BuildUpsertSQL(
		schema,
		r.TargetTable,
		registry.PrimaryKeyField,
		pkValue,
		payload,
		string(rawJSON),
		"dlq-retry",
		md5Hex(rawJSON),
		srcTsMs,
	)

	if err := w.db.WithContext(ctx).Exec(query, values...).Error; err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// refreshStuckGauge samples count per status for Prom export.
func (w *DLQWorker) refreshStuckGauge(ctx context.Context) {
	type row struct {
		Status string
		Cnt    int64
	}
	var rows []row
	err := w.db.WithContext(ctx).Raw(
		`SELECT status, COUNT(*) AS cnt FROM failed_sync_logs GROUP BY status`,
	).Scan(&rows).Error
	if err != nil {
		return
	}
	// Reset known statuses to zero first so tiles disappear when
	// DLQ is drained.
	for _, s := range []string{"pending", "failed", "retrying", "resolved", "dead_letter"} {
		DLQStuckRecords.WithLabelValues(s).Set(0)
	}
	for _, r := range rows {
		DLQStuckRecords.WithLabelValues(r.Status).Set(float64(r.Cnt))
	}
	// Keep parity with the existing consumer-lag-style metric surface.
	_ = metrics.ConsumerLag
}

// BackoffDelay maps a retry_count (1-based; 1 means "after first
// failure") to the next sleep. Plan v3 §9 schedule.
func BackoffDelay(retryCount int) time.Duration {
	switch retryCount {
	case 1:
		return 1 * time.Minute
	case 2:
		return 5 * time.Minute
	case 3:
		return 30 * time.Minute
	case 4:
		return 2 * time.Hour
	default:
		return 6 * time.Hour
	}
}

// truncate bounds a string to `max` runes. Defensive against
// pathological SQL error payloads.
func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
