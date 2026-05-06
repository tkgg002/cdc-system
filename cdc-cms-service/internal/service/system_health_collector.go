// Package service — system_health_collector.go
//
// Purpose: Run health probes asynchronously in the background, cache a
// snapshot JSON in Redis (`system_health:snapshot`, TTL 60s), and let the
// HTTP handler simply read from cache. This replaces a 5-way synchronous
// percentile SQL) that drove the handler p99 to 2-5s and made it
// cascade-failure-prone.
//
// Contract with the handler:
//   - Key: `system_health:snapshot` (configurable via Config.CacheKey)
//   - Value: JSON-encoded Snapshot
//   - TTL: 60s (Config.CacheTTL)
//   - Handler computes `cache_age_seconds = now - Snapshot.Timestamp`.
//
// Per-probe timeout is 2s via context.WithTimeout; a failed probe records
// its section as `unknown`/`down` but never blocks the others (errgroup
// swallows non-nil errors by using g.Go funcs that always return nil).
package service

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	infrahttp "cdc-cms-service/internal/infra/http"
	"cdc-cms-service/internal/service/health/probes"
	"cdc-cms-service/pkgs/rediscache"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

// ----- Wire-format structs -----
//
// All JSON keys are stable; field additions are backward-compatible. The FE
// must tolerate unknown keys. Existing top-level keys (`timestamp`, `overall`,
// `infrastructure`, `cdc_pipeline`, `reconciliation`, `latency`, `failed_sync`,
// `alerts`, `recent_events`) are preserved so the current UI does not break.

// Section status vocabulary.
const (
	StatusOK       = "ok"
	StatusDegraded = "degraded"
	StatusDown     = "down"
	StatusUnknown  = "unknown"
	StatusUp       = "up" // legacy alias retained for the old FE
)

// Snapshot is the JSON body cached in Redis.
type Snapshot struct {
	Timestamp       time.Time              `json:"timestamp"`
	CacheAgeSeconds int                    `json:"cache_age_seconds"` // filled by handler on read
	Overall         string                 `json:"overall"`
	Infrastructure  map[string]any         `json:"infrastructure"`
	CDCPipeline     map[string]any         `json:"cdc_pipeline"`
	Reconciliation  []map[string]any       `json:"reconciliation"`
	Latency         infrahttp.LatencyResult `json:"latency"`
	FailedSync      map[string]any         `json:"failed_sync"`
	Alerts          []map[string]any       `json:"alerts"`
	RecentEvents    []map[string]any       `json:"recent_events"`
	// Optional diagnostic meta; mirrors per-section visibility.
	Meta map[string]any `json:"meta,omitempty"`
}

// CollectorConfig holds everything the collector needs.
type CollectorConfig struct {
	WorkerURL       string        // required; e.g. http://localhost:8082
	KafkaConnectURL string        // required; e.g. http://localhost:18083
	NATSMonitorURL  string        // required; e.g. http://localhost:18222
	// KafkaExporterURL is the kafka-exporter sidecar Prometheus text-format
	// endpoint (e.g. http://localhost:9308/metrics). When empty the
	// consumer_lag probe reports "unknown" status instead of erroring.
	KafkaExporterURL string
	Interval         time.Duration // default 15s
	ProbeTimeout     time.Duration // default 2s
	CacheKey         string        // default "system_health:snapshot"
	CacheTTL         time.Duration // default 60s
	LatencyWindow    string        // default "5m" PromQL range
	DebeziumName     string        // default "goopay-mongodb-cdc"
	// LagTopicPrefix restricts per-topic lag aggregation in the snapshot to
	// topics starting with this prefix (e.g. "cdc.goopay."). The total_lag
	// figure still sums across ALL consumer groups/topics so the alert rule
	// mirrors upstream kafka_consumergroup_lag semantics.
	LagTopicPrefix string // default "cdc.goopay."
}

// Collector aggregates external probes and writes a cached snapshot to Redis.
type Collector struct {
	cfg        CollectorConfig
	db         *gorm.DB
	redis      *rediscache.RedisCache
	prom       *infrahttp.PromClient
	httpClient *http.Client
	logger     *zap.Logger

	// alerts is optional — when nil the collector simply skips alert ingest,
	// preserving the original Phase 0 behaviour. When set, each tick calls
	// evaluateAlerts() which fires/resolves rows on the cdc_alerts table.
	alerts *AlertManager
}

// NewCollector wires dependencies with defensible defaults.
func NewCollector(
	cfg CollectorConfig,
	db *gorm.DB,
	redis *rediscache.RedisCache,
	prom *infrahttp.PromClient,
	logger *zap.Logger,
) *Collector {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Second
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 2 * time.Second
	}
	if cfg.CacheKey == "" {
		cfg.CacheKey = "system_health:snapshot"
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 60 * time.Second
	}
	if cfg.LatencyWindow == "" {
		cfg.LatencyWindow = "5m"
	}
	if cfg.DebeziumName == "" {
		cfg.DebeziumName = "goopay-mongodb-cdc"
	}
	if cfg.LagTopicPrefix == "" {
		cfg.LagTopicPrefix = "cdc.goopay."
	}
	return &Collector{
		cfg:   cfg,
		db:    db,
		redis: redis,
		prom:  prom,
		// Shared HTTP client with a small pool; per-request timeout via context.
		httpClient: &http.Client{Timeout: cfg.ProbeTimeout + 500*time.Millisecond},
		logger:     logger,
	}
}

// Run loops the collector. It seeds the cache immediately (so handlers don't
// 503 forever on cold start) and then ticks every Interval.
func (c *Collector) Run(ctx context.Context) {
	if c == nil {
		return
	}
	// Immediate seed; ignore error (handler will return 503 until the next tick).
	c.collectAndCache(ctx)

	t := time.NewTicker(c.cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("system health collector stopped")
			return
		case <-t.C:
			c.collectAndCache(ctx)
		}
	}
}

// CollectOnce exposes a single collection pass. Useful for tests / manual refresh.
func (c *Collector) CollectOnce(ctx context.Context) error {
	return c.collectAndCache(ctx)
}

func (c *Collector) collectAndCache(parent context.Context) error {
	snap := &Snapshot{
		Timestamp:      time.Now().UTC(),
		Infrastructure: map[string]any{},
		CDCPipeline:    map[string]any{},
	}

	// Protect concurrent writes into the shared maps.
	var mu sync.Mutex
	set := func(bucket string, k string, v any) {
		mu.Lock()
		defer mu.Unlock()
		switch bucket {
		case "infra":
			snap.Infrastructure[k] = v
		case "pipeline":
			snap.CDCPipeline[k] = v
		}
	}

	g, gCtx := errgroup.WithContext(parent)
	hd := probes.HTTPDeps{Client: c.httpClient, ProbeTimeout: c.cfg.ProbeTimeout}

	// ----- Infrastructure probes -----
	g.Go(func() error { set("infra", "kafka", probes.KafkaConnect(gCtx, hd, c.cfg.KafkaConnectURL)); return nil })
	g.Go(func() error { set("infra", "nats", probes.NATS(gCtx, hd, c.cfg.NATSMonitorURL)); return nil })
	g.Go(func() error { set("infra", "postgres", probes.Postgres(gCtx, c.db, c.cfg.ProbeTimeout)); return nil })
	g.Go(func() error { set("infra", "redis", probes.Redis(gCtx, c.redis, c.cfg.ProbeTimeout)); return nil })

	// ----- CDC pipeline probes -----
	g.Go(func() error { set("pipeline", "worker", probes.Worker(gCtx, hd, c.cfg.WorkerURL)); return nil })
	g.Go(func() error {
		set("pipeline", "debezium", probes.Debezium(gCtx, hd, c.cfg.KafkaConnectURL, c.cfg.DebeziumName))
		return nil
	})
	g.Go(func() error {
		set("pipeline", "consumer_lag", probes.KafkaLag(gCtx, hd, c.cfg.KafkaExporterURL, c.cfg.LagTopicPrefix))
		return nil
	})

	// ----- DB-derived sections (PG-only, each bounded by probeTimeout) -----
	g.Go(func() error {
		snap.Reconciliation = c.queryReconciliation(gCtx)
		return nil
	})
	g.Go(func() error {
		snap.FailedSync = c.queryFailedCount(gCtx)
		return nil
	})
	g.Go(func() error {
		snap.RecentEvents = c.queryRecentEvents(gCtx)
		return nil
	})

	// ----- Prometheus-backed percentiles (Path A + fallback) -----
	g.Go(func() error {
		ctxQ, cancel := context.WithTimeout(gCtx, c.cfg.ProbeTimeout*2)
		defer cancel()
		snap.Latency = c.prom.QueryLatencyTriple(ctxQ, c.cfg.LatencyWindow)
		return nil
	})

	_ = g.Wait() // probes always return nil; per-section status captures failures

	// Compute alerts + overall from already-collected sections.
	snap.Alerts = computeAlerts(snap)
	snap.Overall = computeOverall(snap)

	// Phase 6 — persist alert state machine transitions based on the snapshot.
	// Skip when AlertManager was not wired (backwards compatibility with pure
	// Phase 0 deployments / tests that construct a bare Collector).
	if c.alerts != nil {
		c.evaluateAlerts(parent, snap)
	}

	// Persist to Redis. If Redis is down we simply log — handler will 503 on next read.
	data, err := json.Marshal(snap)
	if err != nil {
		c.logger.Error("marshal snapshot", zap.Error(err))
		return err
	}
	if c.redis == nil {
		c.logger.Warn("redis nil; skipping snapshot cache write")
		return nil
	}
	writeCtx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := c.redis.Set(writeCtx, c.cfg.CacheKey, string(data), c.cfg.CacheTTL); err != nil {
		c.logger.Warn("redis SET snapshot failed", zap.Error(err))
		return err
	}
	return nil
}

// queryReconciliation, queryFailedCount, queryRecentEvents live in
// system_health_queries.go. computeAlerts + computeOverall live in
// system_health_compute.go. This file owns the orchestration
// (Run loop + collectAndCache + Redis cache write).
