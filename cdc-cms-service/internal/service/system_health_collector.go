// Package service — system_health_collector.go
//
// Purpose: Run health probes asynchronously in the background, cache a
// snapshot JSON in Redis (`system_health:snapshot`, TTL 60s), and let the
// HTTP handler simply read from cache. This replaces a 5-way synchronous
// aggregate (kafka-connect, worker, nats, postgres, redis, airbyte,
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
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"cdc-cms-service/internal/model"
	"cdc-cms-service/pkgs/airbyte"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
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
	Latency         LatencyResult          `json:"latency"`
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
	cfg           CollectorConfig
	db            *gorm.DB
	redis         *rediscache.RedisCache
	airbyteClient *airbyte.Client
	prom          *PromClient
	httpClient    *http.Client
	logger        *zap.Logger

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
	airbyteClient *airbyte.Client,
	prom *PromClient,
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
		cfg:           cfg,
		db:            db,
		redis:         redis,
		airbyteClient: airbyteClient,
		prom:          prom,
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

	// ----- Infrastructure probes -----
	g.Go(func() error { set("infra", "kafka", c.probeKafkaConnect(gCtx)); return nil })
	g.Go(func() error { set("infra", "nats", c.probeNATS(gCtx)); return nil })
	g.Go(func() error { set("infra", "postgres", c.probePostgres(gCtx)); return nil })
	g.Go(func() error { set("infra", "redis", c.probeRedis(gCtx)); return nil })
	g.Go(func() error { set("infra", "airbyte", c.probeAirbyte(gCtx)); return nil })

	// ----- CDC pipeline probes -----
	g.Go(func() error { set("pipeline", "worker", c.probeWorker(gCtx)); return nil })
	g.Go(func() error { set("pipeline", "debezium", c.probeDebezium(gCtx)); return nil })
	g.Go(func() error { set("pipeline", "consumer_lag", c.probeKafkaLag(gCtx)); return nil })

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

// ----- Probes -----

func (c *Collector) probeWorker(ctx context.Context) map[string]any {
	url := strings.TrimRight(c.cfg.WorkerURL, "/") + "/health"
	start := time.Now()
	body, code, err := c.httpGet(ctx, url)
	sec := map[string]any{"latency_ms": time.Since(start).Milliseconds()}
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = sanitizeErr(err)
		return sec
	}
	if code >= 300 {
		sec["status"] = StatusDown
		sec["http_status"] = code
		return sec
	}
	var obj map[string]any
	_ = json.Unmarshal(body, &obj)
	if obj == nil {
		obj = map[string]any{}
	}
	obj["status"] = StatusUp
	obj["latency_ms"] = sec["latency_ms"]
	return obj
}

func (c *Collector) probeKafkaConnect(ctx context.Context) map[string]any {
	url := strings.TrimRight(c.cfg.KafkaConnectURL, "/") + "/"
	start := time.Now()
	body, code, err := c.httpGet(ctx, url)
	sec := map[string]any{"latency_ms": time.Since(start).Milliseconds()}
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = sanitizeErr(err)
		return sec
	}
	if code >= 300 {
		sec["status"] = StatusDown
		sec["http_status"] = code
		return sec
	}
	var obj map[string]any
	_ = json.Unmarshal(body, &obj)
	if obj == nil {
		obj = map[string]any{}
	}
	obj["status"] = StatusUp
	obj["latency_ms"] = sec["latency_ms"]
	return obj
}

func (c *Collector) probeDebezium(ctx context.Context) map[string]any {
	url := strings.TrimRight(c.cfg.KafkaConnectURL, "/") + "/connectors/" + c.cfg.DebeziumName + "/status"
	body, code, err := c.httpGet(ctx, url)
	if err != nil {
		return map[string]any{"status": StatusUnknown, "error": sanitizeErr(err)}
	}
	if code >= 300 {
		return map[string]any{"status": StatusDown, "http_status": code}
	}
	var obj map[string]any
	_ = json.Unmarshal(body, &obj)
	if obj == nil {
		return map[string]any{"status": StatusUnknown, "error": "empty response"}
	}

	connector, _ := obj["connector"].(map[string]any)
	tasks, _ := obj["tasks"].([]any)

	taskDetails := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		task, _ := t.(map[string]any)
		if task == nil {
			continue
		}
		d := map[string]any{"id": task["id"], "state": task["state"]}
		if task["state"] == "FAILED" {
			if trace, ok := task["trace"].(string); ok {
				if len(trace) > 500 {
					trace = trace[:500] + "..."
				}
				d["trace"] = trace
			}
		}
		taskDetails = append(taskDetails, d)
	}

	state := StatusUnknown
	if connector != nil {
		if s, ok := connector["state"].(string); ok {
			state = s
		}
	}

	return map[string]any{
		"status":    state,
		"connector": c.cfg.DebeziumName,
		"tasks":     taskDetails,
	}
}

// probeKafkaLag scrapes the kafka-exporter sidecar's Prometheus text endpoint
// and aggregates `kafka_consumergroup_lag{consumergroup,topic,partition}` into
// `total_lag` + a per-topic breakdown restricted to the CDC topic prefix.
//
// Shape written to snap.CDCPipeline["consumer_lag"]:
//
//	{
//	  "status": "ok" | "unknown" | "down",
//	  "source": "kafka_exporter",
//	  "total_lag": <int64>,
//	  "per_topic": { "cdc.goopay.foo": <int64>, ... },
//	  "latency_ms": <int64>,
//	  "error": "..."                // only on failure
//	}
//
// Absent/failed scrapes degrade gracefully: downstream `HighConsumerLag` alert
// reads `total_lag` via toFloat64 and a missing field coerces to 0 (no fire),
// keeping behaviour backward-compatible with the pre-wired snapshot.
func (c *Collector) probeKafkaLag(ctx context.Context) map[string]any {
	start := time.Now()
	sec := map[string]any{
		"source":     "kafka_exporter",
		"latency_ms": int64(0),
	}

	if strings.TrimSpace(c.cfg.KafkaExporterURL) == "" {
		sec["status"] = StatusUnknown
		sec["error"] = "kafka_exporter_url not configured"
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxQ, http.MethodGet, c.cfg.KafkaExporterURL, nil)
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = sanitizeErr(err)
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = sanitizeErr(err)
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		sec["status"] = StatusDown
		sec["http_status"] = resp.StatusCode
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	parser := expfmt.NewTextParser(prommodel.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = "parse metrics: " + sanitizeErr(err)
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	fam := families["kafka_consumergroup_lag"]
	if fam == nil {
		// kafka-exporter up but metric absent (no consumers yet). Report ok
		// with zero lag so downstream alert sees a definitive value.
		sec["status"] = StatusOK
		sec["total_lag"] = int64(0)
		sec["per_topic"] = map[string]int64{}
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	var totalLag int64
	perTopic := map[string]int64{}
	for _, m := range fam.GetMetric() {
		g := m.GetGauge()
		if g == nil {
			continue
		}
		v := g.GetValue()
		if v < 0 {
			// kafka-exporter occasionally reports -1 while rebalancing; ignore.
			continue
		}
		lag := int64(v)
		totalLag += lag

		var topic string
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "topic" {
				topic = lp.GetValue()
				break
			}
		}
		if c.cfg.LagTopicPrefix == "" || strings.HasPrefix(topic, c.cfg.LagTopicPrefix) {
			perTopic[topic] += lag
		}
	}

	sec["status"] = StatusOK
	sec["total_lag"] = totalLag
	sec["per_topic"] = perTopic
	sec["latency_ms"] = time.Since(start).Milliseconds()
	return sec
}

func (c *Collector) probeNATS(ctx context.Context) map[string]any {
	url := strings.TrimRight(c.cfg.NATSMonitorURL, "/") + "/jsz"
	start := time.Now()
	body, code, err := c.httpGet(ctx, url)
	sec := map[string]any{"latency_ms": time.Since(start).Milliseconds()}
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = sanitizeErr(err)
		return sec
	}
	if code >= 300 {
		sec["status"] = StatusDown
		sec["http_status"] = code
		return sec
	}
	var obj map[string]any
	_ = json.Unmarshal(body, &obj)
	if obj == nil {
		sec["status"] = StatusUnknown
		return sec
	}
	sec["status"] = StatusUp
	sec["streams"] = obj["streams"]
	sec["consumers"] = obj["consumers"]
	sec["messages"] = obj["messages"]
	return sec
}

func (c *Collector) probePostgres(ctx context.Context) map[string]any {
	start := time.Now()
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	var tableCount, rowCount int64
	db := c.db.WithContext(ctxQ)
	if err := db.Model(&model.TableRegistry{}).Count(&tableCount).Error; err != nil {
		return map[string]any{"status": StatusDown, "error": sanitizeErr(err), "latency_ms": time.Since(start).Milliseconds()}
	}
	db.Raw("SELECT COALESCE(SUM(n_live_tup),0) FROM pg_stat_user_tables WHERE schemaname='public'").Scan(&rowCount)

	return map[string]any{
		"status":            StatusUp,
		"tables_registered": tableCount,
		"total_rows":        rowCount,
		"latency_ms":        time.Since(start).Milliseconds(),
	}
}

func (c *Collector) probeRedis(ctx context.Context) map[string]any {
	if c.redis == nil {
		return map[string]any{"status": StatusUnknown}
	}
	start := time.Now()
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()
	if err := c.redis.Ping(ctxQ); err != nil {
		return map[string]any{"status": StatusDown, "error": sanitizeErr(err), "latency_ms": time.Since(start).Milliseconds()}
	}
	return map[string]any{"status": StatusUp, "latency_ms": time.Since(start).Milliseconds()}
}

func (c *Collector) probeAirbyte(ctx context.Context) map[string]any {
	if c.airbyteClient == nil {
		return map[string]any{"status": StatusUnknown}
	}
	base := strings.TrimRight(c.airbyteClient.GetBaseURL(), "/")
	start := time.Now()
	_, code, err := c.httpGet(ctx, base+"/v1/health")
	if err != nil {
		return map[string]any{"status": StatusDown, "error": sanitizeErr(err), "latency_ms": time.Since(start).Milliseconds()}
	}
	if code >= 300 {
		return map[string]any{"status": StatusDown, "http_status": code, "latency_ms": time.Since(start).Milliseconds()}
	}
	return map[string]any{"status": StatusUp, "latency_ms": time.Since(start).Milliseconds()}
}

// ----- DB-derived sections -----

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

func (c *Collector) queryFailedCount(ctx context.Context) map[string]any {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	// Bounded range (`> lower AND <= NOW()`) enables runtime partition pruning on
	// the range-partitioned table `failed_sync_logs` (migration 010). Without the
	// upper bound, Postgres opens every partition (Append over N partitions) which
	// caused 300ms+ planning overhead in production.
	var count24h, count1h int64
	c.db.WithContext(ctxQ).Model(&model.FailedSyncLog{}).
		Where("created_at > NOW() - INTERVAL '24 hours' AND created_at <= NOW()").Count(&count24h)
	c.db.WithContext(ctxQ).Model(&model.FailedSyncLog{}).
		Where("created_at > NOW() - INTERVAL '1 hour' AND created_at <= NOW()").Count(&count1h)
	return map[string]any{"count_24h": count24h, "count_1h": count1h}
}

func (c *Collector) queryRecentEvents(ctx context.Context) []map[string]any {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()

	// Bound the scan to the last 24h so the daily-partitioned `cdc_activity_log`
	// only opens 1–2 partitions (via runtime pruning) instead of merge-appending
	// every partition. Index `idx_act_new_started` on started_at DESC serves the
	// ORDER BY + LIMIT within each partition cheaply.
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

// ----- HTTP helper -----

// httpGet performs a GET with the collector's probe timeout enforced via ctx.
func (c *Collector) httpGet(ctx context.Context, url string) ([]byte, int, error) {
	ctxQ, cancel := context.WithTimeout(ctx, c.cfg.ProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctxQ, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// sanitizeErr avoids leaking full URLs or credentials in error strings.
// (Security gate: per CLAUDE.md Rule 8 — fallback must not leak internal info.)
//
// Strategy: scan left-to-right, when we hit "://" collapse everything up to
// the next space/quote into a fixed marker and continue from AFTER the marker
// (not from the "://" we just wrote — that would infinite-loop).
func sanitizeErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	const marker = "<scheme-redacted>"

	var out strings.Builder
	out.Grow(len(msg))
	i := 0
	for i < len(msg) {
		j := strings.Index(msg[i:], "://")
		if j < 0 {
			out.WriteString(msg[i:])
			break
		}
		// absolute offset of "://" in msg
		at := i + j
		// Walk backwards to consume the scheme prefix (alphanum + "+-.").
		start := at
		for start > i && isSchemeByte(msg[start-1]) {
			start--
		}
		// Walk forward to find the URL terminator.
		end := at + 3
		for end < len(msg) {
			ch := msg[end]
			if ch == ' ' || ch == '"' || ch == '\'' || ch == '\n' || ch == '\t' {
				break
			}
			end++
		}
		out.WriteString(msg[i:start])
		out.WriteString(marker)
		i = end
	}
	return out.String()
}

func isSchemeByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '+' || b == '-' || b == '.':
		return true
	}
	return false
}

// ----- Overall + Alerts -----

// computeAlerts mirrors the existing contract so the FE alert banner keeps working.
func computeAlerts(snap *Snapshot) []map[string]any {
	var alerts []map[string]any

	for name, v := range snap.Infrastructure {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if s, _ := m["status"].(string); s == StatusDown {
			alerts = append(alerts, map[string]any{
				"level":     "critical",
				"component": name,
				"message":   name + " is DOWN",
			})
		}
	}

	if deb, ok := snap.CDCPipeline["debezium"].(map[string]any); ok {
		if s, _ := deb["status"].(string); s == "FAILED" {
			alerts = append(alerts, map[string]any{
				"level":     "critical",
				"component": "debezium",
				"message":   "Debezium connector FAILED",
			})
		}
	}

	driftCount, errorCount := 0, 0
	var errorTables []string
	for _, r := range snap.Reconciliation {
		if s, _ := r["status"].(string); s == "drift" {
			driftCount++
		}
		if s, _ := r["status"].(string); s == "error" {
			errorCount++
			if t, ok := r["table"].(string); ok {
				errorTables = append(errorTables, t)
			}
		}
	}
	if driftCount > 0 {
		alerts = append(alerts, map[string]any{
			"level":     "warning",
			"component": "reconciliation",
			"message":   fmt.Sprintf("%d tables have data drift", driftCount),
		})
	}
	if errorCount > 0 {
		msg := fmt.Sprintf("%d tables failed reconciliation check (source unreachable)", errorCount)
		if len(errorTables) > 0 && len(errorTables) <= 5 {
			msg += ": " + fmt.Sprintf("%v", errorTables)
		}
		alerts = append(alerts, map[string]any{
			"level":     "critical",
			"component": "reconciliation",
			"message":   msg,
		})
	}

	if c, ok := snap.FailedSync["count_1h"].(int64); ok && c > 0 {
		alerts = append(alerts, map[string]any{
			"level":     "warning",
			"component": "sync",
			"message":   fmt.Sprintf("%d failed syncs in last hour", c),
		})
	}

	return alerts
}

// computeOverall walks alerts and reports "healthy" / "degraded" / "critical".
func computeOverall(snap *Snapshot) string {
	overall := "healthy"
	for _, a := range snap.Alerts {
		if a["level"] == "critical" {
			return "critical"
		}
		if a["level"] == "warning" {
			overall = "degraded"
		}
	}
	return overall
}
