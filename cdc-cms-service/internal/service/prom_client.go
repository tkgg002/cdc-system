// Package service — prom_client.go
//
// Purpose: Wrap Prometheus HTTP API + a fallback that scrapes the Worker
// `/metrics` endpoint and computes percentiles in-process from histogram
// buckets. Replaces the silent-bug "percentile_cont on cdc_activity_log"
// approach (which aggregates batch averages, not individual events).
//
// Design notes:
//   - Path A (preferred): issue PromQL `histogram_quantile(Q, sum by (le)
//     (rate(cdc_e2e_latency_seconds_bucket[W])))` against a configured
//     Prometheus server. Source label = "prometheus".
//   - Path B (fallback): GET `<workerURL>/metrics`, parse Prometheus text
//     exposition using `expfmt.TextParser`, then compute the quantile from
//     the cumulative histogram buckets. Source label = "fallback_worker_metrics".
//   - If both fail, return source "unknown" with a sentinel NaN value so the
//     caller can decide per-section status (kept non-breaking for the API).
package service

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
	"go.uber.org/zap"
)

// PercentileSource indicates which path produced the percentile value.
type PercentileSource string

const (
	SourcePrometheus     PercentileSource = "prometheus"
	SourceFallbackWorker PercentileSource = "fallback_worker_metrics"
	SourceUnknown        PercentileSource = "unknown"
)

// LatencyResult bundles the three standard percentile buckets + source provenance.
type LatencyResult struct {
	Source PercentileSource `json:"source"`
	P50Ms  float64          `json:"p50_ms"`
	P95Ms  float64          `json:"p95_ms"`
	P99Ms  float64          `json:"p99_ms"`
	Error  string           `json:"error,omitempty"`
}

// PromClient wraps the Prometheus HTTP API + worker-metrics fallback.
type PromClient struct {
	api          promv1.API    // nil when prometheusURL is empty
	workerURL    string        // base URL, e.g. http://localhost:9090
	metricName   string        // histogram base name, e.g. "cdc_e2e_latency_seconds"
	httpTimeout  time.Duration // per-HTTP request timeout
	queryTimeout time.Duration // per-PromQL query timeout
	logger       *zap.Logger
}

// PromClientConfig — all values are explicit so no prod URL is hardcoded.
type PromClientConfig struct {
	PrometheusURL string        // optional; empty => skip Path A
	WorkerURL     string        // required for Path B fallback scrape
	MetricName    string        // default "cdc_e2e_latency_seconds"
	HTTPTimeout   time.Duration // default 2s
	QueryTimeout  time.Duration // default 3s
}

// NewPromClient constructs a client. Either PrometheusURL or WorkerURL must be
// set; otherwise the client will always return SourceUnknown.
func NewPromClient(cfg PromClientConfig, logger *zap.Logger) (*PromClient, error) {
	if cfg.MetricName == "" {
		cfg.MetricName = "cdc_e2e_latency_seconds"
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 2 * time.Second
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 3 * time.Second
	}

	pc := &PromClient{
		workerURL:    strings.TrimRight(cfg.WorkerURL, "/"),
		metricName:   cfg.MetricName,
		httpTimeout:  cfg.HTTPTimeout,
		queryTimeout: cfg.QueryTimeout,
		logger:       logger,
	}

	if cfg.PrometheusURL != "" {
		client, err := promapi.NewClient(promapi.Config{
			Address: cfg.PrometheusURL,
		})
		if err != nil {
			return nil, fmt.Errorf("prom api client: %w", err)
		}
		pc.api = promv1.NewAPI(client)
	}

	return pc, nil
}

// QueryPercentile runs `histogram_quantile(quantile, sum by (le) (rate(<metric>_bucket[rangeWindow])))`.
// On Prometheus error/unreachable, it falls back to scraping the Worker `/metrics` endpoint.
func (p *PromClient) QueryPercentile(ctx context.Context, quantile float64, rangeWindow string) (float64, PercentileSource, error) {
	if p.api != nil {
		ctxQ, cancel := context.WithTimeout(ctx, p.queryTimeout)
		defer cancel()

		q := fmt.Sprintf(
			"histogram_quantile(%g, sum by (le) (rate(%s_bucket[%s])))",
			quantile, p.metricName, rangeWindow,
		)
		result, warnings, err := p.api.Query(ctxQ, q, time.Now())
		if err == nil {
			if len(warnings) > 0 && p.logger != nil {
				p.logger.Debug("prom query warnings", zap.Strings("warnings", warnings), zap.String("q", q))
			}
			if v, ok := extractScalar(result); ok {
				return v, SourcePrometheus, nil
			}
			// empty vector = no samples; treat as fallback opportunity
			if p.logger != nil {
				p.logger.Debug("prom returned empty; falling back", zap.String("q", q))
			}
		} else if p.logger != nil {
			p.logger.Debug("prom query error; falling back", zap.Error(err), zap.String("q", q))
		}
	}

	// Path B: scrape worker /metrics
	if p.workerURL == "" {
		return math.NaN(), SourceUnknown, fmt.Errorf("no prom server and no worker URL configured")
	}
	val, err := p.scrapeWorkerPercentile(ctx, quantile)
	if err != nil {
		return math.NaN(), SourceUnknown, err
	}
	return val, SourceFallbackWorker, nil
}

// QueryLatencyTriple returns P50/P95/P99 in ms and reports the effective source.
// The source reflects the FIRST successful path; if any of the three fails,
// this function records the error but continues — it won't block the overall API.
func (p *PromClient) QueryLatencyTriple(ctx context.Context, rangeWindow string) LatencyResult {
	res := LatencyResult{Source: SourceUnknown}
	var firstErr string

	p50, src50, err := p.QueryPercentile(ctx, 0.50, rangeWindow)
	if err != nil {
		firstErr = err.Error()
	} else {
		res.P50Ms = toMs(p50)
		res.Source = src50
	}

	p95, src95, err := p.QueryPercentile(ctx, 0.95, rangeWindow)
	if err != nil {
		if firstErr == "" {
			firstErr = err.Error()
		}
	} else {
		res.P95Ms = toMs(p95)
		if res.Source == SourceUnknown {
			res.Source = src95
		}
	}

	p99, src99, err := p.QueryPercentile(ctx, 0.99, rangeWindow)
	if err != nil {
		if firstErr == "" {
			firstErr = err.Error()
		}
	} else {
		res.P99Ms = toMs(p99)
		if res.Source == SourceUnknown {
			res.Source = src99
		}
	}

	if res.Source == SourceUnknown && firstErr != "" {
		res.Error = firstErr
	}
	return res
}

// scrapeWorkerPercentile fetches the worker `/metrics` endpoint and computes
// the quantile from the cumulative histogram buckets in-process.
//
// We sum the bucket counters across all label-value permutations because the
// caller wants a global percentile (same semantics as `sum by (le)` in PromQL).
func (p *PromClient) scrapeWorkerPercentile(ctx context.Context, quantile float64) (float64, error) {
	ctxHTTP, cancel := context.WithTimeout(ctx, p.httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxHTTP, http.MethodGet, p.workerURL+"/metrics", nil)
	if err != nil {
		return math.NaN(), fmt.Errorf("build /metrics request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return math.NaN(), fmt.Errorf("fetch worker /metrics: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return math.NaN(), fmt.Errorf("worker /metrics status %d", resp.StatusCode)
	}

	// prometheus/common v0.67 requires explicit ValidationScheme; zero-value
	// TextParser panics with "Invalid name validation scheme: unset".
	parser := expfmt.NewTextParser(prommodel.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return math.NaN(), fmt.Errorf("parse /metrics: %w", err)
	}
	fam, ok := families[p.metricName]
	if !ok || fam == nil {
		return math.NaN(), fmt.Errorf("metric %q not found in worker /metrics", p.metricName)
	}
	return histogramQuantileFromFamily(fam, quantile)
}

// histogramQuantileFromFamily mimics Prometheus' `histogram_quantile` over a
// summed histogram. It treats each bucket's cumulative count as additive
// across all label permutations (global aggregation).
func histogramQuantileFromFamily(fam *dto.MetricFamily, q float64) (float64, error) {
	if fam.GetType() != dto.MetricType_HISTOGRAM {
		return math.NaN(), fmt.Errorf("metric %q is not a histogram", fam.GetName())
	}
	// Aggregate cumulative counts by bucket upper bound.
	sumByLe := map[float64]uint64{}
	var total uint64
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		if h == nil {
			continue
		}
		total += h.GetSampleCount()
		for _, b := range h.GetBucket() {
			sumByLe[b.GetUpperBound()] += b.GetCumulativeCount()
		}
	}
	if total == 0 || len(sumByLe) == 0 {
		return math.NaN(), fmt.Errorf("histogram %q has no samples", fam.GetName())
	}

	// Sort upper bounds ascending.
	les := make([]float64, 0, len(sumByLe))
	for le := range sumByLe {
		les = append(les, le)
	}
	sort.Float64s(les)

	return computeHistogramQuantile(les, sumByLe, q, total)
}

// computeHistogramQuantile is the shared quantile math, exposed for unit
// testing against synthetic bucket data.
//
// Algorithm mirrors prometheus/prometheus `bucketQuantile`:
//  1. rank = q * totalCount
//  2. find the first bucket whose cumulative count >= rank
//  3. linear-interpolate between the bucket's lower and upper bound
//
// Note: because the summed buckets are cumulative (across all label perms),
// the last bucket (+Inf) cannot exceed `total`. If q hits +Inf, we return the
// previous bucket's upper bound (Prom does the same).
func computeHistogramQuantile(les []float64, sumByLe map[float64]uint64, q float64, total uint64) (float64, error) {
	if q < 0 || q > 1 {
		return math.NaN(), fmt.Errorf("quantile out of range: %g", q)
	}
	if total == 0 {
		return math.NaN(), fmt.Errorf("no samples")
	}
	rank := q * float64(total)

	// Find target bucket.
	targetIdx := -1
	for i, le := range les {
		if float64(sumByLe[le]) >= rank {
			targetIdx = i
			break
		}
	}
	if targetIdx == -1 {
		// Rank exceeds everything; use last finite bucket.
		return les[len(les)-1], nil
	}

	upper := les[targetIdx]
	// +Inf bucket: return previous finite upper bound (Prom convention).
	if math.IsInf(upper, +1) {
		if targetIdx == 0 {
			return math.NaN(), fmt.Errorf("only +Inf bucket present")
		}
		return les[targetIdx-1], nil
	}

	var lower float64
	var lowerCount uint64
	if targetIdx > 0 {
		lower = les[targetIdx-1]
		lowerCount = sumByLe[les[targetIdx-1]]
	}
	upperCount := sumByLe[upper]

	bucketCount := float64(upperCount - lowerCount)
	if bucketCount <= 0 {
		return upper, nil
	}
	frac := (rank - float64(lowerCount)) / bucketCount
	return lower + (upper-lower)*frac, nil
}

// extractScalar pulls a single value from a PromQL Vector result (the typical
// shape of `histogram_quantile` when aggregated with `sum by (le)`).
func extractScalar(v any) (float64, bool) {
	switch vv := v.(type) {
	case prommodel.Vector:
		if len(vv) == 0 {
			return 0, false
		}
		f := float64(vv[0].Value)
		if math.IsNaN(f) {
			return 0, false
		}
		return f, true
	case *prommodel.Scalar:
		f := float64(vv.Value)
		if math.IsNaN(f) {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func toMs(sec float64) float64 {
	if math.IsNaN(sec) || math.IsInf(sec, 0) {
		return 0
	}
	return sec * 1000
}
