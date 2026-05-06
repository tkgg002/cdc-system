package probes

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
)

// KafkaLag scrapes the kafka-exporter sidecar's Prometheus text
// endpoint and aggregates `kafka_consumergroup_lag{...}` into
// total_lag + a per-topic breakdown restricted to the CDC topic
// prefix. Empty kafkaExporterURL → status=unknown (graceful when the
// sidecar isn't deployed). Negative gauge values from kafka-exporter
// during rebalance are skipped.
//
// Output shape (cached as snap.CDCPipeline["consumer_lag"]):
//
//	{
//	  "status":     "ok" | "unknown" | "down",
//	  "source":     "kafka_exporter",
//	  "total_lag":  <int64>,
//	  "per_topic":  { "cdc.goopay.foo": <int64>, ... },
//	  "latency_ms": <int64>,
//	  "error":      "..."   // only on failure
//	}
//
// The HighConsumerLag alert reads total_lag via toFloat64; a missing
// field coerces to 0 (no fire), preserving backward compatibility
// with snapshots produced before kafka-exporter was wired.
func KafkaLag(ctx context.Context, deps HTTPDeps, kafkaExporterURL, lagTopicPrefix string) map[string]any {
	start := time.Now()
	sec := map[string]any{
		"source":     "kafka_exporter",
		"latency_ms": int64(0),
	}

	if strings.TrimSpace(kafkaExporterURL) == "" {
		sec["status"] = StatusUnknown
		sec["error"] = "kafka_exporter_url not configured"
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	ctxQ, cancel := context.WithTimeout(ctx, deps.ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxQ, http.MethodGet, kafkaExporterURL, nil)
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = SanitizeErr(err)
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}
	resp, err := deps.Client.Do(req)
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = SanitizeErr(err)
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
		sec["error"] = "parse metrics: " + SanitizeErr(err)
		sec["latency_ms"] = time.Since(start).Milliseconds()
		return sec
	}

	fam := families["kafka_consumergroup_lag"]
	if fam == nil {
		// kafka-exporter up but metric absent (no consumers yet).
		// Report ok with zero lag so downstream alert sees a definitive value.
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
		if lagTopicPrefix == "" || strings.HasPrefix(topic, lagTopicPrefix) {
			perTopic[topic] += lag
		}
	}

	sec["status"] = StatusOK
	sec["total_lag"] = totalLag
	sec["per_topic"] = perTopic
	sec["latency_ms"] = time.Since(start).Milliseconds()
	return sec
}
