// system_health_collector_test.go — contract tests.
//
// Scope: pure logic that doesn't require Redis/Postgres connectivity.
// The Redis-backed end-to-end handler test is done at runtime (see
// 03_implementation_v3_cms_phase0.md §Runtime verify).
package service

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestComputeOverall(t *testing.T) {
	cases := []struct {
		name   string
		alerts []map[string]any
		want   string
	}{
		{"no alerts", nil, "healthy"},
		{"warning only", []map[string]any{{"level": "warning"}}, "degraded"},
		{"critical wins", []map[string]any{{"level": "warning"}, {"level": "critical"}}, "critical"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := &Snapshot{Alerts: tc.alerts}
			if got := computeOverall(snap); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestComputeAlertsPerSection(t *testing.T) {
	snap := &Snapshot{
		Infrastructure: map[string]any{
			"kafka":    map[string]any{"status": StatusUp},
			"postgres": map[string]any{"status": StatusDown},
		},
		CDCPipeline: map[string]any{
			"debezium": map[string]any{"status": "FAILED"},
		},
		Reconciliation: []map[string]any{
			{"status": "drift", "table": "payments"},
			{"status": "error", "table": "loyalty"},
		},
		FailedSync: map[string]any{"count_1h": int64(3)},
	}
	alerts := computeAlerts(snap)
	if len(alerts) < 4 {
		t.Fatalf("expected >= 4 alerts; got %d: %+v", len(alerts), alerts)
	}
	// critical for postgres + debezium + recon error
	criticals := 0
	for _, a := range alerts {
		if a["level"] == "critical" {
			criticals++
		}
	}
	if criticals < 3 {
		t.Fatalf("expected >= 3 critical alerts; got %d", criticals)
	}
}

func TestSanitizeErrRedactsURLs(t *testing.T) {
	got := sanitizeErr(errors.New(`Get "http://admin:secret@kafka.internal:18083/connectors/xyz": dial error`))
	if strings.Contains(got, "kafka.internal") || strings.Contains(got, "secret") {
		t.Fatalf("URL/credential not redacted: %q", got)
	}
	if !strings.Contains(got, "redacted") {
		t.Fatalf("expected redaction marker: %q", got)
	}
}

// TestDetectConditions_HighConsumerLag verifies the alert rule fires at the
// documented thresholds (>10k warning, >100k critical) when the probe
// populates cdc_pipeline.consumer_lag.total_lag. Keeps the rule wiring honest
// even if the map schema drifts in future refactors.
func TestDetectConditions_HighConsumerLag(t *testing.T) {
	cases := []struct {
		name     string
		totalLag any
		wantFire bool
		wantSev  string
	}{
		{"no lag", int64(0), false, ""},
		{"below warning", int64(9_999), false, ""},
		{"warning boundary", int64(10_001), true, "warning"},
		{"critical boundary", int64(100_001), true, "critical"},
		{"float accepted", float64(50_000), true, "warning"},
		{"missing field", nil, false, ""},
	}
	c := &Collector{cfg: CollectorConfig{DebeziumName: "x"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lagSection := map[string]any{"status": StatusOK}
			if tc.totalLag != nil {
				lagSection["total_lag"] = tc.totalLag
			}
			snap := &Snapshot{
				CDCPipeline: map[string]any{"consumer_lag": lagSection},
			}
			got := c.detectConditions(snap)
			var fired *detectedCondition
			for i := range got {
				if got[i].req.Name == "HighConsumerLag" {
					fired = &got[i]
					break
				}
			}
			if tc.wantFire && fired == nil {
				t.Fatalf("expected HighConsumerLag to fire; got none")
			}
			if !tc.wantFire && fired != nil {
				t.Fatalf("did not expect HighConsumerLag to fire; got %+v", fired.req)
			}
			if tc.wantFire && fired.req.Severity != tc.wantSev {
				t.Fatalf("severity = %s, want %s", fired.req.Severity, tc.wantSev)
			}
		})
	}
}

func TestSnapshotJSONStability(t *testing.T) {
	// Ensures the top-level keys required by the FE stay present + named.
	snap := &Snapshot{
		Overall:        "healthy",
		Infrastructure: map[string]any{},
		CDCPipeline:    map[string]any{},
		Reconciliation: []map[string]any{},
		FailedSync:     map[string]any{},
		Alerts:         []map[string]any{},
		RecentEvents:   []map[string]any{},
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	required := []string{
		`"timestamp"`,
		`"cache_age_seconds"`,
		`"overall"`,
		`"infrastructure"`,
		`"cdc_pipeline"`,
		`"reconciliation"`,
		`"latency"`,
		`"failed_sync"`,
		`"alerts"`,
		`"recent_events"`,
	}
	body := string(data)
	for _, k := range required {
		if !strings.Contains(body, k) {
			t.Fatalf("missing required key %s in JSON: %s", k, body)
		}
	}
}
