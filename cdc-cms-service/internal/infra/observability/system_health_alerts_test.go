// system_health_alerts_test.go — pure-fn guards for the
// snapshot → detected-condition pipeline. The DB-bound
// AlertManager.Fire/Resolve path stays in deploy-time E2E
// (project convention: no sqlmock).
//
// What we pin here is the closed set of detection rules
// (DebeziumConnectorFailed / HighConsumerLag / ReconDrift /
// InfrastructureDown) and the type-coerce contract that lets
// the rules read JSON-numeric variants safely.
package observability

import (
	"testing"
)

func TestToFloat64_NumericVariants(t *testing.T) {
	cases := []struct {
		in   any
		want float64
	}{
		{int(7), 7},
		{int32(8), 8},
		{int64(9), 9},
		{float32(1.5), 1.5},
		{float64(2.5), 2.5},
		{"not-a-number", 0}, // unknown → 0 (no spurious alert fire)
		{nil, 0},
		{map[string]int{"k": 1}, 0}, // map default → 0
	}
	for _, c := range cases {
		if got := toFloat64(c.in); got != c.want {
			t.Errorf("toFloat64(%T %v): got %v want %v", c.in, c.in, got, c.want)
		}
	}
}

func TestOwnsAlertName(t *testing.T) {
	c := &Collector{}
	owned := []string{
		"DebeziumConnectorFailed",
		"HighConsumerLag",
		"ReconDrift",
		"InfrastructureDown",
	}
	for _, n := range owned {
		if !c.ownsAlertName(n) {
			t.Errorf("ownsAlertName(%q): want true", n)
		}
	}
	for _, n := range []string{"", "ManualAlert", "Random"} {
		if c.ownsAlertName(n) {
			t.Errorf("ownsAlertName(%q): want false", n)
		}
	}
}

func TestDetectConditions_DebeziumFailed(t *testing.T) {
	c := &Collector{cfg: CollectorConfig{DebeziumName: "goopay-mongodb-cdc"}}
	snap := &Snapshot{
		CDCPipeline: map[string]any{
			"debezium": map[string]any{"status": "FAILED", "connector": "x"},
		},
	}
	out := c.detectConditions(snap)
	if len(out) != 1 || out[0].req.Name != "DebeziumConnectorFailed" {
		t.Fatalf("FAILED state: want DebeziumConnectorFailed, got %v", out)
	}
	if out[0].req.Severity != "critical" {
		t.Errorf("severity: want critical, got %q", out[0].req.Severity)
	}
	if out[0].req.Labels["connector"] != "x" {
		t.Errorf("connector label lost: %v", out[0].req.Labels)
	}
}

func TestDetectConditions_DebeziumDownTreatedAsFailed(t *testing.T) {
	// Probe maps unreachable connector to status="down"; it must
	// produce the same alert as FAILED because the downstream
	// effect (no events flowing) is identical.
	c := &Collector{cfg: CollectorConfig{DebeziumName: "fallback-name"}}
	snap := &Snapshot{
		CDCPipeline: map[string]any{
			"debezium": map[string]any{"status": StatusDown},
		},
	}
	out := c.detectConditions(snap)
	if len(out) != 1 || out[0].req.Name != "DebeziumConnectorFailed" {
		t.Fatalf("DOWN state must fire same alert: %v", out)
	}
	if out[0].req.Labels["connector"] != "fallback-name" {
		t.Errorf("missing connector should fall back to cfg.DebeziumName, got %v", out[0].req.Labels)
	}
}

func TestDetectConditions_DebeziumTaskFailed(t *testing.T) {
	c := &Collector{cfg: CollectorConfig{DebeziumName: "x"}}
	snap := &Snapshot{
		CDCPipeline: map[string]any{
			"debezium": map[string]any{
				"status": "RUNNING",
				"tasks": []map[string]any{
					{"state": "RUNNING"},
					{"state": "FAILED"},
				},
			},
		},
	}
	out := c.detectConditions(snap)
	if len(out) != 1 {
		t.Fatalf("task FAILED escalates: want 1, got %d (%v)", len(out), out)
	}
}

func TestDetectConditions_ConsumerLagThresholds(t *testing.T) {
	c := &Collector{}
	cases := []struct {
		lag      any
		want     int
		severity string
	}{
		{int64(150_000), 1, "critical"},
		{int64(50_000), 1, "warning"},
		{int64(5_000), 0, ""},      // below threshold
		{float64(100_001), 1, "critical"},
	}
	for _, tc := range cases {
		snap := &Snapshot{
			CDCPipeline: map[string]any{
				"consumer_lag": map[string]any{"total_lag": tc.lag},
			},
		}
		out := c.detectConditions(snap)
		if len(out) != tc.want {
			t.Errorf("lag=%v: want %d alerts, got %d (%v)", tc.lag, tc.want, len(out), out)
			continue
		}
		if tc.want > 0 && out[0].req.Severity != tc.severity {
			t.Errorf("lag=%v: severity %q want %q", tc.lag, out[0].req.Severity, tc.severity)
		}
	}
}

func TestDetectConditions_ReconDriftPerTable(t *testing.T) {
	c := &Collector{}
	snap := &Snapshot{
		Reconciliation: []map[string]any{
			{"status": "drift", "table": "orders"},
			{"status": "drift", "table": "users"},
			{"status": "ok", "table": "ledger"},
			{"status": "error", "table": "payments"}, // not "drift" → ignored here
		},
	}
	out := c.detectConditions(snap)
	if len(out) != 2 {
		t.Fatalf("want 2 ReconDrift, got %d (%v)", len(out), out)
	}
	tables := map[string]bool{}
	for _, c := range out {
		if c.req.Name != "ReconDrift" || c.req.Severity != "warning" {
			t.Errorf("unexpected: %+v", c.req)
		}
		tables[c.req.Labels["table"]] = true
	}
	if !tables["orders"] || !tables["users"] {
		t.Errorf("missing per-table label: %v", tables)
	}
}

func TestDetectConditions_InfrastructureDown(t *testing.T) {
	c := &Collector{}
	snap := &Snapshot{
		Infrastructure: map[string]any{
			"postgres": map[string]any{"status": StatusDown},
			"redis":    map[string]any{"status": StatusOK},
			"mongo":    map[string]any{"status": StatusDown},
			"kafka":    map[string]any{"status": StatusDown}, // not in detect list
		},
	}
	out := c.detectConditions(snap)
	if len(out) != 2 {
		t.Fatalf("want 2 InfrastructureDown, got %d (%v)", len(out), out)
	}
	got := map[string]bool{}
	for _, cond := range out {
		got[cond.req.Labels["component"]] = true
	}
	if !got["postgres"] || !got["mongo"] {
		t.Errorf("missing expected components: %v", got)
	}
	if got["kafka"] {
		t.Errorf("kafka not in detect list, must not fire: %v", got)
	}
}

func TestDetectConditions_EmptySnapshotIsSilent(t *testing.T) {
	c := &Collector{cfg: CollectorConfig{DebeziumName: "x"}}
	if out := c.detectConditions(&Snapshot{}); len(out) != 0 {
		t.Fatalf("empty snapshot must produce no conditions, got %v", out)
	}
}
