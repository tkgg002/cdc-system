// system_health_compute_test.go — pure-fn coverage for the
// alert/overall derivation. The wire shape (`level`, `component`,
// `message`) is what the FE banner reads, so we pin it here.
//
// Project convention: collector orchestration tests live in
// system_health_collector_test.go; this file only covers the two
// pure helpers that translate Snapshot → alerts/overall.
package service

import (
	"strings"
	"testing"
)

func TestComputeAlerts_EmptySnapshotIsHealthy(t *testing.T) {
	snap := &Snapshot{}
	alerts := computeAlerts(snap)
	if len(alerts) != 0 {
		t.Fatalf("empty snapshot: want no alerts, got %d (%v)", len(alerts), alerts)
	}
}

func TestComputeAlerts_InfrastructureDownEmitsCritical(t *testing.T) {
	snap := &Snapshot{
		Infrastructure: map[string]any{
			"postgres_master": map[string]any{"status": StatusDown},
			"redis":           map[string]any{"status": StatusOK}, // ignored
			"junk":            "not-a-map",                        // skipped silently
		},
	}
	alerts := computeAlerts(snap)
	if len(alerts) != 1 {
		t.Fatalf("want 1 alert, got %d (%v)", len(alerts), alerts)
	}
	a := alerts[0]
	if a["level"] != "critical" || a["component"] != "postgres_master" {
		t.Errorf("unexpected alert: %v", a)
	}
	if !strings.Contains(a["message"].(string), "DOWN") {
		t.Errorf("message missing DOWN: %v", a["message"])
	}
}

func TestComputeAlerts_DebeziumFailedEmitsCritical(t *testing.T) {
	snap := &Snapshot{
		CDCPipeline: map[string]any{
			"debezium": map[string]any{"status": "FAILED"},
		},
	}
	alerts := computeAlerts(snap)
	if len(alerts) != 1 || alerts[0]["component"] != "debezium" {
		t.Fatalf("debezium FAILED: want 1 critical alert, got %v", alerts)
	}
}

func TestComputeAlerts_ReconciliationDriftAndError(t *testing.T) {
	snap := &Snapshot{
		Reconciliation: []map[string]any{
			{"status": "drift", "table": "orders"},
			{"status": "drift", "table": "users"},
			{"status": "error", "table": "payments"},
			{"status": "ok", "table": "ledger"}, // ignored
		},
	}
	alerts := computeAlerts(snap)
	if len(alerts) != 2 {
		t.Fatalf("want drift+error alerts (2), got %d (%v)", len(alerts), alerts)
	}
	var drift, errAlert map[string]any
	for _, a := range alerts {
		if a["level"] == "warning" {
			drift = a
		}
		if a["level"] == "critical" {
			errAlert = a
		}
	}
	if drift == nil || !strings.Contains(drift["message"].(string), "2 tables") {
		t.Errorf("drift summary wrong: %v", drift)
	}
	if errAlert == nil || !strings.Contains(errAlert["message"].(string), "payments") {
		t.Errorf("error alert should list table names: %v", errAlert)
	}
}

func TestComputeAlerts_FailedSyncEmitsWarning(t *testing.T) {
	snap := &Snapshot{
		FailedSync: map[string]any{"count_1h": int64(7)},
	}
	alerts := computeAlerts(snap)
	if len(alerts) != 1 || alerts[0]["level"] != "warning" {
		t.Fatalf("failed sync: want 1 warning, got %v", alerts)
	}
	if !strings.Contains(alerts[0]["message"].(string), "7") {
		t.Errorf("count missing in message: %v", alerts[0])
	}
	// count_1h == 0 must NOT emit.
	snap2 := &Snapshot{FailedSync: map[string]any{"count_1h": int64(0)}}
	if a := computeAlerts(snap2); len(a) != 0 {
		t.Errorf("zero count should be silent, got %v", a)
	}
}

func TestComputeOverall_CriticalBeatsWarning(t *testing.T) {
	snap := &Snapshot{
		Alerts: []map[string]any{
			{"level": "warning"},
			{"level": "critical"},
			{"level": "warning"},
		},
	}
	if got := computeOverall(snap); got != "critical" {
		t.Errorf("critical should win: got %q", got)
	}
}

func TestComputeOverall_WarningOnlyIsDegraded(t *testing.T) {
	snap := &Snapshot{
		Alerts: []map[string]any{{"level": "warning"}, {"level": "warning"}},
	}
	if got := computeOverall(snap); got != "degraded" {
		t.Errorf("warning only: got %q want degraded", got)
	}
}

func TestComputeOverall_EmptyIsHealthy(t *testing.T) {
	if got := computeOverall(&Snapshot{}); got != "healthy" {
		t.Errorf("empty: got %q want healthy", got)
	}
}
