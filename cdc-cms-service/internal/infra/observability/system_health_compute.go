// system_health_compute.go — pure functions that derive the FE-facing
// `alerts` slice and the rolled-up `overall` field from a snapshot.
// Lives next to the collector because both Snapshot and the status
// vocabulary are defined there; splitting these out keeps the
// orchestration file (system_health_collector.go) under the 300-line
// readability budget without exposing internal-only helpers.
package observability

import "fmt"

// computeAlerts mirrors the existing wire contract so the FE alert
// banner keeps working. It walks the per-section status maps the
// probes already wrote into the snapshot and translates them into a
// flat list of {level, component, message} objects.
//
// Note: the AlertManager state machine in system_health_alerts.go is
// a SEPARATE concern (Phase 6 persistence). This function stays
// in-process and is consumed by the cached snapshot only.
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

// computeOverall walks alerts and reports "healthy" / "degraded" /
// "critical". critical wins over warning; absence of either is healthy.
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
