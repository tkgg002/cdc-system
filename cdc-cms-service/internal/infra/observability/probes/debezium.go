package probes

import (
	"context"
	"encoding/json"
	"strings"
)

// Debezium probes the named CDC connector's status endpoint. Returns
// the connector-level state (RUNNING / FAILED / PAUSED / etc) plus a
// per-task summary. FAILED tasks include a truncated trace (≤500
// chars) so the FE can surface the root cause without leaking massive
// stack traces in the cached snapshot.
func Debezium(ctx context.Context, deps HTTPDeps, kafkaConnectURL, debeziumName string) map[string]any {
	url := strings.TrimRight(kafkaConnectURL, "/") + "/connectors/" + debeziumName + "/status"
	body, code, err := deps.Get(ctx, url)
	if err != nil {
		return map[string]any{"status": StatusUnknown, "error": SanitizeErr(err)}
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
		"connector": debeziumName,
		"tasks":     taskDetails,
	}
}
