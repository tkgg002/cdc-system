package probes

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// NATS probes the JetStream monitoring endpoint /jsz to surface
// stream/consumer/message counts. Up means the JS subsystem is
// reachable; the per-stream depth is left for the FE drilldown.
func NATS(ctx context.Context, deps HTTPDeps, natsMonitorURL string) map[string]any {
	url := strings.TrimRight(natsMonitorURL, "/") + "/jsz"
	start := time.Now()
	body, code, err := deps.Get(ctx, url)
	sec := map[string]any{"latency_ms": time.Since(start).Milliseconds()}
	if err != nil {
		sec["status"] = StatusUnknown
		sec["error"] = SanitizeErr(err)
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
