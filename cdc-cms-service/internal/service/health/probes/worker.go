package probes

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// Worker probes the centralized-data-service worker via its no-auth
// /healthz dev endpoint. The Phase F1 admin-api guards /health with
// JWT, so /healthz is what the collector must scrape.
func Worker(ctx context.Context, deps HTTPDeps, workerURL string) map[string]any {
	url := strings.TrimRight(workerURL, "/") + "/healthz"
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
		obj = map[string]any{}
	}
	obj["status"] = StatusUp
	obj["latency_ms"] = sec["latency_ms"]
	return obj
}
