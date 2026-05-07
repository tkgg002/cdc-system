package probes

import (
	"context"
	"time"

	"cdc-cms-service/pkgs/rediscache"
)

// Redis probes the cache via PING. nil cache → status=unknown to
// keep the contract sane during early bootstrap (the collector seeds
// before all dependencies are wired in tests).
func Redis(ctx context.Context, redis *rediscache.RedisCache, probeTimeout time.Duration) map[string]any {
	if redis == nil {
		return map[string]any{"status": StatusUnknown}
	}
	start := time.Now()
	ctxQ, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	if err := redis.Ping(ctxQ); err != nil {
		return map[string]any{"status": StatusDown, "error": SanitizeErr(err), "latency_ms": time.Since(start).Milliseconds()}
	}
	return map[string]any{"status": StatusUp, "latency_ms": time.Since(start).Milliseconds()}
}
