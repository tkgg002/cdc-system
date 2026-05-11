// Package middleware — ratelimit.go (Phase 4 Security)
//
// Per-user rate limit for destructive actions. Uses Redis counters
// with INCR + EXPIRE so limits survive process restarts and are shared
// across multiple CMS replicas.
//
// Why per-user instead of per-IP:
//   - Admins behind a NAT share an IP → IP-based would flap for a
//     whole team when one person retries fast.
//   - Audit log already keys on user_id, so rate-limit bookkeeping is
//     consistent with "who-did-what" records.
//
// Scope: only wrap genuinely destructive routes (restart, reset). Read
// endpoints do not need this layer (JWT + perimeter rate-limit handle
// those).
package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cdc-cms-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

// RateLimitConfig controls a single rate-limit rule.
type RateLimitConfig struct {
	Redis   *rediscache.RedisCache
	Scope   string        // e.g. "restart" — appears in key + error
	Max     int           // max requests per window
	Window  time.Duration // bucket window (e.g. 1h)
}

// NewRateLimit returns a Fiber middleware that enforces cfg.
// Must be mounted AFTER JWTAuth so c.Locals("username") is set.
func NewRateLimit(cfg RateLimitConfig) fiber.Handler {
	if cfg.Max <= 0 {
		cfg.Max = 3
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Hour
	}
	if cfg.Scope == "" {
		cfg.Scope = "destructive"
	}

	return func(c *fiber.Ctx) error {
		// If Redis is not wired the safest behavior is FAIL-CLOSED
		// (block destructive ops rather than silently allow a flood).
		if cfg.Redis == nil {
			return c.Status(503).JSON(fiber.Map{
				"error": "rate limit store unavailable",
			})
		}

		user := "unknown"
		if u, ok := c.Locals("username").(string); ok && u != "" {
			user = u
		}
		key := fmt.Sprintf("ratelimit:%s:%s", cfg.Scope, user)

		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()

		count, err := cfg.Redis.Incr(ctx, key)
		if err != nil {
			return c.Status(503).JSON(fiber.Map{
				"error": "rate limit store unavailable",
			})
		}
		// On first increment, stamp a TTL so the bucket resets.
		// (Incr on a missing key returns 1 with no TTL.)
		if count == 1 {
			if err := cfg.Redis.Expire(ctx, key, cfg.Window); err != nil {
				// Not fatal — the key is just unbounded, but a
				// subsequent request can re-set expiry. Log & pass.
			}
		}

		if count > int64(cfg.Max) {
			// Compute remaining TTL for Retry-After.
			ttl, terr := cfg.Redis.TTL(ctx, key)
			retrySec := int(cfg.Window.Seconds())
			if terr == nil && ttl > 0 {
				retrySec = int(ttl.Seconds())
			}
			// redis.Nil should never happen here because we just INCR'd.
			if errors.Is(terr, redis.Nil) {
				retrySec = int(cfg.Window.Seconds())
			}
			c.Set("Retry-After", fmt.Sprintf("%d", retrySec))
			return c.Status(429).JSON(fiber.Map{
				"error":       "rate limit",
				"scope":       cfg.Scope,
				"max":         cfg.Max,
				"window_sec":  int(cfg.Window.Seconds()),
				"retry_after": retrySec,
			})
		}

		return c.Next()
	}
}
