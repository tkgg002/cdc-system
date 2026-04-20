// Package middleware — idempotency.go (Phase 4 Security)
//
// Idempotency-Key middleware per RFC draft-ietf-httpapi-idempotency-key-05.
//
// Why:
//   Destructive admin actions (restart connector, reset offset, heal table)
//   are expensive and sometimes long-running. Networks retry. Browsers
//   replay. Without idempotency, a flaky connection can double-trigger a
//   snapshot and corrupt downstream state. We gate every POST with a
//   client-supplied Idempotency-Key and short-circuit duplicates.
//
// Contract:
//   - Missing/empty `Idempotency-Key` header   → 400.
//   - Concurrent request with same key         → 409 {retry_after: 30}.
//     (The *first* request holds a short-TTL lock so the second one
//      refuses instead of silently returning stale cache.)
//   - Repeat request after success (<1h)       → 200 replay from cache.
//   - Repeat request after failure             → NOT cached → client can
//                                                  retry with same key
//                                                  (that's the whole
//                                                  point of idempotency).
//
// Redis keys (all under `idem:` namespace, collision-free with health):
//   - idem:{route}:{key}:lock     TTL 30s  (active-request lock)
//   - idem:{route}:{key}:response TTL 1h   (cached success body)
//
// Route is derived from c.Route().Path (the pattern like
// `/api/reconciliation/heal/:table`) so separate endpoints can share
// the same client-side key space without colliding.
package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

// redisClient is a tiny surface so the middleware can be unit-tested
// without Redis by passing a fake. Matches the methods we actually use
// on *redis.Client.
type idempotencyStore interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, key string) error
}

// goRedisAdapter wraps a *redis.Client to match idempotencyStore.
// We can't rely on rediscache.RedisCache because it doesn't expose
// SetNX — which is the heart of the lock protocol.
type goRedisAdapter struct {
	c *redis.Client
}

func (a *goRedisAdapter) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return a.c.SetNX(ctx, key, value, ttl).Result()
}
func (a *goRedisAdapter) Get(ctx context.Context, key string) (string, error) {
	return a.c.Get(ctx, key).Result()
}
func (a *goRedisAdapter) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return a.c.Set(ctx, key, value, ttl).Err()
}
func (a *goRedisAdapter) Del(ctx context.Context, key string) error {
	return a.c.Del(ctx, key).Err()
}

// Configurable knobs — exposed so tests can tighten TTLs.
type IdempotencyConfig struct {
	Store       idempotencyStore
	LockTTL     time.Duration // default 30s
	ResponseTTL time.Duration // default 1h
	KeyPrefix   string        // default "idem"
}

func NewIdempotencyFromRedisClient(c *redis.Client) IdempotencyConfig {
	return IdempotencyConfig{
		Store:       &goRedisAdapter{c: c},
		LockTTL:     30 * time.Second,
		ResponseTTL: 1 * time.Hour,
		KeyPrefix:   "idem",
	}
}

// RawClient is a small escape hatch — the idempotency middleware needs
// SetNX which our rediscache wrapper does not expose. We add a helper
// on rediscache side later; for now, the server wires *redis.Client
// directly via a function accessor injected in NewIdempotency.
//
// NewIdempotency returns a fiber.Handler that enforces the idempotency
// contract described at the top of this file. Must be mounted AFTER
// JWTAuth (so we can audit who issued the key) but BEFORE the actual
// business handler.
func NewIdempotency(cfg IdempotencyConfig) fiber.Handler {
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.ResponseTTL == 0 {
		cfg.ResponseTTL = 1 * time.Hour
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "idem"
	}
	store := cfg.Store

	return func(c *fiber.Ctx) error {
		key := c.Get("Idempotency-Key")
		if key == "" {
			return c.Status(400).JSON(fiber.Map{"error": "missing Idempotency-Key header"})
		}
		// Defend against header injection — the key is embedded in a
		// Redis key and echoed in audit rows. Whitelist a sane charset.
		if !isSafeIdempotencyKey(key) {
			return c.Status(400).JSON(fiber.Map{"error": "invalid Idempotency-Key format"})
		}

		route := c.Route().Path
		if route == "" {
			route = c.Path()
		}
		base := fmt.Sprintf("%s:%s:%s", cfg.KeyPrefix, route, key)
		lockKey := base + ":lock"
		respKey := base + ":response"

		ctx, cancel := context.WithTimeout(c.Context(), 2*time.Second)
		defer cancel()

		// Cache hit fast-path — serve replay without acquiring the lock.
		if store != nil {
			cached, err := store.Get(ctx, respKey)
			if err == nil && cached != "" {
				c.Set("X-Idempotent-Replay", "true")
				c.Set("Content-Type", "application/json")
				return c.Status(200).SendString(cached)
			} else if err != nil && !errors.Is(err, redis.Nil) {
				// Redis down → fail-open would defeat the purpose of
				// idempotency (we cannot verify). 503 is the safest
				// signal; the client will retry after a backoff.
				return c.Status(503).JSON(fiber.Map{"error": "idempotency store unavailable"})
			}
		}

		// Acquire lock. If we lose the race, the first request is still
		// running — bounce the second one.
		if store != nil {
			ok, err := store.SetNX(ctx, lockKey, "1", cfg.LockTTL)
			if err != nil {
				return c.Status(503).JSON(fiber.Map{"error": "idempotency store unavailable"})
			}
			if !ok {
				return c.Status(409).JSON(fiber.Map{
					"error":       "in progress",
					"retry_after": int(cfg.LockTTL.Seconds()),
				})
			}
			// Always release lock at end of request (even on panic).
			defer func() {
				// Use a detached context with short budget so shutdown
				// doesn't leave dangling locks. Ignore error: if we
				// can't DEL, the TTL will reap it.
				rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
				defer rcancel()
				_ = store.Del(rctx, lockKey)
			}()
		}

		// Execute the real handler.
		if err := c.Next(); err != nil {
			return err
		}

		// Cache only successful responses. 4xx/5xx intentionally stay
		// un-cached so the client may retry with the same key.
		if store != nil && c.Response().StatusCode() < 400 {
			body := bytes.Clone(c.Response().Body())
			rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer rcancel()
			_ = store.Set(rctx, respKey, string(body), cfg.ResponseTTL)
		}
		return nil
	}
}

// isSafeIdempotencyKey allows only UUID-ish / opaque tokens. Rejects
// anything with whitespace, control chars, or chars that would change
// Redis key structure.
func isSafeIdempotencyKey(k string) bool {
	if len(k) < 8 || len(k) > 128 {
		return false
	}
	for _, r := range k {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	// Also reject keys that look like they contain our separator by
	// accident. (redundant given the charset above, kept for clarity.)
	return !strings.ContainsAny(k, ": \t\n")
}
