package middleware

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"cdc-cms-service/config"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// newTestRedis pulls REDIS_URL from env with a sensible docker default.
// Skips the test if Redis isn't reachable.
func newTestRedis(t *testing.T) *rediscache.RedisCache {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:16379"
	}
	logger := zap.NewNop()
	rc, err := rediscache.NewRedisCache(&config.AppConfig{
		Redis: config.RedisConfig{URL: url},
	}, logger)
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	return rc
}

// TestRateLimitRestart — 4th restart within the hour must be 429.
// We set Max=3 and use a fresh per-test scope so reruns don't collide.
func TestRateLimitRestart(t *testing.T) {
	rc := newTestRedis(t)
	defer rc.Close()

	scope := fmt.Sprintf("restart-test-%d", time.Now().UnixNano())
	user := "rl-user-1"

	// Clean up after the test.
	defer rc.Delete(context.Background(),
		fmt.Sprintf("ratelimit:%s:%s", scope, user))

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", user)
		return c.Next()
	})
	app.Post("/restart", NewRateLimit(RateLimitConfig{
		Redis:  rc,
		Scope:  scope,
		Max:    3,
		Window: time.Minute, // short window so TTL fires fast if run repeats
	}), func(c *fiber.Ctx) error {
		return c.SendStatus(200)
	})

	// 3 successes, then 1 rate-limit.
	for i := 1; i <= 3; i++ {
		req := httptest.NewRequest("POST", "/restart", nil)
		resp, _ := app.Test(req, -1)
		if resp.StatusCode != 200 {
			t.Fatalf("call %d: expected 200, got %d", i, resp.StatusCode)
		}
	}
	req := httptest.NewRequest("POST", "/restart", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 429 {
		t.Fatalf("4th call: expected 429, got %d", resp.StatusCode)
	}
	if retry := resp.Header.Get("Retry-After"); retry == "" {
		t.Errorf("Retry-After header missing on 429")
	}
}
