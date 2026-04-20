package middleware

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

// fakeStore is an in-memory implementation of idempotencyStore. The
// middleware uses only four operations so the test double stays tiny.
type fakeStore struct {
	mu   sync.Mutex
	data map[string]string
	// locked keys — simulate SetNX semantics.
	locks map[string]time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		data:  map[string]string{},
		locks: map[string]time.Time{},
	}
}

func (f *fakeStore) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.locks[key]; ok && time.Now().Before(t) {
		return false, nil
	}
	f.locks[key] = time.Now().Add(ttl)
	return true, nil
}
func (f *fakeStore) Get(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.data[key]; ok {
		return v, nil
	}
	// Mimic redis.Nil semantics — return "" with a sentinel error.
	return "", nil
}
func (f *fakeStore) Set(_ context.Context, key, value string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return nil
}
func (f *fakeStore) Del(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.locks, key)
	return nil
}

// TestIdempotencyMissingHeader — request without Idempotency-Key → 400.
func TestIdempotencyMissingHeader(t *testing.T) {
	app := fiber.New()
	app.Post("/x", NewIdempotency(IdempotencyConfig{Store: newFakeStore()}),
		func(c *fiber.Ctx) error { return c.SendString("ok") })

	req := httptest.NewRequest("POST", "/x", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// TestIdempotencyReplay — same key twice → second returns cached body.
func TestIdempotencyReplay(t *testing.T) {
	store := newFakeStore()
	app := fiber.New()

	var calls atomic.Int32
	app.Post("/x", NewIdempotency(IdempotencyConfig{
		Store:       store,
		LockTTL:     2 * time.Second,
		ResponseTTL: time.Minute,
	}), func(c *fiber.Ctx) error {
		calls.Add(1)
		return c.Status(200).JSON(fiber.Map{"ok": true, "n": calls.Load()})
	})

	key := "abcd1234-test-key-0001"
	// First call — executes handler.
	req1 := httptest.NewRequest("POST", "/x", nil)
	req1.Header.Set("Idempotency-Key", key)
	resp1, err := app.Test(req1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.StatusCode != 200 {
		t.Fatalf("first: expected 200, got %d", resp1.StatusCode)
	}
	body1, _ := io.ReadAll(resp1.Body)

	// Second call — must return cached body without running handler.
	req2 := httptest.NewRequest("POST", "/x", nil)
	req2.Header.Set("Idempotency-Key", key)
	resp2, _ := app.Test(req2, -1)
	if resp2.StatusCode != 200 {
		t.Fatalf("second: expected 200 replay, got %d", resp2.StatusCode)
	}
	body2, _ := io.ReadAll(resp2.Body)

	if calls.Load() != 1 {
		t.Fatalf("handler must run exactly once, ran %d times", calls.Load())
	}
	if !strings.EqualFold(string(body1), string(body2)) {
		t.Fatalf("replay body mismatch: %q vs %q", string(body1), string(body2))
	}
	if h := resp2.Header.Get("X-Idempotent-Replay"); h != "true" {
		t.Fatalf("expected X-Idempotent-Replay=true, got %q", h)
	}
}

// TestIdempotencyConflict — concurrent call with the same key gets 409.
func TestIdempotencyConflict(t *testing.T) {
	store := newFakeStore()

	// Pre-lock the key so the middleware sees a busy lock.
	_, _ = store.SetNX(context.Background(),
		"idem:/x:concurrent-key-abcdef01:lock", "1", time.Minute)

	app := fiber.New()
	app.Post("/x", NewIdempotency(IdempotencyConfig{Store: store}),
		func(c *fiber.Ctx) error { return c.SendStatus(200) })

	req := httptest.NewRequest("POST", "/x", nil)
	req.Header.Set("Idempotency-Key", "concurrent-key-abcdef01")
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 409 {
		t.Fatalf("expected 409 for held lock, got %d", resp.StatusCode)
	}
}

// TestIdempotencyFailNotCached — handler returns 500 → cache stays
// empty so the next retry actually executes the handler.
func TestIdempotencyFailNotCached(t *testing.T) {
	store := newFakeStore()
	var calls atomic.Int32
	app := fiber.New()
	app.Post("/x", NewIdempotency(IdempotencyConfig{Store: store}),
		func(c *fiber.Ctx) error {
			calls.Add(1)
			return c.Status(500).SendString("boom")
		})

	key := "fail-key-0123456789"
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("POST", "/x", nil)
		req.Header.Set("Idempotency-Key", key)
		resp, _ := app.Test(req, -1)
		if resp.StatusCode != 500 {
			t.Fatalf("i=%d: expected 500, got %d", i, resp.StatusCode)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("failed response must not be cached; runs=%d", calls.Load())
	}
}

// TestIdempotencyInvalidKey — keys with injection-risk chars rejected.
func TestIdempotencyInvalidKey(t *testing.T) {
	app := fiber.New()
	app.Post("/x", NewIdempotency(IdempotencyConfig{Store: newFakeStore()}),
		func(c *fiber.Ctx) error { return c.SendStatus(200) })

	// Colon would break the key structure.
	req := httptest.NewRequest("POST", "/x", nil)
	req.Header.Set("Idempotency-Key", "not:a:valid:key")
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for unsafe key, got %d", resp.StatusCode)
	}
}
