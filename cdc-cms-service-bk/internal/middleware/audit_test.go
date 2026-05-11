package middleware

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// dsnFromEnv falls back to the docker-local default used by the rest
// of this repo. Tests that need the DB will skip if it's unreachable.
func dsnFromEnv() string {
	if d := os.Getenv("TEST_PG_DSN"); d != "" {
		return d
	}
	return "host=localhost port=5432 user=user password=password dbname=goopay_dw sslmode=disable"
}

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(postgres.Open(dsnFromEnv()), &gorm.Config{})
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	return db
}

// TestAuditLogInsert — wrap a handler with the audit middleware, run
// one request, and verify exactly one row was written to admin_actions.
func TestAuditLogInsert(t *testing.T) {
	db := openTestDB(t)

	// Ensure partitioned table exists (migration 005). Skip if not.
	var exists bool
	if err := db.Raw(
		"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'admin_actions')",
	).Scan(&exists).Error; err != nil || !exists {
		t.Skipf("admin_actions table missing (run migration 005): exists=%v err=%v", exists, err)
	}

	// Unique marker so we can find OUR row even if the DB is shared.
	marker := fmt.Sprintf("unit-test-%d", time.Now().UnixNano())

	logger := zap.NewNop()
	al := NewAuditLogger(db, logger, map[string]string{
		"/test/audit": "unit_test_action",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go al.Run(ctx)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", marker) // masquerade as user=marker
		return c.Next()
	})
	app.Post("/test/audit", al.Middleware(), func(c *fiber.Ctx) error {
		return c.Status(200).JSON(fiber.Map{"ok": true})
	})

	body := `{"reason":"unit test needs audit row to appear"}`
	req := httptest.NewRequest("POST", "/test/audit",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "audit-key-test-0001")
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("handler status = %d", resp.StatusCode)
	}

	// Give the async writer a moment to flush.
	deadline := time.Now().Add(5 * time.Second)
	var count int64
	for time.Now().Before(deadline) {
		db.Raw(
			"SELECT count(*) FROM admin_actions WHERE user_id = ?", marker,
		).Scan(&count)
		if count > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 audit row for user=%s, got %d", marker, count)
	}

	// Verify fields match what we set.
	type row struct {
		Action         string
		Reason         string
		Result         string
		IdempotencyKey string `gorm:"column:idempotency_key"`
	}
	var r row
	db.Raw(
		"SELECT action, reason, result, idempotency_key FROM admin_actions WHERE user_id = ? LIMIT 1",
		marker,
	).Scan(&r)
	if r.Action != "unit_test_action" {
		t.Errorf("action = %q, want unit_test_action", r.Action)
	}
	if r.Result != "success" {
		t.Errorf("result = %q, want success", r.Result)
	}
	if r.IdempotencyKey != "audit-key-test-0001" {
		t.Errorf("idempotency_key = %q", r.IdempotencyKey)
	}
	if !strings.Contains(r.Reason, "unit test") {
		t.Errorf("reason = %q", r.Reason)
	}

	// Cleanup our row.
	db.Exec("DELETE FROM admin_actions WHERE user_id = ?", marker)
}

// TestAuditRejectsShortReason — reason < 10 chars → 400, no DB row.
func TestAuditRejectsShortReason(t *testing.T) {
	db := openTestDB(t)
	logger := zap.NewNop()
	al := NewAuditLogger(db, logger, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go al.Run(ctx)

	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "short-reason-test")
		return c.Next()
	})
	app.Post("/x", al.Middleware(), func(c *fiber.Ctx) error {
		t.Fatal("handler must not be reached when reason is too short")
		return nil
	})

	req := httptest.NewRequest("POST", "/x", strings.NewReader(`{"reason":"short"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for short reason, got %d", resp.StatusCode)
	}
}
