package middleware

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// TestRBACForbids403 — when JWTAuth has set a viewer role, the
// destructive chain must reject with 403 before the handler runs.
func TestRBACForbids403(t *testing.T) {
	app := fiber.New()

	// Simulate JWTAuth putting "viewer" into locals.
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "alice")
		c.Locals("role", "viewer")
		return c.Next()
	})
	app.Post("/api/destructive", RequireOpsAdmin(), func(c *fiber.Ctx) error {
		return c.SendString("reached handler")
	})

	req := httptest.NewRequest("POST", "/api/destructive", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 403 {
		t.Fatalf("expected 403 for viewer, got %d", resp.StatusCode)
	}
}

// TestRBACAllowsOpsAdmin — ops-admin role passes.
func TestRBACAllowsOpsAdmin(t *testing.T) {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "bob")
		c.Locals("role", "ops-admin")
		return c.Next()
	})
	app.Post("/api/destructive", RequireOpsAdmin(), func(c *fiber.Ctx) error {
		return c.SendStatus(200)
	})

	req := httptest.NewRequest("POST", "/api/destructive", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 for ops-admin, got %d", resp.StatusCode)
	}
}

// TestRBACAdminBackCompat — legacy "admin" role keeps working during
// the IdP migration window (see comment in rbac.go).
func TestRBACAdminBackCompat(t *testing.T) {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "carol")
		c.Locals("role", "admin")
		return c.Next()
	})
	app.Post("/api/destructive", RequireOpsAdmin(), func(c *fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest("POST", "/api/destructive", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 200 {
		t.Fatalf("legacy admin must pass, got %d", resp.StatusCode)
	}
}

// TestRBACAdminUsersFallback — ADMIN_USERS env lets named users bypass
// without a role claim.
func TestRBACAdminUsersFallback(t *testing.T) {
	t.Setenv("ADMIN_USERS", "ops1, ops2")
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "ops1")
		c.Locals("role", "viewer") // would normally be blocked
		return c.Next()
	})
	app.Post("/api/destructive", RequireOpsAdmin(), func(c *fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest("POST", "/api/destructive", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 200 {
		t.Fatalf("ADMIN_USERS member must pass, got %d", resp.StatusCode)
	}
}

// TestRBACNoAuth — when upstream JWT middleware didn't set locals at
// all (e.g. the user hit the route without a token and something
// mis-routed it), we return 401 not 403 so monitoring can distinguish.
func TestRBACNoAuth(t *testing.T) {
	app := fiber.New()
	app.Post("/api/destructive", RequireOpsAdmin(), func(c *fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest("POST", "/api/destructive", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401 for no-auth, got %d", resp.StatusCode)
	}
}
