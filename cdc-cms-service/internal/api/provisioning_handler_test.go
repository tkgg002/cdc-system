package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/service"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

// stubError lets us shove arbitrary errors through service.Provisioning*
// using errors.Is semantics — we wrap the canonical sentinels.
func newApp(t *testing.T) *fiber.App {
	t.Helper()
	app := fiber.New()
	// Bypass JWTAuth: pretend the user has ops-admin role so RequireOpsAdmin passes.
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "test-admin")
		c.Locals("role", "ops-admin")
		return c.Next()
	})
	return app
}

// TestErrMapping_404 — ErrProvisioningSourceNotFound returns 404.
func TestErrMapping_404(t *testing.T) {
	app := newApp(t)
	app.Use(middleware.RequireOpsAdmin())
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Get("/x/:id", func(c *fiber.Ctx) error {
		return h.mapErr(c, 99, service.ErrProvisioningSourceNotFound)
	})
	req := httptest.NewRequest("GET", "/x/99", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", resp.StatusCode)
	}
}

// TestErrMapping_422 — ErrProvisioningInvalidTransition returns 422.
func TestErrMapping_422(t *testing.T) {
	app := newApp(t)
	app.Use(middleware.RequireOpsAdmin())
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Post("/x/:id/advance", func(c *fiber.Ctx) error {
		return h.mapErr(c, 5, service.ErrProvisioningInvalidTransition)
	})
	req := httptest.NewRequest("POST", "/x/5/advance", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d, want 422", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if got["error"] != "invalid transition" {
		t.Fatalf("body.error=%v, want 'invalid transition'", got["error"])
	}
}

// TestErrMapping_409 — ErrProvisioningConflict returns 409.
func TestErrMapping_409(t *testing.T) {
	app := newApp(t)
	app.Use(middleware.RequireOpsAdmin())
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Post("/x/:id/pause", func(c *fiber.Ctx) error {
		return h.mapErr(c, 7, service.ErrProvisioningConflict)
	})
	req := httptest.NewRequest("POST", "/x/7/pause", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d, want 409", resp.StatusCode)
	}
}

// TestParseSourceID_Bad — bad path id → 400.
func TestParseSourceID_Bad(t *testing.T) {
	app := newApp(t)
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Get("/x/:id", func(c *fiber.Ctx) error {
		_, err := h.parseSourceID(c)
		if err != nil {
			return nil
		}
		return c.SendString("ok")
	})
	req := httptest.NewRequest("GET", "/x/abc", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

// TestSetMode_BadBody — invalid JSON body → 400.
func TestSetMode_BadBody(t *testing.T) {
	app := newApp(t)
	app.Use(middleware.RequireOpsAdmin())
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Post("/x/:id/mode", h.SetMode)
	req := httptest.NewRequest("POST", "/x/1/mode", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

// TestSetMode_BadModeValue — body parses but mode != auto|manual → 400.
func TestSetMode_BadModeValue(t *testing.T) {
	app := newApp(t)
	app.Use(middleware.RequireOpsAdmin())
	h := &ProvisioningHandler{logger: zap.NewNop()}
	app.Post("/x/:id/mode", h.SetMode)
	req := httptest.NewRequest("POST", "/x/1/mode", strings.NewReader(`{"mode":"foo"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 for invalid mode value", resp.StatusCode)
	}
}

// TestRequireOpsAdmin_Forbids — viewer role -> 403 before reaching handler.
func TestRequireOpsAdmin_Forbids(t *testing.T) {
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("username", "viewer1")
		c.Locals("role", "viewer")
		return c.Next()
	})
	app.Get("/x/:id/provisioning",
		middleware.RequireOpsAdmin(),
		func(c *fiber.Ctx) error { return c.SendString("should not reach") })
	req := httptest.NewRequest("GET", "/x/1/provisioning", nil)
	resp, _ := app.Test(req, -1)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d, want 403 for viewer", resp.StatusCode)
	}
}
