package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestBatchUpdate_EmptyIDs(t *testing.T) {
	app := fiber.New()
	// Handler without DB — will fail on repo call but should validate input first
	handler := &MappingRuleHandler{}
	app.Patch("/api/mapping-rules/batch", handler.BatchUpdate)

	body := `{"ids":[],"status":"approved"}`
	req := httptest.NewRequest("PATCH", "/api/mapping-rules/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestBatchUpdate_MissingStatus(t *testing.T) {
	app := fiber.New()
	handler := &MappingRuleHandler{}
	app.Patch("/api/mapping-rules/batch", handler.BatchUpdate)

	body := `{"ids":[1,2,3],"status":""}`
	req := httptest.NewRequest("PATCH", "/api/mapping-rules/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestBatchUpdate_InvalidJSON(t *testing.T) {
	app := fiber.New()
	handler := &MappingRuleHandler{}
	app.Patch("/api/mapping-rules/batch", handler.BatchUpdate)

	req := httptest.NewRequest("PATCH", "/api/mapping-rules/batch", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestUpdateStatus_MissingStatus(t *testing.T) {
	app := fiber.New()
	handler := &MappingRuleHandler{}
	app.Patch("/api/mapping-rules/:id", handler.UpdateStatus)

	body := `{"status":""}`
	req := httptest.NewRequest("PATCH", "/api/mapping-rules/1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	if result["error"] != "status is required" {
		t.Errorf("expected error 'status is required', got %v", result["error"])
	}
}
