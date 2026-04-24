package handler

import (
	"encoding/json"
	"testing"

	"centralized-data-service/internal/service"
)

func TestReconHandlerSanitizeRetryRawJSONMasksTopLevelFields(t *testing.T) {
	h := &ReconHandler{}
	h.WithMaskingService(service.NewMaskingService(nil, nil, "phone", "email"))

	raw := h.sanitizeRetryRawJSON("customer_profiles", `{"phone":"0901234567","email":"alice@example.com","name":"Alice"}`)
	if raw == "" {
		t.Fatal("sanitized raw JSON should not be empty")
	}

	payload := decodeReconRetryRawJSON(t, raw)
	if payload["phone"] != "***" {
		t.Fatalf("phone should be masked, got %#v", payload["phone"])
	}
	if payload["email"] != "***" {
		t.Fatalf("email should be masked, got %#v", payload["email"])
	}
	if payload["name"] != "Alice" {
		t.Fatalf("non-sensitive field changed unexpectedly, got %#v", payload["name"])
	}
}

func TestReconHandlerSanitizeRetryRawJSONMasksNestedAndArrayFields(t *testing.T) {
	h := &ReconHandler{}
	h.WithMaskingService(service.NewMaskingService(nil, nil, "balance", "phone"))

	raw := h.sanitizeRetryRawJSON("wallet_accounts", `{"metadata":{"user_info":{"balance":125000,"tier":"gold"}},"contacts":[{"phone_number":"0901","label":"home"}]}`)
	payload := decodeReconRetryRawJSON(t, raw)

	metadata := payload["metadata"].(map[string]interface{})
	userInfo := metadata["user_info"].(map[string]interface{})
	if userInfo["balance"] != "***" {
		t.Fatalf("nested balance should be masked, got %#v", userInfo["balance"])
	}
	contacts := payload["contacts"].([]interface{})
	first := contacts[0].(map[string]interface{})
	if first["phone_number"] != "***" {
		t.Fatalf("array phone_number should be masked, got %#v", first["phone_number"])
	}
	if first["label"] != "home" {
		t.Fatalf("non-sensitive array field changed unexpectedly, got %#v", first["label"])
	}
}

func TestReconHandlerSanitizeRetryRawJSONHeuristicMaskingWithoutRegistry(t *testing.T) {
	h := &ReconHandler{}
	h.WithMaskingService(service.NewMaskingService(nil, nil))

	raw := h.sanitizeRetryRawJSON("orders", `{"customer_secret":"token-123","remaining_balance":5000,"altPhone":"0902","status":"active"}`)
	payload := decodeReconRetryRawJSON(t, raw)

	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
}

func decodeReconRetryRawJSON(t *testing.T, raw string) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal recon retry raw JSON: %v", err)
	}
	return payload
}
