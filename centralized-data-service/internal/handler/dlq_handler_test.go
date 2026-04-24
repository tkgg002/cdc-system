package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"centralized-data-service/internal/service"
)

func TestDLQMessageSerialize(t *testing.T) {
	msg := DLQMessage{
		OriginalSubject: "cdc.goopay.db.table",
		OriginalData:    []byte(`{"key":"value"}`),
		Error:           "connection refused",
		RetryCount:      3,
		FailedAt:        "2026-04-14T10:00:00Z",
		SourceTable:     "payment-bills",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var parsed DLQMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}

	if parsed.OriginalSubject != msg.OriginalSubject {
		t.Errorf("subject mismatch: %s vs %s", parsed.OriginalSubject, msg.OriginalSubject)
	}
	if parsed.RetryCount != 3 {
		t.Errorf("retry count: %d, want 3", parsed.RetryCount)
	}
	if parsed.SourceTable != "payment-bills" {
		t.Errorf("source table: %s, want payment-bills", parsed.SourceTable)
	}
}

func TestDLQHandlerBuildFailedSyncLogMasksTopLevelFields(t *testing.T) {
	handler := &DLQHandler{}
	handler.SetMaskingService(service.NewMaskingService(nil, nil, "phone", "email"))

	row, err := handler.buildFailedSyncLog(
		"cdc.goopay.db.customer_profiles",
		[]byte(`{"_id":"1","phone":"0901234567","email":"alice@example.com","name":"Alice"}`),
		"customer_profiles",
		errors.New("boom"),
	)
	if err != nil {
		t.Fatalf("buildFailedSyncLog returned error: %v", err)
	}

	payload := decodeDLQRawJSON(t, row.RawJSON)
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

func TestDLQHandlerBuildFailedSyncLogMasksNestedAndArrayFields(t *testing.T) {
	handler := &DLQHandler{}
	handler.SetMaskingService(service.NewMaskingService(nil, nil, "balance", "phone"))

	row, err := handler.buildFailedSyncLog(
		"cdc.goopay.db.wallet_accounts",
		[]byte(`{
			"_id":"1",
			"metadata":{"user_info":{"balance":125000,"tier":"gold"}},
			"contacts":[{"phone_number":"0901","label":"home"}]
		}`),
		"wallet_accounts",
		errors.New("boom"),
	)
	if err != nil {
		t.Fatalf("buildFailedSyncLog returned error: %v", err)
	}

	payload := decodeDLQRawJSON(t, row.RawJSON)
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

func TestDLQHandlerHeuristicMaskingWithoutRegistry(t *testing.T) {
	handler := &DLQHandler{}
	handler.SetMaskingService(service.NewMaskingService(nil, nil))

	row, err := handler.buildFailedSyncLog(
		"cdc.goopay.db.orders",
		[]byte(`{"record_id":"r1","customer_secret":"token-123","remaining_balance":5000,"altPhone":"0902","status":"active"}`),
		"orders",
		errors.New("boom"),
	)
	if err != nil {
		t.Fatalf("buildFailedSyncLog returned error: %v", err)
	}

	payload := decodeDLQRawJSON(t, row.RawJSON)
	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
}

func TestDLQHandlerNormalizeRawJSONSanitizesBeforePersistence(t *testing.T) {
	handler := &DLQHandler{}
	handler.SetMaskingService(service.NewMaskingService(nil, nil, "phone"))

	raw := handler.normalizeDLQRawJSON("customer_profiles", []byte(`{"phone":"0901234567","name":"Alice"}`))
	if strings.Contains(string(raw), "0901234567") {
		t.Fatalf("normalized raw JSON still contains unmasked phone: %s", string(raw))
	}
	if !strings.Contains(string(raw), `"phone":"***"`) {
		t.Fatalf("normalized raw JSON missing masked phone: %s", string(raw))
	}

	row, err := handler.buildFailedSyncLog(
		"cdc.goopay.db.customer_profiles",
		[]byte(raw),
		"customer_profiles",
		errors.New("boom"),
	)
	if err != nil {
		t.Fatalf("buildFailedSyncLog returned error: %v", err)
	}
	if strings.Contains(string(row.RawJSON), "0901234567") {
		t.Fatalf("RawJSON persisted to failed_sync_logs still contains unmasked phone: %s", string(row.RawJSON))
	}
}

func TestDLQHandlerSanitizeDLQErrorRedactsSensitiveText(t *testing.T) {
	got := sanitizeDLQError("boom\nemail=alice@example.com phone=0901234567 secret=token-123")
	for _, needle := range []string{"alice@example.com", "0901234567", "token-123"} {
		if strings.Contains(got, needle) {
			t.Fatalf("sanitized dlq error still contains %q: %q", needle, got)
		}
	}
	for _, expected := range []string{"email=***", "phone=***", "secret=***"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("sanitized dlq error missing redaction %q: %q", expected, got)
		}
	}
}

func TestHandleWithRetrySuccessFirstTry(t *testing.T) {
	handler := &DLQHandler{}

	attempts := 0
	ok := handler.HandleWithRetryContext(context.Background(), "cdc.goopay.db.table", nil, "table", func(context.Context) error {
		attempts++
		return nil
	})

	if !ok {
		t.Fatal("expected retry handler to succeed")
	}
	if attempts != 1 {
		t.Fatalf("attempts: %d, want 1", attempts)
	}
}

func TestHandleWithRetryFailAllRetries(t *testing.T) {
	handler := &DLQHandler{}

	attempts := 0
	ok := handler.HandleWithRetryContext(context.Background(), "cdc.goopay.db.table", nil, "table", func(context.Context) error {
		attempts++
		return errors.New("always fail")
	})

	if ok {
		t.Fatal("expected retry handler to fail after retries")
	}
	if attempts != MaxRetries {
		t.Fatalf("attempts: %d, want %d", attempts, MaxRetries)
	}
}

func decodeDLQRawJSON(t *testing.T, raw json.RawMessage) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal dlq raw JSON: %v", err)
	}
	return payload
}
