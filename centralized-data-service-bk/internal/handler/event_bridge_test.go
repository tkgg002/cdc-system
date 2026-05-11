package handler

import (
	"reflect"
	"testing"
)

func TestMinimizeBridgePayloadDropsRawDocumentButKeepsMetadata(t *testing.T) {
	got := minimizeBridgePayload("customer_profiles", map[string]interface{}{
		"table":  "customer_profiles",
		"count":  float64(1),
		"op":     "u",
		"source": "postgres",
		"after": map[string]interface{}{
			"_id":   "abc123",
			"phone": "0901234567",
			"email": "alice@example.com",
			"name":  "Alice",
		},
	})

	if got["record_id"] != "abc123" {
		t.Fatalf("record_id mismatch: %#v", got["record_id"])
	}
	if _, ok := got["after"]; ok {
		t.Fatalf("raw after payload must not be forwarded: %#v", got)
	}
	if _, ok := got["phone"]; ok {
		t.Fatalf("top-level raw fields must not be forwarded: %#v", got)
	}

	expectedFields := []string{"_id", "email", "name", "phone"}
	if !reflect.DeepEqual(got["changed_fields"], expectedFields) {
		t.Fatalf("changed_fields mismatch: got %#v want %#v", got["changed_fields"], expectedFields)
	}
}

func TestMinimizeBridgePayloadFallsBackToChannelTableOnly(t *testing.T) {
	got := minimizeBridgePayload("cdc_change", map[string]interface{}{})
	if len(got) != 1 || got["table"] != "cdc_change" {
		t.Fatalf("empty payload should collapse to table only, got %#v", got)
	}
}

func TestMinimizeBridgePayloadUsesTopLevelRecordIDAndSortedFields(t *testing.T) {
	got := minimizeBridgePayload("wallet_accounts", map[string]interface{}{
		"record_id": "r1",
		"balance":   5000,
		"status":    "active",
	})

	if got["record_id"] != "r1" {
		t.Fatalf("record_id mismatch: %#v", got["record_id"])
	}
	expectedFields := []string{"balance", "status"}
	if !reflect.DeepEqual(got["changed_fields"], expectedFields) {
		t.Fatalf("changed_fields mismatch: got %#v want %#v", got["changed_fields"], expectedFields)
	}
	if _, ok := got["balance"]; ok {
		t.Fatalf("top-level raw values must not survive minimization: %#v", got)
	}
}
