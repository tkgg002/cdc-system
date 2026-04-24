package handler

import (
	"encoding/json"
	"errors"
	"testing"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
)

func TestBatchBufferBuildFailedSyncLogMasksTopLevelFields(t *testing.T) {
	bb := &BatchBuffer{}
	bb.SetMaskingService(service.NewMaskingService(nil, nil, "phone", "email"))

	row := bb.buildFailedSyncLog("customer_profiles", &model.UpsertRecord{
		PrimaryKeyValue: "1",
		RawData:         `{"phone":"0901234567","email":"alice@example.com","name":"Alice"}`,
	}, errors.New("boom"))

	payload := decodeBatchBufferRawJSON(t, row.RawJSON)
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

func TestBatchBufferBuildFailedSyncLogMasksNestedAndArrayFields(t *testing.T) {
	bb := &BatchBuffer{}
	bb.SetMaskingService(service.NewMaskingService(nil, nil, "balance", "phone"))

	row := bb.buildFailedSyncLog("wallet_accounts", &model.UpsertRecord{
		PrimaryKeyValue: "1",
		RawData:         `{"metadata":{"user_info":{"balance":125000,"tier":"gold"}},"contacts":[{"phone_number":"0901","label":"home"}]}`,
	}, errors.New("boom"))

	payload := decodeBatchBufferRawJSON(t, row.RawJSON)
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

func TestBatchBufferBuildFailedSyncLogHeuristicMaskingWithoutRegistry(t *testing.T) {
	bb := &BatchBuffer{}
	bb.SetMaskingService(service.NewMaskingService(nil, nil))

	row := bb.buildFailedSyncLog("orders", &model.UpsertRecord{
		PrimaryKeyValue: "r1",
		RawData:         `{"customer_secret":"token-123","remaining_balance":5000,"altPhone":"0902","status":"active"}`,
	}, errors.New("boom"))

	payload := decodeBatchBufferRawJSON(t, row.RawJSON)
	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
}

func decodeBatchBufferRawJSON(t *testing.T, raw json.RawMessage) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal batch buffer raw JSON: %v", err)
	}
	return payload
}
