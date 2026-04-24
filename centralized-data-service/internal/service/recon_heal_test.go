package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestReconHealerMasksTopLevelFields(t *testing.T) {
	healer := &ReconHealer{
		masking: NewMaskingService(nil, nil, "phone", "email"),
	}

	raw := healer.buildMaskedRawJSON("customer_profiles", map[string]interface{}{
		"phone": "0901234567",
		"email": "alice@example.com",
		"name":  "Alice",
	})

	payload := decodeMaskedPayload(t, raw)
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

func TestReconHealerMasksNestedObjects(t *testing.T) {
	healer := &ReconHealer{
		masking: NewMaskingService(nil, nil, "balance"),
	}

	raw := healer.buildMaskedRawJSON("wallet_accounts", map[string]interface{}{
		"metadata": map[string]interface{}{
			"user_info": map[string]interface{}{
				"balance": float64(125000),
				"tier":    "gold",
			},
		},
	})

	payload := decodeMaskedPayload(t, raw)
	metadata := payload["metadata"].(map[string]interface{})
	userInfo := metadata["user_info"].(map[string]interface{})
	if userInfo["balance"] != "***" {
		t.Fatalf("nested balance should be masked, got %#v", userInfo["balance"])
	}
	if userInfo["tier"] != "gold" {
		t.Fatalf("non-sensitive nested field changed unexpectedly, got %#v", userInfo["tier"])
	}
}

func TestReconHealerMasksArraysOfObjects(t *testing.T) {
	healer := &ReconHealer{
		masking: NewMaskingService(nil, nil, "phone", "email"),
	}

	raw := healer.buildMaskedRawJSON("crm_contacts", map[string]interface{}{
		"contacts": []interface{}{
			map[string]interface{}{
				"name":         "A",
				"phone_number": "0901",
			},
			map[string]interface{}{
				"name":          "B",
				"email_address": "b@example.com",
			},
		},
	})

	payload := decodeMaskedPayload(t, raw)
	contacts := payload["contacts"].([]interface{})
	first := contacts[0].(map[string]interface{})
	second := contacts[1].(map[string]interface{})
	if first["phone_number"] != "***" {
		t.Fatalf("array phone_number should be masked, got %#v", first["phone_number"])
	}
	if second["email_address"] != "***" {
		t.Fatalf("array email_address should be masked, got %#v", second["email_address"])
	}
	if first["name"] != "A" || second["name"] != "B" {
		t.Fatalf("non-sensitive array fields changed unexpectedly, got %#v / %#v", first["name"], second["name"])
	}
}

func TestReconHealerHeuristicMaskingWithoutRegistry(t *testing.T) {
	healer := &ReconHealer{
		masking: NewMaskingService(nil, nil),
	}

	raw := healer.buildMaskedRawJSON("orders", map[string]interface{}{
		"customer_secret":   "token-123",
		"remaining_balance": 5000,
		"altPhone":          "0902",
		"status":            "active",
	})

	payload := decodeMaskedPayload(t, raw)
	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
}

func TestReconHealerMaskingKeepsOCCSemantics(t *testing.T) {
	healer := &ReconHealer{
		masking: NewMaskingService(nil, nil, "phone"),
	}
	sa := &SchemaAdapter{}
	schema := &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":        {Name: "_id", DataType: "text"},
			"name":       {Name: "name", DataType: "text"},
			"_raw_data":  {Name: "_raw_data", DataType: "jsonb"},
			"_hash":      {Name: "_hash", DataType: "text"},
			"_source_ts": {Name: "_source_ts", DataType: "bigint"},
		},
		PKColumn: "_id",
	}

	data := map[string]interface{}{
		"name":  "Alice",
		"phone": "0901234567",
	}
	rawJSON := healer.buildMaskedRawJSON("customer_profiles", data)
	sourceTsMs := time.Now().UnixMilli()

	query, args := sa.BuildUpsertSQL(
		schema,
		"customer_profiles",
		"_id",
		"id-1",
		map[string]interface{}{"name": data["name"]},
		string(rawJSON),
		"recon-heal",
		"hash-123",
		sourceTsMs,
	)

	if !strings.Contains(query, `"customer_profiles"."_source_ts" <= EXCLUDED."_source_ts"`) {
		t.Fatalf("OCC WHERE clause missing _source_ts guard:\n%s", query)
	}
	if strings.Contains(query, `"_hash" IS DISTINCT FROM`) {
		t.Fatalf("unexpected hash fallback when source ts is present:\n%s", query)
	}

	foundTs := false
	foundMaskedRaw := false
	for _, arg := range args {
		if v, ok := arg.(int64); ok && v == sourceTsMs {
			foundTs = true
		}
		if v, ok := arg.(string); ok && strings.Contains(v, `"phone":"***"`) {
			foundMaskedRaw = true
		}
	}
	if !foundTs {
		t.Fatalf("expected source ts %d to appear in args, got %#v", sourceTsMs, args)
	}
	if !foundMaskedRaw {
		t.Fatalf("expected masked raw JSON to be used in args, got %#v", args)
	}
	if strings.Contains(string(rawJSON), "0901234567") {
		t.Fatalf("raw JSON still contains unmasked phone: %s", string(rawJSON))
	}
}

// TestHealOCCSkipsOlderTs verifies the SQL shape emitted by
// SchemaAdapter includes the _source_ts guard when sourceTsMs > 0.
func TestHealOCCSkipsOlderTs(t *testing.T) {
	sa := &SchemaAdapter{}
	schema := &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":        {Name: "_id", DataType: "text"},
			"name":       {Name: "name", DataType: "text"},
			"_source_ts": {Name: "_source_ts", DataType: "bigint"},
			"_hash":      {Name: "_hash", DataType: "text"},
		},
		PKColumn: "_id",
	}
	olderTs := time.Now().Add(-1 * time.Hour).UnixMilli()

	query, args := sa.BuildUpsertSQL(
		schema, "test_tbl", "_id",
		"some-id",
		map[string]interface{}{"name": "alpha"},
		`{"name":"alpha"}`, "recon-heal", "abc123",
		olderTs,
	)

	if !strings.Contains(query, `"test_tbl"."_source_ts" <= EXCLUDED."_source_ts"`) {
		t.Fatalf("OCC WHERE clause missing _source_ts guard:\n%s", query)
	}
	if !strings.Contains(query, `ON CONFLICT ("_id")`) {
		t.Fatalf("upsert missing ON CONFLICT target: %s", query)
	}

	found := false
	for _, a := range args {
		if v, ok := a.(int64); ok && v == olderTs {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected source ts %d to appear in args, got %v", olderTs, args)
	}
}

// TestHealOCCAppliesNewerTs verifies newer timestamps still use the
// same OCC _source_ts guard rather than the hash fallback branch.
func TestHealOCCAppliesNewerTs(t *testing.T) {
	sa := &SchemaAdapter{}
	schema := &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":        {Name: "_id", DataType: "text"},
			"amount":     {Name: "amount", DataType: "bigint"},
			"_source_ts": {Name: "_source_ts", DataType: "bigint"},
		},
		PKColumn: "_id",
	}
	newerTs := time.Now().UnixMilli()

	query, args := sa.BuildUpsertSQL(
		schema, "test_tbl", "_id",
		"some-id",
		map[string]interface{}{"amount": 100},
		`{"amount":100}`, "recon-heal", "deadbeef",
		newerTs,
	)

	if strings.Contains(query, `"_hash" IS DISTINCT FROM`) {
		t.Errorf("unexpected hash fallback in query:\n%s", query)
	}
	if !strings.Contains(query, `"_source_ts" <= EXCLUDED."_source_ts"`) {
		t.Errorf("OCC guard missing for newer ts upsert:\n%s", query)
	}
	found := false
	for _, a := range args {
		if v, ok := a.(int64); ok && v == newerTs {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected source ts %d to appear in args", newerTs)
	}
}

// TestExtractSourceTsFromDoc covers the three preference tiers:
// updated_at (time.Time), updated_at (primitive.DateTime), ObjectID ts.
func TestExtractSourceTsFromDoc(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	if got := extractSourceTsFromDoc(map[string]interface{}{
		"updated_at": now,
	}); got != now.UnixMilli() {
		t.Errorf("time.Time path: got %d, want %d", got, now.UnixMilli())
	}

	dt := primitive.NewDateTimeFromTime(now)
	if got := extractSourceTsFromDoc(map[string]interface{}{
		"updated_at": dt,
	}); got != now.UnixMilli() {
		t.Errorf("primitive.DateTime path: got %d, want %d", got, now.UnixMilli())
	}

	oid := primitive.NewObjectIDFromTimestamp(now)
	got := extractSourceTsFromDoc(map[string]interface{}{"_id": oid})
	wantSec := now.Unix() * 1000
	if got != wantSec {
		t.Errorf("ObjectID fallback: got %d, want ~%d", got, wantSec)
	}

	if got := extractSourceTsFromDoc(map[string]interface{}{"something": 1}); got != 0 {
		t.Errorf("unknown doc should return 0, got %d", got)
	}
}

// TestSchemaValidatorDriftDetection keeps the pre-existing drift check
// coverage alongside the new ReconHealer masking invariants.
func TestSchemaValidatorDriftDetection(t *testing.T) {
	sv := &SchemaValidator{}

	exp := &tableExpectations{
		Required: map[string]struct{}{"user_id": {}},
		Known:    map[string]struct{}{"user_id": {}, "amount": {}},
	}
	sv.cache.Store("t", exp)

	if err := sv.ValidatePayload("t", map[string]interface{}{
		"user_id": "u1", "amount": 100,
	}); err != nil {
		t.Errorf("expected nil for valid payload, got %v", err)
	}

	if err := sv.ValidatePayload("t", map[string]interface{}{"amount": 100}); err == nil {
		t.Errorf("expected missing-required error")
	}

	err := sv.ValidatePayload("t", map[string]interface{}{
		"user_id": "u1", "amount": 100, "new_field": "x",
	})
	if err == nil {
		t.Errorf("expected schema-drift error")
	} else if !strings.Contains(err.Error(), "unknown_field") {
		t.Errorf("expected unknown_field in error, got: %v", err)
	}
}

func decodeMaskedPayload(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal masked payload: %v", err)
	}
	return payload
}
