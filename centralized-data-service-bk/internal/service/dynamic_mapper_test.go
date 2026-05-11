package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"centralized-data-service/internal/model"
)

func TestConvertType_Int(t *testing.T) {
	tests := []struct {
		input    interface{}
		dataType string
		expected int64
	}{
		{float64(42), "BIGINT", 42},
		{float64(0), "INTEGER", 0},
		{"123", "INT", 123},
		{json.Number("999"), "BIGINT", 999},
	}
	for _, tt := range tests {
		result, err := convertType(tt.input, tt.dataType)
		if err != nil {
			t.Errorf("convertType(%v, %s) error: %v", tt.input, tt.dataType, err)
			continue
		}
		if result.(int64) != tt.expected {
			t.Errorf("convertType(%v, %s) = %v, want %v", tt.input, tt.dataType, result, tt.expected)
		}
	}
}

func TestConvertType_Float(t *testing.T) {
	result, err := convertType(float64(3.14), "NUMERIC")
	if err != nil {
		t.Fatal(err)
	}
	if result.(float64) != 3.14 {
		t.Errorf("got %v, want 3.14", result)
	}

	result, err = convertType("99.5", "DECIMAL")
	if err != nil {
		t.Fatal(err)
	}
	if result.(float64) != 99.5 {
		t.Errorf("got %v, want 99.5", result)
	}
}

func TestConvertType_Bool(t *testing.T) {
	result, _ := convertType(true, "BOOLEAN")
	if result.(bool) != true {
		t.Error("expected true")
	}

	result, _ = convertType("false", "BOOL")
	if result.(bool) != false {
		t.Error("expected false")
	}
}

func TestConvertType_Timestamp(t *testing.T) {
	result, err := convertType("2026-04-14T10:00:00Z", "TIMESTAMP")
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := result.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", result)
	}
	if ts.Year() != 2026 || ts.Month() != 4 || ts.Day() != 14 {
		t.Errorf("unexpected date: %v", ts)
	}
}

func TestConvertType_MongoDate(t *testing.T) {
	mongoDate := map[string]interface{}{"$date": "2026-01-15T08:30:00Z"}
	result, err := convertType(mongoDate, "TIMESTAMP")
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := result.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", result)
	}
	if ts.Month() != 1 || ts.Day() != 15 {
		t.Errorf("unexpected date: %v", ts)
	}
}

func TestConvertType_Text(t *testing.T) {
	result, _ := convertType(42, "TEXT")
	if result.(string) != "42" {
		t.Errorf("got %v, want '42'", result)
	}
}

func TestConvertType_Nil(t *testing.T) {
	result, err := convertType(nil, "TEXT")
	if err != nil || result != nil {
		t.Errorf("nil input should return nil, got %v, err %v", result, err)
	}
}

func TestGetNestedField(t *testing.T) {
	data := map[string]interface{}{
		"info": map[string]interface{}{
			"fee":  50000,
			"bank": map[string]interface{}{"name": "BIDV"},
		},
		"top": "value",
	}

	if v := getNestedField(data, "info.fee"); v != 50000 {
		// int literal in Go map = int, not float64
		t.Errorf("info.fee = %v (%T), want 50000", v, v)
	}
	if v := getNestedField(data, "info.bank.name"); v != "BIDV" {
		t.Errorf("info.bank.name = %v, want BIDV", v)
	}
	if v := getNestedField(data, "top"); v != "value" {
		t.Errorf("top = %v, want value", v)
	}
	if v := getNestedField(data, "missing.field"); v != nil {
		t.Errorf("missing.field = %v, want nil", v)
	}
}

func TestDynamicMapperMasksRawJSONWithoutRules(t *testing.T) {
	registry := &RegistryService{mappingCache: map[string][]model.MappingRule{}}
	mapper := NewDynamicMapper(registry, nil)
	mapper.SetMaskingService(NewMaskingService(nil, nil, "phone", "email"))

	mapped, err := mapper.MapData(context.Background(), "customer_profiles", map[string]interface{}{
		"phone": "0901234567",
		"email": "alice@example.com",
		"name":  "Alice",
	})
	if err != nil {
		t.Fatalf("MapData returned error: %v", err)
	}

	payload := decodeDynamicMapperRawJSON(t, mapped.RawJSON)
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

func TestDynamicMapperMasksNestedAndArrayFields(t *testing.T) {
	registry := &RegistryService{mappingCache: map[string][]model.MappingRule{
		"customer_profiles": {
			{
				SourceField:  "profile.name",
				TargetColumn: "full_name",
				DataType:     "TEXT",
				IsActive:     true,
			},
		},
	}}
	mapper := NewDynamicMapper(registry, nil)
	mapper.SetMaskingService(NewMaskingService(nil, nil, "phone", "balance"))

	mapped, err := mapper.MapData(context.Background(), "customer_profiles", map[string]interface{}{
		"profile": map[string]interface{}{
			"name": "Alice",
		},
		"metadata": map[string]interface{}{
			"user_info": map[string]interface{}{
				"balance": float64(125000),
			},
		},
		"contacts": []interface{}{
			map[string]interface{}{"phone_number": "0901", "label": "home"},
		},
	})
	if err != nil {
		t.Fatalf("MapData returned error: %v", err)
	}
	if mapped.Columns["full_name"] != "Alice" {
		t.Fatalf("expected mapped column full_name=Alice, got %#v", mapped.Columns["full_name"])
	}

	payload := decodeDynamicMapperRawJSON(t, mapped.RawJSON)
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

func TestDynamicMapperHeuristicMaskingWithoutRegistry(t *testing.T) {
	registry := &RegistryService{mappingCache: map[string][]model.MappingRule{}}
	mapper := NewDynamicMapper(registry, nil)
	mapper.SetMaskingService(NewMaskingService(nil, nil))

	mapped, err := mapper.MapData(context.Background(), "orders", map[string]interface{}{
		"customer_secret":   "token-123",
		"remaining_balance": 5000,
		"altPhone":          "0902",
		"status":            "active",
	})
	if err != nil {
		t.Fatalf("MapData returned error: %v", err)
	}

	payload := decodeDynamicMapperRawJSON(t, mapped.RawJSON)
	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
}

func TestDynamicMapperBuildUpsertQueryUsesMaskedRawJSON(t *testing.T) {
	registry := &RegistryService{mappingCache: map[string][]model.MappingRule{}}
	adapter := &SchemaAdapter{}
	adapter.cache.Store("customer_profiles", &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":       {Name: "_id", DataType: "text"},
			"full_name": {Name: "full_name", DataType: "text"},
			"_raw_data": {Name: "_raw_data", DataType: "jsonb"},
		},
		PKColumn: "_id",
	})

	mapper := NewDynamicMapper(registry, nil, adapter)
	mapper.SetMaskingService(NewMaskingService(nil, nil, "phone"))

	mapped, err := mapper.MapData(context.Background(), "customer_profiles", map[string]interface{}{
		"phone": "0901234567",
		"name":  "Alice",
	})
	if err != nil {
		t.Fatalf("MapData returned error: %v", err)
	}
	mapped.Columns["full_name"] = "Alice"

	query, args := mapper.BuildUpsertQuery("customer_profiles", "_id", mapped)
	if !strings.Contains(query, `"_raw_data"`) {
		t.Fatalf("expected upsert query to include _raw_data, got %s", query)
	}

	foundMaskedRaw := false
	for _, arg := range args {
		if v, ok := arg.(string); ok && strings.Contains(v, `"phone":"***"`) {
			foundMaskedRaw = true
		}
	}
	if !foundMaskedRaw {
		t.Fatalf("expected masked _raw_data in upsert args, got %#v", args)
	}
	if strings.Contains(string(mapped.RawJSON), "0901234567") {
		t.Fatalf("raw JSON still contains unmasked phone: %s", string(mapped.RawJSON))
	}
}

func decodeDynamicMapperRawJSON(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal dynamic mapper raw JSON: %v", err)
	}
	return payload
}
