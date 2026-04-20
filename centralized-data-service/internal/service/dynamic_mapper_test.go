package service

import (
	"encoding/json"
	"testing"
	"time"
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
