package utils

import (
	"strings"
	"testing"
)

func TestInferDataType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"nil", nil, "TEXT"},
		{"bool_true", true, "BOOLEAN"},
		{"bool_false", false, "BOOLEAN"},
		{"integer_small", float64(42), "INTEGER"},
		{"integer_zero", float64(0), "INTEGER"},
		{"integer_negative", float64(-100), "INTEGER"},
		{"bigint", float64(9999999999), "BIGINT"},
		{"bigint_negative", float64(-9999999999), "BIGINT"},
		{"decimal", float64(3.14), "DECIMAL(18,6)"},
		{"decimal_small", float64(0.001), "DECIMAL(18,6)"},
		{"timestamp_rfc3339", "2026-03-30T10:30:00Z", "TIMESTAMP"},
		{"timestamp_offset", "2026-03-30T10:30:00+07:00", "TIMESTAMP"},
		{"timestamp_space", "2026-03-30 10:30:00", "TIMESTAMP"},
		{"varchar_short", "hello", "VARCHAR(100)"},
		{"varchar_medium", strings.Repeat("x", 150), "VARCHAR(255)"},
		{"text_long", strings.Repeat("x", 300), "TEXT"},
		{"jsonb_map", map[string]interface{}{"key": "val"}, "JSONB"},
		{"jsonb_array", []interface{}{1, 2, 3}, "JSONB"},
		{"jsonb_nested", map[string]interface{}{"a": map[string]interface{}{"b": 1}}, "JSONB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferDataType(tt.value)
			if result != tt.expected {
				t.Errorf("InferDataType(%v) = %q, want %q", tt.value, result, tt.expected)
			}
		})
	}
}
