package service

import (
	"testing"

	"centralized-data-service/internal/model"
)

func TestShouldUseAirbyte(t *testing.T) {
	cases := []struct {
		name   string
		engine string
		want   bool
	}{
		{"airbyte", "airbyte", true},
		{"both", "both", true},
		{"debezium", "debezium", false},
		{"empty", "", false},
		{"uppercase airbyte", "AIRBYTE", true},
		{"padded both", "  both ", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &model.TableRegistry{SyncEngine: tc.engine}
			if got := ShouldUseAirbyte(e); got != tc.want {
				t.Fatalf("ShouldUseAirbyte(%q) = %v, want %v", tc.engine, got, tc.want)
			}
		})
	}
	if ShouldUseAirbyte(nil) {
		t.Fatalf("nil entry should not route to Airbyte")
	}
}

func TestShouldUseDebezium(t *testing.T) {
	cases := []struct {
		engine string
		want   bool
	}{
		{"debezium", true},
		{"both", true},
		{"airbyte", false},
		{"", false},
	}
	for _, tc := range cases {
		e := &model.TableRegistry{SyncEngine: tc.engine}
		if got := ShouldUseDebezium(e); got != tc.want {
			t.Fatalf("ShouldUseDebezium(%q) = %v, want %v", tc.engine, got, tc.want)
		}
	}
	if ShouldUseDebezium(nil) {
		t.Fatalf("nil entry should not route to Debezium")
	}
}

func TestInferTypeFromRawData(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"nil", nil, "TEXT"},
		{"bool", true, "BOOLEAN"},
		{"int-as-float", float64(42), "BIGINT"},
		{"float", float64(3.14), "NUMERIC"},
		{"rfc3339", "2026-04-17T12:34:56Z", "TIMESTAMP"},
		{"plain string", "hello", "TEXT"},
		{"object", map[string]interface{}{"x": 1}, "JSONB"},
		{"array", []interface{}{1, 2, 3}, "JSONB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := InferTypeFromRawData(tc.in); got != tc.want {
				t.Fatalf("InferTypeFromRawData(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
