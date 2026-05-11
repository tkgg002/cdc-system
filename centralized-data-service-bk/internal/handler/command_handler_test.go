package handler

import (
	"strings"
	"testing"
)

func TestSanitizeAdminErrorStripsNewlinesAndTruncates(t *testing.T) {
	input := "boom:\nsecret=token-123 email=alice@example.com phone=0901234567\r\n" + strings.Repeat("x", 260)
	got := sanitizeAdminError(input)

	if strings.Contains(got, "\n") || strings.Contains(got, "\r") {
		t.Fatalf("sanitized error still contains line breaks: %q", got)
	}
	if !strings.HasPrefix(got, "boom: secret=*** email=*** phone=***") {
		t.Fatalf("sanitized error lost visible context: %q", got)
	}
	for _, needle := range []string{"token-123", "alice@example.com", "0901234567"} {
		if strings.Contains(got, needle) {
			t.Fatalf("sanitized error still contains sensitive value %q: %q", needle, got)
		}
	}
	if len(got) > 243 {
		t.Fatalf("sanitized error should be truncated, got len=%d", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("sanitized error should end with ellipsis after truncation: %q", got)
	}
}

func TestSanitizeAdminResultMapKeepsAllowlistOnly(t *testing.T) {
	got := sanitizeAdminResultMap(map[string]interface{}{
		"command":      "alter-column",
		"target_table": "customer_profiles",
		"column_name":  "phone",
		"status":       "success",
		"sql":          `ALTER TABLE "customer_profiles" DROP COLUMN "phone"`,
		"payload": map[string]interface{}{
			"phone": "0901234567",
		},
	})

	if _, ok := got["sql"]; ok {
		t.Fatalf("sql should be dropped from admin result: %#v", got)
	}
	if _, ok := got["payload"]; ok {
		t.Fatalf("payload should be dropped from admin result: %#v", got)
	}
	if got["command"] != "alter-column" || got["column_name"] != "phone" {
		t.Fatalf("allowlisted fields missing from sanitized map: %#v", got)
	}
}

func TestSanitizeAdminResultMapSanitizesErrorFieldsAndSortsNewFields(t *testing.T) {
	got := sanitizeAdminResultMap(map[string]interface{}{
		"status":         "error",
		"error":          "db exploded\nphone=0901234567 email=alice@example.com",
		"reason":         " failed \r\n because secret=token ",
		"debezium_error": "connector down\ntrace line 1\napi_key=abc123",
		"new_fields":     []interface{}{"zeta", "alpha", "beta"},
	})

	if got["error"] != "db exploded phone=*** email=***" {
		t.Fatalf("error not sanitized as expected: %#v", got["error"])
	}
	if got["reason"] != "failed because secret=***" {
		t.Fatalf("reason not sanitized as expected: %#v", got["reason"])
	}
	if got["debezium_error"] != "connector down trace line 1 api_key=***" {
		t.Fatalf("debezium_error not sanitized as expected: %#v", got["debezium_error"])
	}

	fields, ok := got["new_fields"].([]string)
	if !ok {
		t.Fatalf("new_fields should be []string, got %#v", got["new_fields"])
	}
	expected := []string{"alpha", "beta", "zeta"}
	if strings.Join(fields, ",") != strings.Join(expected, ",") {
		t.Fatalf("new_fields not sorted: got %v want %v", fields, expected)
	}
}
