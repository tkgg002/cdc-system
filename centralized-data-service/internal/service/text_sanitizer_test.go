package service

import (
	"strings"
	"testing"
)

func TestSanitizeFreeformTextRedactsCommonSensitiveValues(t *testing.T) {
	got := SanitizeFreeformText("boom\nemail=alice@example.com phone=0901234567 secret=token-123 api_key=xyz", 2000)

	for _, needle := range []string{"alice@example.com", "0901234567", "token-123", "xyz"} {
		if strings.Contains(got, needle) {
			t.Fatalf("sanitized text still contains %q: %q", needle, got)
		}
	}
	for _, expected := range []string{"email=***", "phone=***", "secret=***", "api_key=***"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("sanitized text missing %q: %q", expected, got)
		}
	}
}

func TestSanitizeNestedStringsPreservesShapeButRedactsLeaves(t *testing.T) {
	value := map[string]interface{}{
		"error": "email=alice@example.com",
		"nested": map[string]interface{}{
			"phone": "0901234567",
		},
		"items": []interface{}{
			"secret=token-123",
			map[string]interface{}{"api_key": "api_key=xyz"},
		},
	}

	got := SanitizeNestedStrings(value, 2000).(map[string]interface{})
	if got["error"] != "email=***" {
		t.Fatalf("top-level string not sanitized: %#v", got["error"])
	}
	nested := got["nested"].(map[string]interface{})
	if nested["phone"] != "***" {
		t.Fatalf("nested string not sanitized: %#v", nested["phone"])
	}
	items := got["items"].([]interface{})
	if items[0] != "secret=***" {
		t.Fatalf("slice string not sanitized: %#v", items[0])
	}
	itemMap := items[1].(map[string]interface{})
	if itemMap["api_key"] != "api_key=***" {
		t.Fatalf("nested map in slice not sanitized: %#v", itemMap["api_key"])
	}
}
