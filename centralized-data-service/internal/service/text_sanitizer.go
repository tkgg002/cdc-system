package service

import (
	"encoding/json"
	"regexp"
	"strings"
)

var (
	sensitiveSecretPattern = regexp.MustCompile(`(?i)\b(secret|token|password|passwd|api_key|apikey)\b\s*[:=]\s*([^\s,;]+)`)
	sensitiveEmailPattern  = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	sensitivePhonePattern  = regexp.MustCompile(`(?i)(\+?\d[\d\-\s()]{7,}\d)`)
)

// SanitizeFreeformText normalizes whitespace, redacts common sensitive
// text fragments, and truncates to max characters when requested.
func SanitizeFreeformText(input string, max int) string {
	msg := strings.TrimSpace(input)
	if msg == "" {
		return ""
	}
	msg = strings.ReplaceAll(msg, "\n", " ")
	msg = strings.ReplaceAll(msg, "\r", " ")
	msg = strings.Join(strings.Fields(msg), " ")
	msg = sensitiveSecretPattern.ReplaceAllString(msg, `$1=***`)
	msg = sensitiveEmailPattern.ReplaceAllString(msg, "***")
	msg = sensitivePhonePattern.ReplaceAllStringFunc(msg, func(match string) string {
		digits := 0
		for _, r := range match {
			if r >= '0' && r <= '9' {
				digits++
			}
		}
		if digits < 8 {
			return match
		}
		return "***"
	})
	if max > 0 && len(msg) > max {
		msg = msg[:max] + "..."
	}
	return msg
}

// SanitizeNestedStrings walks arbitrary JSON-like values and applies
// free-form text sanitization to every string leaf while preserving the
// original shape for logs/audit payloads.
func SanitizeNestedStrings(value interface{}, max int) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			out[key] = SanitizeNestedStrings(item, max)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = SanitizeNestedStrings(item, max)
		}
		return out
	case []string:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = SanitizeFreeformText(item, max)
		}
		return out
	case string:
		return SanitizeFreeformText(typed, max)
	case json.RawMessage:
		var parsed interface{}
		if err := json.Unmarshal(typed, &parsed); err != nil {
			return SanitizeFreeformText(string(typed), max)
		}
		return SanitizeNestedStrings(parsed, max)
	default:
		return value
	}
}
