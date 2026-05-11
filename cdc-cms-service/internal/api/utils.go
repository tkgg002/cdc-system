package api

import (
	"strconv"
	"github.com/gofiber/fiber/v2"
)

// intQuery parses an integer query parameter with a fallback default.
func intQuery(c *fiber.Ctx, key string, defaultVal int) int {
	v, err := strconv.Atoi(c.Query(key))
	if err != nil || v <= 0 {
		return defaultVal
	}
	return v
}

// normalizeShadowIdent converts an arbitrary source_db value into a
// Postgres-safe identifier suffix used to derive shadow_<src> schema
// names. Lowercases letters; non-alphanumeric/underscore → underscore.
// No length cap here — caller's validateIdent enforces 63-byte limit
// at the schema layer.
func normalizeShadowIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
			out = append(out, c+32)
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// isValidTimestampField returns true when the name is a safe Mongo field
// identifier: ^[A-Za-z_][A-Za-z0-9_]{0,63}$. Matches resolveTimestampField
// in centralized-data-service/internal/service/recon_source_agent.go so CMS
// and Worker agree on what is storable. Rejects dotted paths ($where, etc).
func isValidTimestampField(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}
