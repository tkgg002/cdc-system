// Package naming centralizes schema/object naming conventions that the
// data-lake layer (currently called "shadow") uses across admin,
// provisioning, and sinkworker. The prefix is sourced from
// CDC_SHADOW_SCHEMA_PREFIX so renaming the convention (e.g. "lake_",
// "raw_") is a config change, not a code change.
package naming

import (
	"os"
	"strings"
	"sync"
)

const defaultShadowPrefix = "shadow_"

var (
	shadowOnce   sync.Once
	shadowPrefix string
)

// ShadowSchemaPrefix returns the configured prefix (default "shadow_").
// Cached after first read; env changes after boot are ignored.
func ShadowSchemaPrefix() string {
	shadowOnce.Do(func() {
		shadowPrefix = os.Getenv("CDC_SHADOW_SCHEMA_PREFIX")
		if shadowPrefix == "" {
			shadowPrefix = defaultShadowPrefix
		}
	})
	return shadowPrefix
}

// ShadowSchemaName concatenates the configured prefix with suffix.
// Example: ShadowSchemaName("goopay_source") -> "shadow_goopay_source".
func ShadowSchemaName(suffix string) string {
	return ShadowSchemaPrefix() + suffix
}

// NormalizeIdentifier converts an external identifier (e.g. Mongo collection name with hyphen)
// to a PG-safe lowercase snake_case form.
func NormalizeIdentifier(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		// replace any non-alphanum with underscore, collapse repeats
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unknown"
	}
	// PG identifiers are limited to 63 bytes
	if len(out) > 63 {
		out = out[:63]
	}
	return out
}
