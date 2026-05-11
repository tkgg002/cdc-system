// Package naming centralizes schema/object naming conventions that the
// data-lake layer (currently called "shadow") uses across admin,
// provisioning, and sinkworker. The prefix is sourced from
// CDC_SHADOW_SCHEMA_PREFIX so renaming the convention (e.g. "lake_",
// "raw_") is a config change, not a code change.
package naming

import (
	"os"
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
