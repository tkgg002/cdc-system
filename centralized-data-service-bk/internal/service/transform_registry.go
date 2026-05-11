// Package service hosts the Shadow→Master transmutation pipeline.
//
// transform_registry.go: the whitelist of value transforms a mapping rule
// can opt into via `transform_fn`. Anything outside this map is rejected
// at rule-save time (CMS) and at rule-load time (Worker) so a typo in the
// admin UI can never turn into arbitrary code execution.
package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TransformFunc converts a raw extracted value (Go-side representation of
// whatever gjson returned) into the shape the target SQL column expects.
//
// Returning an error lets the caller decide between NULL-out vs skip-row
// depending on the rule's is_nullable flag.
type TransformFunc func(raw any) (any, error)

// ErrTransformNotWhitelisted signals a rule referenced an unknown
// transform_fn — the caller MUST reject the rule, never fallback.
var ErrTransformNotWhitelisted = errors.New("transform_fn not whitelisted")

// transformRegistry is the single source of truth. Keys match the values
// stored in cdc_mapping_rules.transform_fn.
var transformRegistry = map[string]TransformFunc{
	"mongo_date_ms":     transformMongoDateMs,
	"oid_to_hex":        transformOIDToHex,
	"bigint_str":        transformBigIntStr,
	"numeric_cast":      transformNumericCast,
	"lowercase":         transformLowercase,
	"jsonb_passthrough": transformJSONBPassthrough,
	"null_if_empty":     transformNullIfEmpty,
}

// ApplyTransform resolves and applies a transform by name. An empty name
// is a no-op identity pass (rules without transform_fn still validate).
func ApplyTransform(name string, raw any) (any, error) {
	if name == "" {
		return raw, nil
	}
	fn, ok := transformRegistry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransformNotWhitelisted, name)
	}
	return fn(raw)
}

// IsTransformWhitelisted is the cheap check CMS uses to validate incoming
// rule payloads before persisting them.
func IsTransformWhitelisted(name string) bool {
	if name == "" {
		return true
	}
	_, ok := transformRegistry[name]
	return ok
}

// ListTransforms returns the sorted whitelist for UI dropdowns.
func ListTransforms() []string {
	out := make([]string, 0, len(transformRegistry))
	for k := range transformRegistry {
		out = append(out, k)
	}
	// Deterministic order helps FE select options stay stable.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[i] > out[j] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// ---- Individual transforms ----

// transformMongoDateMs understands the three shapes Debezium MongoDB
// connectors emit for dates: {"$date": <ISO-8601 string>}, {"$date": <ms int>},
// or the bare epoch-ms number after a prior stripping pass. Returns a
// time.Time suitable for a TIMESTAMPTZ column.
func transformMongoDateMs(raw any) (any, error) {
	// Unwrap Mongo Extended JSON wrapper.
	if m, ok := raw.(map[string]any); ok {
		if v, has := m["$date"]; has {
			return transformMongoDateMs(v)
		}
	}
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case string:
		// Try ISO-8601 first (most common from Debezium).
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UTC(), nil
		}
		// Fall back to numeric string (epoch ms).
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.UnixMilli(n).UTC(), nil
		}
		return nil, fmt.Errorf("mongo_date_ms: unrecognised string %q", v)
	case float64:
		return time.UnixMilli(int64(v)).UTC(), nil
	case int64:
		return time.UnixMilli(v).UTC(), nil
	case int:
		return time.UnixMilli(int64(v)).UTC(), nil
	}
	return nil, fmt.Errorf("mongo_date_ms: unsupported type %T", raw)
}

// transformOIDToHex unwraps {"$oid": "abc"} to the hex string. Pass-through
// for plain strings so rules can safely chain transforms.
func transformOIDToHex(raw any) (any, error) {
	if m, ok := raw.(map[string]any); ok {
		if v, has := m["$oid"]; has {
			if s, ok := v.(string); ok {
				return s, nil
			}
		}
	}
	if s, ok := raw.(string); ok {
		return s, nil
	}
	if raw == nil {
		return nil, nil
	}
	return nil, fmt.Errorf("oid_to_hex: unsupported type %T", raw)
}

// transformBigIntStr handles {"$numberLong": "123"}, plain numbers, and
// string-encoded ints — returns int64.
func transformBigIntStr(raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	if m, ok := raw.(map[string]any); ok {
		if v, has := m["$numberLong"]; has {
			return transformBigIntStr(v)
		}
		if v, has := m["$numberInt"]; has {
			return transformBigIntStr(v)
		}
	}
	switch v := raw.(type) {
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bigint_str: parse %q: %w", v, err)
		}
		return n, nil
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	}
	return nil, fmt.Errorf("bigint_str: unsupported type %T", raw)
}

// transformNumericCast handles the 6-shape Mongo numeric matrix:
// plain int/float/string, {"$numberLong"}, {"$numberInt"}, {"$numberDecimal"}.
// Returns a string representation so downstream GORM + pgx can pick up
// NUMERIC precision without lossy float round-trip.
func transformNumericCast(raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	if m, ok := raw.(map[string]any); ok {
		for _, k := range []string{"$numberDecimal", "$numberLong", "$numberInt"} {
			if v, has := m[k]; has {
				return transformNumericCast(v)
			}
		}
	}
	switch v := raw.(type) {
	case string:
		// Validate parse-ability; return cleaned string for NUMERIC column.
		s := strings.TrimSpace(v)
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return nil, fmt.Errorf("numeric_cast: parse %q: %w", v, err)
		}
		return s, nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	}
	return nil, fmt.Errorf("numeric_cast: unsupported type %T", raw)
}

func transformLowercase(raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("lowercase: expected string, got %T", raw)
	}
	return strings.ToLower(s), nil
}

// transformJSONBPassthrough serialises any value to a JSON string so the
// caller can bind it into a JSONB column directly.
func transformJSONBPassthrough(raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("jsonb_passthrough: marshal: %w", err)
	}
	return string(b), nil
}

// transformNullIfEmpty turns empty string / map / slice into nil so the
// target column stores NULL instead of a meaningless empty container.
func transformNullIfEmpty(raw any) (any, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case string:
		if v == "" {
			return nil, nil
		}
	case map[string]any:
		if len(v) == 0 {
			return nil, nil
		}
	case []any:
		if len(v) == 0 {
			return nil, nil
		}
	}
	return raw, nil
}
