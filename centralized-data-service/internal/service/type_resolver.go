package service

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

// TypeResolver validates `data_type` strings from mapping_rules and, when
// needed, looks up ENUM membership from cdc_internal.enum_types.
//
// The regex here MUST stay in lockstep with the CHECK constraint in
// migration 020_mapping_rule_jsonpath.sql — any relaxation there needs a
// matching relaxation here.
type TypeResolver struct {
	db  *gorm.DB
	mu  sync.RWMutex
	enm map[string]enumEntry
}

type enumEntry struct {
	values   []string
	loadedAt time.Time
	isActive bool
}

func NewTypeResolver(db *gorm.DB) *TypeResolver {
	return &TypeResolver{
		db:  db,
		enm: make(map[string]enumEntry),
	}
}

var (
	reTypeWhitelist = regexp.MustCompile(`^(SMALLINT|INTEGER|BIGINT|REAL|DOUBLE PRECISION|BOOLEAN|DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL|JSON|JSONB|UUID|INET|CIDR|MACADDR|BYTEA|TEXT|CHAR\([1-9][0-9]{0,7}\)|VARCHAR\([1-9][0-9]{0,7}\)|NUMERIC\([1-9][0-9]?,[0-9][0-9]?\)|DECIMAL\([1-9][0-9]?,[0-9][0-9]?\)|(SMALLINT|INTEGER|BIGINT|TEXT|UUID)\[\]|ENUM:[a-z_][a-z0-9_]{0,62})$`)

	reCharLen    = regexp.MustCompile(`^(?:VAR)?CHAR\((\d+)\)$`)
	reNumericPrc = regexp.MustCompile(`^(?:NUMERIC|DECIMAL)\((\d+),(\d+)\)$`)
	reEnumRef    = regexp.MustCompile(`^ENUM:([a-z_][a-z0-9_]{0,62})$`)
)

// Validate returns true iff `spec` matches the whitelist regex.
func (r *TypeResolver) Validate(spec string) bool {
	return reTypeWhitelist.MatchString(strings.TrimSpace(spec))
}

// ValidateValue checks that `value` fits the declared type bounds. Returns
// a non-empty violation string (e.g. "too long: 15 > 10") when it does not;
// empty string means the value is acceptable.
//
// This is a best-effort pre-insert guard — the database's own CHECK / type
// constraint is still the ultimate authority.
func (r *TypeResolver) ValidateValue(ctx context.Context, spec string, value any) string {
	if value == nil {
		return ""
	}
	spec = strings.TrimSpace(spec)

	if m := reCharLen.FindStringSubmatch(spec); m != nil {
		maxLen, _ := strconv.Atoi(m[1])
		s, ok := value.(string)
		if !ok {
			return fmt.Sprintf("expected string for %s, got %T", spec, value)
		}
		if len(s) > maxLen {
			return fmt.Sprintf("too long: %d > %d (%s)", len(s), maxLen, spec)
		}
		return ""
	}

	if m := reNumericPrc.FindStringSubmatch(spec); m != nil {
		precision, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		return validateNumericPrecision(value, precision, scale, spec)
	}

	if m := reEnumRef.FindStringSubmatch(spec); m != nil {
		enumName := m[1]
		members, err := r.ResolveEnum(ctx, enumName)
		if err != nil {
			return fmt.Sprintf("enum lookup %q failed: %v", enumName, err)
		}
		s, ok := value.(string)
		if !ok {
			return fmt.Sprintf("enum %s expects string, got %T", enumName, value)
		}
		for _, m := range members {
			if m == s {
				return ""
			}
		}
		return fmt.Sprintf("enum %s: %q not in members %v", enumName, s, members)
	}

	return ""
}

// ResolveEnum fetches enum member list with 60s TTL cache.
func (r *TypeResolver) ResolveEnum(ctx context.Context, name string) ([]string, error) {
	r.mu.RLock()
	if e, ok := r.enm[name]; ok && time.Since(e.loadedAt) < 60*time.Second {
		r.mu.RUnlock()
		if !e.isActive {
			return nil, fmt.Errorf("enum %q is inactive", name)
		}
		return e.values, nil
	}
	r.mu.RUnlock()

	var row struct {
		Values   string `gorm:"column:values"`
		IsActive bool   `gorm:"column:is_active"`
	}
	// Postgres arrays come back as {a,b,c} text when we cast to text — we
	// pull as text so GORM doesn't need a pq.StringArray binding.
	err := r.db.WithContext(ctx).Raw(
		`SELECT array_to_string(values, ',') AS values, is_active
		   FROM cdc_internal.enum_types WHERE name = ?`, name,
	).Scan(&row).Error
	if err != nil {
		return nil, fmt.Errorf("load enum %q: %w", name, err)
	}
	if row.Values == "" && !row.IsActive {
		return nil, fmt.Errorf("enum %q not found", name)
	}

	members := strings.Split(row.Values, ",")
	r.mu.Lock()
	r.enm[name] = enumEntry{values: members, loadedAt: time.Now(), isActive: row.IsActive}
	r.mu.Unlock()

	if !row.IsActive {
		return nil, fmt.Errorf("enum %q is inactive", name)
	}
	return members, nil
}

// InvalidateEnum clears the cached entry so the next ResolveEnum call
// re-reads — used when CMS updates an enum via admin API.
func (r *TypeResolver) InvalidateEnum(name string) {
	r.mu.Lock()
	delete(r.enm, name)
	r.mu.Unlock()
}

func validateNumericPrecision(value any, precision, scale int, spec string) string {
	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case float64:
		s = strconv.FormatFloat(v, 'f', -1, 64)
	case int64:
		s = strconv.FormatInt(v, 10)
	case int:
		s = strconv.FormatInt(int64(v), 10)
	default:
		return fmt.Sprintf("expected numeric for %s, got %T", spec, value)
	}
	s = strings.TrimPrefix(s, "-")
	dotIdx := strings.Index(s, ".")
	var intPart, fracPart string
	if dotIdx == -1 {
		intPart = s
	} else {
		intPart = s[:dotIdx]
		fracPart = s[dotIdx+1:]
	}
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}
	intDigits := len(intPart)
	if intPart == "0" {
		intDigits = 0
	}
	fracDigits := len(strings.TrimRight(fracPart, "0"))
	if intDigits+fracDigits > precision {
		return fmt.Sprintf("overflow: total digits %d > precision %d (%s)", intDigits+fracDigits, precision, spec)
	}
	if fracDigits > scale {
		return fmt.Sprintf("overflow: fractional digits %d > scale %d (%s)", fracDigits, scale, spec)
	}
	return ""
}
