package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"centralized-data-service/internal/model"
	"centralized-data-service/pkgs/metrics"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ErrSchemaDrift is returned when a Debezium payload references a
// field the registry does not know about. Callers route this to the
// DLQ with error_message="schema_drift: unknown_field=<field>".
var ErrSchemaDrift = errors.New("schema_drift")

// ErrMissingRequired — payload lacks a required field. Also DLQ-worthy
// but distinguished from drift: drift means "schema evolved", missing
// required means "producer bug / partial write".
var ErrMissingRequired = errors.New("missing_required_field")

// tableExpectations captures the allowed + required field sets for a
// single target table. Pulled from cdc_table_registry.expected_fields
// when present, otherwise derived from information_schema column list.
type tableExpectations struct {
	Required map[string]struct{}
	Known    map[string]struct{}
}

// registryExpected mirrors the JSONB schema shipped in migration 013.
type registryExpected struct {
	Required []string `json:"required"`
	Known    []string `json:"known"`
}

// SchemaValidator enforces Phase A (JSON converter) field-level
// validation BEFORE the Worker attempts to upsert a Kafka CDC event.
//
// Behavior:
//  - Missing required field → ErrMissingRequired — DLQ, no panic.
//  - Unknown field → ErrSchemaDrift — DLQ + `cdc_schema_drift_total`
//    counter increment.
//  - Zero-config table (no `expected_fields` + no target PG table yet) →
//    PASS (fail-open on bootstrap to avoid blocking brand-new tables).
//
// Cache: per-table expectations are memoized in-process. Call
// InvalidateCache(table) on schema.config.reload so hot edits take
// effect without a restart.
type SchemaValidator struct {
	db     *gorm.DB
	logger *zap.Logger
	cache  sync.Map // tableName → *tableExpectations
}

// NewSchemaValidator constructs the validator. `db` must point at the
// Postgres instance hosting cdc_table_registry + target tables (the
// worker primary connection).
func NewSchemaValidator(db *gorm.DB, logger *zap.Logger) *SchemaValidator {
	return &SchemaValidator{db: db, logger: logger}
}

// InvalidateCache drops the cached expectation set for `table` so the
// next validation reloads from registry/information_schema.
func (sv *SchemaValidator) InvalidateCache(table string) {
	sv.cache.Delete(table)
}

// ValidatePayload checks a decoded CDC `after` map against the target
// table's expected field set. Returns one of:
//   - nil              → payload is compatible
//   - ErrMissingRequired (wrapped with field name)
//   - ErrSchemaDrift   (wrapped with field name)
func (sv *SchemaValidator) ValidatePayload(tableName string, payload map[string]interface{}) error {
	if tableName == "" || payload == nil {
		return nil
	}

	exp, err := sv.loadOrBuild(tableName)
	if err != nil {
		// DB probe failed — we fail OPEN because blocking all events
		// because of a flaky information_schema query would be worse
		// than letting a possibly-drifted payload through (which the
		// OCC upsert then rejects).
		sv.logger.Warn("schema_validator: expectations probe failed — failing open",
			zap.String("table", tableName), zap.Error(err),
		)
		return nil
	}
	if exp == nil {
		// Zero-config: no registry entry, no materialised table.
		return nil
	}

	// Required-field check. Skip when the expectations list no
	// required fields — that is the common case when we derive from
	// information_schema (we can't distinguish required from nullable
	// at the info_schema level without a NOT NULL inspection; we keep
	// `required` empty by default and populate via registry instead).
	for req := range exp.Required {
		if _, ok := payload[req]; !ok {
			metrics.SchemaDriftDetected.
				WithLabelValues("worker", tableName).
				Inc()
			return fmt.Errorf("%w: %s", ErrMissingRequired, req)
		}
	}

	// Unknown-field check. A single offender is enough to fail — we
	// return the first field name so operators can land a registry
	// update or a migration.
	for k := range payload {
		if _, ok := exp.Known[k]; !ok {
			metrics.SchemaDriftDetected.
				WithLabelValues("worker", tableName).
				Inc()
			return fmt.Errorf("%w: unknown_field=%s", ErrSchemaDrift, k)
		}
	}

	return nil
}

// loadOrBuild returns cached expectations or builds them from the
// registry + information_schema.
func (sv *SchemaValidator) loadOrBuild(table string) (*tableExpectations, error) {
	if v, ok := sv.cache.Load(table); ok {
		return v.(*tableExpectations), nil
	}

	// 1. Pull registry entry.
	var entry model.TableRegistry
	err := sv.db.
		Where("target_table = ? AND is_active = ?", table, true).
		First(&entry).Error

	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("registry lookup: %w", err)
	}
	hasRegistry := err == nil

	exp := &tableExpectations{
		Required: make(map[string]struct{}),
		Known:    make(map[string]struct{}),
	}

	// 2. If registry has an explicit expected_fields payload, use it.
	if hasRegistry && len(entry.ExpectedFields) > 0 && !isJSONNull(entry.ExpectedFields) {
		var rx registryExpected
		if err := json.Unmarshal(entry.ExpectedFields, &rx); err == nil {
			for _, f := range rx.Required {
				exp.Required[f] = struct{}{}
			}
			for _, f := range rx.Known {
				exp.Known[strings.ToLower(f)] = struct{}{}
			}
		}
	}

	// 3. Union with information_schema columns (always — this catches
	// new columns added via schema evolution without requiring operators
	// to repopulate expected_fields manually).
	cols, err := sv.introspectColumns(table)
	if err != nil {
		// Table may not exist yet; that is an acceptable state.
		sv.logger.Debug("schema_validator: introspect returned error",
			zap.String("table", table), zap.Error(err),
		)
	}
	for _, c := range cols {
		exp.Known[strings.ToLower(c)] = struct{}{}
	}

	// If neither registry nor info_schema produced anything, treat as
	// unknown table — zero-config mode. Return nil so ValidatePayload
	// falls through without erroring.
	if len(exp.Known) == 0 {
		sv.cache.Store(table, (*tableExpectations)(nil))
		return nil, nil
	}

	// Always accept CDC housekeeping columns (migration 009 + schema_adapter).
	for _, k := range []string{
		"_id", "_source_ts", "_synced_at", "_hash", "_version",
		"_deleted", "_raw_data", "_source", "_created_at", "_updated_at",
	} {
		exp.Known[k] = struct{}{}
	}

	sv.cache.Store(table, exp)
	return exp, nil
}

// introspectColumns returns the column names on the target PG table.
// Lowercased; empty when the table does not exist.
func (sv *SchemaValidator) introspectColumns(table string) ([]string, error) {
	var names []string
	err := sv.db.Raw(`
		SELECT column_name
		  FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = ?`,
		table,
	).Scan(&names).Error
	if err != nil {
		return nil, err
	}
	return names, nil
}

// isJSONNull handles registry rows whose JSONB is literally `null`.
func isJSONNull(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null"
}

// ValidatePayloadWithCase is the case-insensitive lookup variant. The
// main path lowercases the expected set — payloads may ship mixed case
// (Mongo user-provided keys). This helper normalises the payload before
// checking.
//
// NOTE: we only lowercase for MATCHING; the unknown-field error message
// preserves the original case so operators see exactly what came in.
func (sv *SchemaValidator) ValidatePayloadWithCase(tableName string, payload map[string]interface{}) error {
	if payload == nil {
		return sv.ValidatePayload(tableName, payload)
	}
	exp, err := sv.loadOrBuild(tableName)
	if err != nil || exp == nil {
		return nil
	}
	for req := range exp.Required {
		found := false
		for k := range payload {
			if strings.EqualFold(k, req) {
				found = true
				break
			}
		}
		if !found {
			metrics.SchemaDriftDetected.
				WithLabelValues("worker", tableName).
				Inc()
			return fmt.Errorf("%w: %s", ErrMissingRequired, req)
		}
	}
	for k := range payload {
		if _, ok := exp.Known[strings.ToLower(k)]; !ok {
			metrics.SchemaDriftDetected.
				WithLabelValues("worker", tableName).
				Inc()
			return fmt.Errorf("%w: unknown_field=%s", ErrSchemaDrift, k)
		}
	}
	return nil
}
