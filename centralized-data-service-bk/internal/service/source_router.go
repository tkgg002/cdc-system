// Package service — source_router.go
//
// Helpers shared by the Worker-side command handlers introduced during the
// "CDC worker boundary refactor" (workspace feature-cdc-integration). The
// tables (Debezium-only) and violated the NATS async boundary — see
// 10_gap_analysis_scan_fields_boundary_violation.md for the audit. These
// helpers centralise sync-engine routing so each handler ships with the
// same logic.
package service

import (
	"fmt"
	"strings"
	"time"

	"centralized-data-service/internal/model"
)

// ShouldUseDebezium returns true for Debezium-only or dual-engine rows.
func ShouldUseDebezium(entry *model.TableRegistry) bool {
	if entry == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(entry.SyncEngine)) {
	case "debezium", "both":
		return true
	default:
		return false
	}
}

// InferTypeFromRawData infers a Postgres column type from a single JSONB
// sample value. Used by HandleScanFields when routing through the
// Debezium path (where _raw_data JSONB is the only schema source we have
// on the Worker side). The inference favours safe supertypes (TEXT for
// unknown, TIMESTAMP for RFC3339 strings, NUMERIC for floats) so approved
// rules can widen later without data loss.
func InferTypeFromRawData(jsonValue interface{}) string {
	if jsonValue == nil {
		return "TEXT"
	}
	switch v := jsonValue.(type) {
	case bool:
		return "BOOLEAN"
	case float64:
		// JSON unmarshals all numbers as float64. Treat integral values as
		// BIGINT so we do not silently widen PKs to NUMERIC.
		if v == float64(int64(v)) {
			return "BIGINT"
		}
		return "NUMERIC"
	case string:
		if isRFC3339Like(v) {
			return "TIMESTAMP"
		}
		return "TEXT"
	case map[string]interface{}, []interface{}:
		return "JSONB"
	default:
		// Covers json.Number, int, int64 etc. that pgx/gjson could yield.
		return fmt.Sprintf("%T_fallback_TEXT", v)[:0] + "TEXT"
	}
}

// isRFC3339Like is a cheap RFC3339 / date heuristic so we do not need to
// parse every string. We only care that the timestamp router catches the
// obvious cases (`2026-04-17T12:34:56Z`, `2026-04-17 12:34:56+07:00`).
func isRFC3339Like(s string) bool {
	if len(s) < 10 || len(s) > 35 {
		return false
	}
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}
