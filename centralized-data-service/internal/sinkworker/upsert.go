package sinkworker

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Columns we never copy on UPDATE:
//   - _gpay_id        : immutable Sonyflake identity (changing would break audit)
//   - _gpay_source_id : anchor / partial-unique key (same by definition)
//   - _created_at     : first-write timestamp, must not drift
var immutableOnUpdate = map[string]struct{}{
	"_gpay_id":        {},
	"_gpay_source_id": {},
	"_created_at":     {},
}

// buildUpsertSQL turns the final record (10 system fields + N business
// fields) into parameterised INSERT ... ON CONFLICT UPDATE SQL. The ordered
// cols/values slices are returned together to guarantee stable binding:
// PostgreSQL uses `$1..$N` by position so the keys *must* be sorted once
// and reused for both the column list and the value slice.
//
// Output SQL template (with OCC guard baked in per gap-fix §7.5):
//
//	INSERT INTO cdc_internal."<table>" ("col1","col2",...)
//	VALUES ($1, $2, ...)
//	ON CONFLICT (_gpay_source_id) WHERE NOT _gpay_deleted
//	DO UPDATE SET "col1"=EXCLUDED."col1", ...
//	WHERE cdc_internal."<table>"._source_ts IS NULL
//	   OR EXCLUDED._source_ts > cdc_internal."<table>"._source_ts
func buildUpsertSQL(table string, record map[string]any) (sqlText string, values []any) {
	keys := make([]string, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	colList := make([]string, 0, len(keys))
	placeholders := make([]string, 0, len(keys))
	values = make([]any, 0, len(keys))
	for i, k := range keys {
		colList = append(colList, quoteIdent(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		values = append(values, sqlBindValue(record[k]))
	}

	updateSets := make([]string, 0, len(keys))
	for _, k := range keys {
		if _, skip := immutableOnUpdate[k]; skip {
			continue
		}
		updateSets = append(updateSets,
			fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(k), quoteIdent(k)))
	}

	qt := "cdc_internal." + quoteIdent(table)

	sqlText = fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
ON CONFLICT (_gpay_source_id) WHERE NOT _gpay_deleted
DO UPDATE SET %s
WHERE %s._source_ts IS NULL
   OR EXCLUDED._source_ts > %s._source_ts`,
		qt,
		strings.Join(colList, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateSets, ", "),
		qt, qt,
	)
	return sqlText, values
}

// buildUpsertSQLSnapshot mirrors buildUpsertSQL but emits ON CONFLICT
// DO NOTHING. Used exclusively for rows produced by Debezium incremental
// or initial snapshot — plan Phase 2 §2 Option B: a replay of historical
// state must never clobber a more recent streaming update of the same
// document. Only INSERTs land; conflicts are silently skipped.
func buildUpsertSQLSnapshot(table string, record map[string]any) (sqlText string, values []any) {
	keys := make([]string, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	colList := make([]string, 0, len(keys))
	placeholders := make([]string, 0, len(keys))
	values = make([]any, 0, len(keys))
	for i, k := range keys {
		colList = append(colList, quoteIdent(k))
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		values = append(values, sqlBindValue(record[k]))
	}

	qt := "cdc_internal." + quoteIdent(table)

	sqlText = fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
ON CONFLICT (_gpay_source_id) WHERE NOT _gpay_deleted
DO NOTHING`,
		qt,
		strings.Join(colList, ", "),
		strings.Join(placeholders, ", "),
	)
	return sqlText, values
}

// quoteIdent safely quotes a SQL identifier by doubling embedded quotes.
// Table names come from the Kafka topic (post-normalisation) and business
// field names come from Mongo, so treating them as untrusted input is
// mandatory (lesson #59 — Dynamic SQL table names PHẢI quoted).
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// sqlBindValue adapts arbitrary Go values into types the pgx driver can
// bind directly. Maps and slices are serialised to JSON text so they
// round-trip cleanly into JSONB columns (and into TEXT columns as
// human-readable JSON). Scalars pass through unchanged.
//
// Without this, GORM/pgx fall back to Go's default stringer for maps —
// producing artifacts like `map[$date:1.7e+12]` that break type parsing.
func sqlBindValue(v any) any {
	switch v.(type) {
	case map[string]any, []any, []map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
	return v
}
