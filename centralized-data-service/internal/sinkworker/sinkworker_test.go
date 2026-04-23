package sinkworker

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestExtractTableFromTopic: sanity-check the Debezium topic → table
// normalisation. Hyphen becomes underscore, case folds down, and
// malformed topics return "" so HandleMessage can reject early.
func TestExtractTableFromTopic(t *testing.T) {
	cases := map[string]string{
		"cdc.goopay.payment-bill-service.payment-bills": "payment_bills",
		"cdc.goopay.centralized-export-service.export-jobs": "export_jobs",
		"cdc.goopay.ServiceA.UserProfiles": "userprofiles",
		"malformed":  "",
		"a.b":        "",
	}
	for in, want := range cases {
		got := extractTableFromTopic(in)
		if got != want {
			t.Errorf("extractTableFromTopic(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestShouldSkipBusinessKey confirms every system field is skipped and
// ordinary business keys (including `d` from the "column D" integrity
// proof) pass through.
func TestShouldSkipBusinessKey(t *testing.T) {
	skip := []string{
		"_gpay_id", "_gpay_source_id", "_gpay_deleted",
		"_id", "_raw_data", "_source", "_synced_at", "_source_ts",
		"_version", "_hash", "_created_at", "_updated_at",
		"__v", "__v2",
	}
	for _, k := range skip {
		if !shouldSkipBusinessKey(k) {
			t.Errorf("expected %q to be skipped", k)
		}
	}
	pass := []string{"amount", "orderId", "state", "d", "createdAt", "testField"}
	for _, k := range pass {
		if shouldSkipBusinessKey(k) {
			t.Errorf("expected %q to pass through", k)
		}
	}
}

// TestExtractSourceID: Mongo $oid, plain string id, and Kafka key fallback.
func TestExtractSourceID(t *testing.T) {
	// 1. after._id with $oid
	after := map[string]any{"_id": map[string]any{"$oid": "abc123"}}
	if got := extractSourceID(after, nil); got != "abc123" {
		t.Errorf("$oid case: got %q, want abc123", got)
	}

	// 2. after._id as bare string
	after = map[string]any{"_id": "docX"}
	if got := extractSourceID(after, nil); got != "docX" {
		t.Errorf("string id: got %q", got)
	}

	// 3. Debezium JSON key fallback: {"id":"{\"$oid\":\"def456\"}"}
	after = map[string]any{}
	key := []byte(`{"id":"{\"$oid\":\"def456\"}"}`)
	if got := extractSourceID(after, key); got != "def456" {
		t.Errorf("key oid fallback: got %q", got)
	}

	// 4. Nothing at all
	if got := extractSourceID(map[string]any{}, nil); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

// TestBuildUpsertSQL confirms the generated statement:
//   - Binds positional params in sorted-key order
//   - Skips _gpay_id, _gpay_source_id, _created_at from UPDATE SET
//   - Embeds the OCC guard (_source_ts comparison)
//   - Quotes the table name
func TestBuildUpsertSQL(t *testing.T) {
	rec := map[string]any{
		"_gpay_id":        int64(1),
		"_gpay_source_id": "oid",
		"_raw_data":       "{}",
		"_source":         "debezium-v125",
		"_synced_at":      "2026-04-17T00:00:00Z",
		"_source_ts":      int64(123),
		"_version":        int64(1),
		"_hash":           "h",
		"_gpay_deleted":   false,
		"_created_at":     "t",
		"_updated_at":     "t",
		"amount":          int64(500),
		"d":               "column-D",
	}
	sqlText, values := buildUpsertSQL("payment_bills", rec)

	// Quoted table name
	if !strings.Contains(sqlText, `cdc_internal."payment_bills"`) {
		t.Fatalf("table not quoted: %s", sqlText)
	}
	// OCC guard presence
	if !strings.Contains(sqlText, "EXCLUDED._source_ts > ") {
		t.Errorf("OCC guard missing: %s", sqlText)
	}
	// ON CONFLICT partial target
	if !strings.Contains(sqlText, `ON CONFLICT (_gpay_source_id) WHERE NOT _gpay_deleted`) {
		t.Errorf("partial conflict target missing")
	}
	// Immutable columns not in UPDATE SET
	for _, immut := range []string{`"_gpay_id" = EXCLUDED`, `"_gpay_source_id" = EXCLUDED`, `"_created_at" = EXCLUDED`} {
		if strings.Contains(sqlText, immut) {
			t.Errorf("immutable column %q should not appear in UPDATE SET: %s", immut, sqlText)
		}
	}
	// Values count equals keys
	if len(values) != len(rec) {
		t.Errorf("values len %d != keys len %d", len(values), len(rec))
	}
}

// TestCanonicalJSONDeterministic: the same logical payload must produce
// the same bytes regardless of insertion order — critical for hash stability.
func TestCanonicalJSONDeterministic(t *testing.T) {
	a := map[string]any{"z": 1, "a": 2, "m": map[string]any{"y": 9, "x": 8}}
	b := map[string]any{"m": map[string]any{"x": 8, "y": 9}, "a": 2, "z": 1}
	ja, err := canonicalJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := canonicalJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(ja) != string(jb) {
		t.Errorf("canonicalJSON not deterministic:\nA=%s\nB=%s", ja, jb)
	}
}

// TestDecodeAfterJSONString: Debezium MongoDB sends `after` as a JSON
// string wrapped in an Avro union.
func TestDecodeAfterJSONString(t *testing.T) {
	raw := map[string]any{
		"string": `{"_id":{"$oid":"x1"},"amount":42,"d":"payload"}`,
	}
	got, err := decodeAfter(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got["amount"].(float64) != 42 {
		t.Errorf("amount mismatch: %v", got["amount"])
	}
	if got["d"] != "payload" {
		t.Errorf("d mismatch")
	}
}

// TestExtractSourceTsMs picks the nested source.ts_ms in multiple numeric
// encodings and falls back to top-level ts_ms.
func TestExtractSourceTsMs(t *testing.T) {
	env := map[string]any{
		"source": map[string]any{"ts_ms": float64(1700000000000)},
	}
	if got := extractSourceTsMs(env); got != 1700000000000 {
		t.Errorf("nested ts_ms: got %d", got)
	}

	env = map[string]any{"ts_ms": int64(42)}
	if got := extractSourceTsMs(env); got != 42 {
		t.Errorf("top-level fallback: got %d", got)
	}

	env = map[string]any{}
	if got := extractSourceTsMs(env); got != 0 {
		t.Errorf("missing: got %d", got)
	}
}

// TestInferSQLType exercises the basic type inference rules. Keep these
// synced with schema_manager.go#inferSQLType when extending.
func TestInferSQLType(t *testing.T) {
	cases := map[string]any{
		"BOOLEAN":     true,
		"NUMERIC":     float64(1.5),
		"JSONB":       map[string]any{"a": 1},
		"TEXT":        "hello",
	}
	for want, in := range cases {
		if got := inferSQLType(in); got != want {
			t.Errorf("inferSQLType(%T) = %q, want %q", in, got, want)
		}
	}
	// Arrays also JSONB
	if got := inferSQLType([]any{1, 2, 3}); got != "JSONB" {
		t.Errorf("array: got %q", got)
	}
}

// TestJSONCanonicalRoundTrip: the canonical bytes remain valid JSON.
func TestJSONCanonicalRoundTrip(t *testing.T) {
	in := map[string]any{"b": 2, "a": 1, "list": []any{1, 2, 3}}
	out, err := canonicalJSON(in)
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("canonical JSON not parseable: %v\n%s", err, out)
	}
}

// TestIsSnapshotEvent covers the four envelope shapes that separate
// snapshot replay from live-oplog streaming (plan Phase 2 §2 Option B).
func TestIsSnapshotEvent(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]any
		want bool
	}{
		{"streaming-missing-source", map[string]any{}, false},
		{"streaming-snapshot-false", map[string]any{
			"source": map[string]any{"snapshot": "false"},
		}, false},
		{"snapshot-true", map[string]any{
			"source": map[string]any{"snapshot": "true"},
		}, true},
		{"snapshot-last", map[string]any{
			"source": map[string]any{"snapshot": "last"},
		}, true},
		{"snapshot-incremental", map[string]any{
			"source": map[string]any{"snapshot": "incremental"},
		}, true},
		{"avro-union-wrapped-true", map[string]any{
			"source": map[string]any{"snapshot": map[string]any{"string": "true"}},
		}, true},
		{"bool-true", map[string]any{
			"source": map[string]any{"snapshot": true},
		}, true},
	}
	for _, c := range cases {
		if got := isSnapshotEvent(c.env); got != c.want {
			t.Errorf("%s: want %v got %v", c.name, c.want, got)
		}
	}
}

// TestBuildUpsertSQLSnapshot verifies the snapshot SQL emits DO NOTHING
// (no UPDATE SET branch) and keeps the same partial conflict target
// as the streaming builder.
func TestBuildUpsertSQLSnapshot(t *testing.T) {
	rec := map[string]any{
		"_gpay_id":        int64(1),
		"_gpay_source_id": "oid-abc",
		"_raw_data":       "{}",
		"_source":         "debezium-v125",
		"_synced_at":      "2026-04-21T00:00:00Z",
		"_source_ts":      int64(1777000000000),
		"_version":        int64(1),
		"_hash":           "h",
		"_gpay_deleted":   false,
		"_created_at":     "t",
		"_updated_at":     "t",
		"amount":          int64(500),
	}
	sqlText, values := buildUpsertSQLSnapshot("export_jobs", rec)

	if !strings.Contains(sqlText, `cdc_internal."export_jobs"`) {
		t.Fatalf("table not quoted: %s", sqlText)
	}
	if !strings.Contains(sqlText, `ON CONFLICT (_gpay_source_id) WHERE NOT _gpay_deleted`) {
		t.Errorf("partial conflict target missing: %s", sqlText)
	}
	if !strings.Contains(sqlText, "DO NOTHING") {
		t.Errorf("DO NOTHING clause missing: %s", sqlText)
	}
	if strings.Contains(sqlText, "DO UPDATE") {
		t.Errorf("snapshot SQL must NOT contain DO UPDATE: %s", sqlText)
	}
	if strings.Contains(sqlText, "EXCLUDED._source_ts > ") {
		t.Errorf("snapshot SQL must NOT contain OCC guard: %s", sqlText)
	}
	if len(values) != len(rec) {
		t.Errorf("values len %d != keys len %d", len(values), len(rec))
	}
}
