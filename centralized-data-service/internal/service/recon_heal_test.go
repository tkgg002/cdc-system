package service

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestHealOCCSkipsOlderTs — verify the OCC WHERE clause emitted by
// SchemaAdapter includes `_source_ts < EXCLUDED._source_ts` guard when
// sourceTsMs > 0. This is the DB-side enforcement that a heal with an
// older timestamp does NOT overwrite a newer row.
//
// We can't spin up Postgres in unit tests — instead we assert the SQL
// shape and WHERE clause, then trust the DB semantics (plan v3 §6).
// Runtime verification is covered in the workspace walkthrough.
func TestHealOCCSkipsOlderTs(t *testing.T) {
	sa := &SchemaAdapter{}
	schema := &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":        {Name: "_id", DataType: "text"},
			"name":       {Name: "name", DataType: "text"},
			"_source_ts": {Name: "_source_ts", DataType: "bigint"},
			"_hash":      {Name: "_hash", DataType: "text"},
		},
		PKColumn: "_id",
	}
	// ts = some past time
	olderTs := time.Now().Add(-1 * time.Hour).UnixMilli()

	query, args := sa.BuildUpsertSQL(
		schema, "test_tbl", "_id",
		"some-id",
		map[string]interface{}{"name": "alpha"},
		`{"name":"alpha"}`, "recon-heal", "abc123",
		olderTs,
	)

	// The WHERE guard must include the _source_ts comparison.
	if !strings.Contains(query, `"test_tbl"."_source_ts" < EXCLUDED."_source_ts"`) {
		t.Fatalf("OCC WHERE clause missing _source_ts guard:\n%s", query)
	}
	if !strings.Contains(query, `ON CONFLICT ("_id")`) {
		t.Fatalf("upsert missing ON CONFLICT target: %s", query)
	}
	// Sanity: the ts value must appear in args so the DB can compare it.
	found := false
	for _, a := range args {
		if v, ok := a.(int64); ok && v == olderTs {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected source ts %d to appear in args, got %v", olderTs, args)
	}
}

// TestHealOCCAppliesNewerTs — symmetric to the above. When the
// incoming ts is newer, the DB executes UPDATE (via ON CONFLICT) only
// if the WHERE guard permits. We assert the generated SQL is
// structurally identical — the caller semantically passes a newer ts
// and trusts PG to run the UPDATE.
func TestHealOCCAppliesNewerTs(t *testing.T) {
	sa := &SchemaAdapter{}
	schema := &TableSchema{
		Columns: map[string]ColumnInfo{
			"_id":        {Name: "_id", DataType: "text"},
			"amount":     {Name: "amount", DataType: "bigint"},
			"_source_ts": {Name: "_source_ts", DataType: "bigint"},
		},
		PKColumn: "_id",
	}
	newerTs := time.Now().UnixMilli()

	query, args := sa.BuildUpsertSQL(
		schema, "test_tbl", "_id",
		"some-id",
		map[string]interface{}{"amount": 100},
		`{"amount":100}`, "recon-heal", "deadbeef",
		newerTs,
	)

	// WHERE guard must STILL be present (the guard is identical — it
	// is the DB that decides apply vs. skip based on stored ts). The
	// point of the test is that when ts is valid (>0) we get the ts
	// guard and NOT the hash-fallback.
	if strings.Contains(query, `"_hash" IS DISTINCT FROM`) {
		t.Errorf("unexpected hash fallback in query:\n%s", query)
	}
	if !strings.Contains(query, `"_source_ts" < EXCLUDED."_source_ts"`) {
		t.Errorf("OCC guard missing for newer ts upsert:\n%s", query)
	}
	found := false
	for _, a := range args {
		if v, ok := a.(int64); ok && v == newerTs {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected source ts %d to appear in args", newerTs)
	}
}

// TestExtractSourceTsFromDoc covers the three preference tiers:
// updated_at (time.Time), updated_at (primitive.DateTime), ObjectID ts.
func TestExtractSourceTsFromDoc(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()

	// 1) time.Time path
	if got := extractSourceTsFromDoc(map[string]interface{}{
		"updated_at": now,
	}); got != now.UnixMilli() {
		t.Errorf("time.Time path: got %d, want %d", got, now.UnixMilli())
	}

	// 2) primitive.DateTime path
	dt := primitive.NewDateTimeFromTime(now)
	if got := extractSourceTsFromDoc(map[string]interface{}{
		"updated_at": dt,
	}); got != now.UnixMilli() {
		t.Errorf("primitive.DateTime path: got %d, want %d", got, now.UnixMilli())
	}

	// 3) ObjectID fallback
	oid := primitive.NewObjectIDFromTimestamp(now)
	got := extractSourceTsFromDoc(map[string]interface{}{"_id": oid})
	// ObjectID timestamp has second-level resolution.
	wantSec := now.Unix() * 1000
	if got != wantSec {
		t.Errorf("ObjectID fallback: got %d, want ~%d", got, wantSec)
	}

	// 4) Unknown → 0
	if got := extractSourceTsFromDoc(map[string]interface{}{"something": 1}); got != 0 {
		t.Errorf("unknown doc should return 0, got %d", got)
	}
}

// TestSchemaValidatorDrift — covers the Phase A drift path. We use a
// nil DB to force the "fail-open" probe path, then exercise a drift
// scenario with an explicit expectation set.
func TestSchemaValidatorDriftDetection(t *testing.T) {
	sv := &SchemaValidator{}

	// Simulate cached expectations
	exp := &tableExpectations{
		Required: map[string]struct{}{"user_id": {}},
		Known:    map[string]struct{}{"user_id": {}, "amount": {}},
	}
	sv.cache.Store("t", exp)

	// Happy path
	if err := sv.ValidatePayload("t", map[string]interface{}{
		"user_id": "u1", "amount": 100,
	}); err != nil {
		t.Errorf("expected nil for valid payload, got %v", err)
	}

	// Missing required
	if err := sv.ValidatePayload("t", map[string]interface{}{"amount": 100}); err == nil {
		t.Errorf("expected missing-required error")
	}

	// Unknown field
	err := sv.ValidatePayload("t", map[string]interface{}{
		"user_id": "u1", "amount": 100, "new_field": "x",
	})
	if err == nil {
		t.Errorf("expected schema-drift error")
	} else if !strings.Contains(err.Error(), "unknown_field") {
		t.Errorf("expected unknown_field in error, got: %v", err)
	}
}
