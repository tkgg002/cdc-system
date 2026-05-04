package service

import (
	"strings"
	"testing"
)

// TestBuildUpsertSQL_PopulatesGpaySourceID verifies that BuildUpsertSQLInSchema
// correctly populates _gpay_source_id from pkValue when the column exists in
// the schema (V2 tables), and does NOT emit the column for V1 tables that lack
// the column — ensuring backward compatibility.
func TestBuildUpsertSQL_PopulatesGpaySourceID(t *testing.T) {
	sa := &SchemaAdapter{}

	// --- Case 1: V2 schema — _gpay_source_id column present ---
	schemaV2 := &TableSchema{
		PKColumn: "id",
		Columns: map[string]ColumnInfo{
			"id":               {Name: "id", DataType: "text"},
			"user_id":          {Name: "user_id", DataType: "integer"},
			"_raw_data":        {Name: "_raw_data", DataType: "text"},
			"_source":          {Name: "_source", DataType: "text"},
			"_synced_at":       {Name: "_synced_at", DataType: "timestamp"},
			"_hash":            {Name: "_hash", DataType: "text"},
			"_gpay_source_id":  {Name: "_gpay_source_id", DataType: "text"},
		},
	}

	mappedData := map[string]interface{}{
		"user_id": 42,
	}
	pkValue := "42"

	sql, vals := sa.BuildUpsertSQLInSchema(
		schemaV2,
		"shadow_goopay_source", "orders",
		"id", pkValue,
		mappedData,
		`{"raw":"data"}`, "debezium", "hash-abc",
		0,
	)

	// Assert INSERT cols contains _gpay_source_id
	if !strings.Contains(sql, `"_gpay_source_id"`) {
		t.Errorf("V2 schema: expected SQL to contain \"_gpay_source_id\" in INSERT cols.\nSQL: %s", sql)
	}

	// Assert UPDATE clause contains EXCLUDED._gpay_source_id
	if !strings.Contains(sql, `"_gpay_source_id" = EXCLUDED."_gpay_source_id"`) {
		t.Errorf("V2 schema: expected SQL to contain \"_gpay_source_id\" = EXCLUDED.\"_gpay_source_id\" in UPDATE.\nSQL: %s", sql)
	}

	// Assert that pkValue "42" appears in finalValues (as the _gpay_source_id value)
	foundPKInValues := false
	for _, v := range vals {
		if s, ok := v.(string); ok && s == "42" {
			foundPKInValues = true
			break
		}
	}
	if !foundPKInValues {
		t.Errorf("V2 schema: expected pkValue %q to appear in finalValues as _gpay_source_id. Values: %v", pkValue, vals)
	}

	// --- Case 2: V1 schema — _gpay_source_id column NOT present (backward compat) ---
	schemaV1 := &TableSchema{
		PKColumn: "id",
		Columns: map[string]ColumnInfo{
			"id":          {Name: "id", DataType: "text"},
			"user_id":     {Name: "user_id", DataType: "integer"},
			"_raw_data":   {Name: "_raw_data", DataType: "text"},
			"_source":     {Name: "_source", DataType: "text"},
			"_synced_at":  {Name: "_synced_at", DataType: "timestamp"},
			"_hash":       {Name: "_hash", DataType: "text"},
		},
	}

	sqlV1, _ := sa.BuildUpsertSQLInSchema(
		schemaV1,
		"shadow_goopay_source", "orders",
		"id", pkValue,
		mappedData,
		`{"raw":"data"}`, "debezium", "hash-abc",
		0,
	)

	// Assert INSERT cols does NOT contain _gpay_source_id for V1
	if strings.Contains(sqlV1, `"_gpay_source_id"`) {
		t.Errorf("V1 schema: expected SQL to NOT contain \"_gpay_source_id\" (backward compat).\nSQL: %s", sqlV1)
	}
}
