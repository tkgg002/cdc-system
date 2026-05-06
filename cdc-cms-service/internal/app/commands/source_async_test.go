package commands

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCreateDefaultColumnsCommand(t *testing.T) {
	if (CreateDefaultColumnsCommand{}).Type() != "source.create-default-columns" {
		t.Fatal("type")
	}
	if err := (CreateDefaultColumnsCommand{}).Validate(); err == nil {
		t.Fatal("expected target_table required")
	}
	if err := (CreateDefaultColumnsCommand{TargetTable: "t"}).Validate(); err == nil {
		t.Fatal("expected registry_id or source_object_id required")
	}
	// V1 publisher path: registry_id only.
	if err := (CreateDefaultColumnsCommand{TargetTable: "t", RegistryID: 1}).Validate(); err != nil {
		t.Errorf("V1 path: %v", err)
	}
	// V2 publisher path: source_object_id only.
	if err := (CreateDefaultColumnsCommand{TargetTable: "t", SourceObjectID: 99}).Validate(); err != nil {
		t.Errorf("V2 path: %v", err)
	}
	// omitempty trims null fields when sender doesn't populate them.
	b, _ := json.Marshal(CreateDefaultColumnsCommand{
		RegistryID: 5, TargetTable: "orders",
		SourceTable: "src_orders", PrimaryKeyField: "id", PrimaryKeyType: "BIGINT",
	})
	got := string(b)
	if strings.Contains(got, "source_object_id") || strings.Contains(got, "shadow_schema") {
		t.Errorf("V1 wire should omit source_object_id/shadow_schema: %s", got)
	}
	if !strings.Contains(got, `"registry_id":5`) {
		t.Errorf("V1 wire missing registry_id: %s", got)
	}
}

func TestStandardizeCommand(t *testing.T) {
	if (StandardizeCommand{}).Type() != "source.standardize" {
		t.Fatal("type")
	}
	if err := (StandardizeCommand{}).Validate(); err == nil {
		t.Fatal("target_table required")
	}
	if err := (StandardizeCommand{TargetTable: "t"}).Validate(); err != nil {
		t.Errorf("ok: %v", err)
	}
}

func TestScanFieldsCommand(t *testing.T) {
	if (ScanFieldsCommand{}).Type() != "source.scan-fields" {
		t.Fatal("type")
	}
	if err := (ScanFieldsCommand{TargetTable: "t"}).Validate(); err != nil {
		t.Errorf("ok: %v", err)
	}
}

func TestDetectTimestampFieldCommand(t *testing.T) {
	if (DetectTimestampFieldCommand{}).Type() != "source.detect-timestamp-field" {
		t.Fatal("type")
	}
	if err := (DetectTimestampFieldCommand{TargetTable: "t"}).Validate(); err != nil {
		t.Errorf("ok: %v", err)
	}
}

func TestAlterColumnCommand(t *testing.T) {
	if (AlterColumnCommand{}).Type() != "mapping.alter-column" {
		t.Fatal("type")
	}
	cases := []struct {
		name    string
		cmd     AlterColumnCommand
		wantErr string
	}{
		{"no table", AlterColumnCommand{ColumnName: "c", Action: "ADD"}, "target_table required"},
		{"no col", AlterColumnCommand{TargetTable: "t", Action: "ADD"}, "column_name required"},
		{"no action", AlterColumnCommand{TargetTable: "t", ColumnName: "c"}, "action required"},
		{"ok", AlterColumnCommand{TargetTable: "t", ColumnName: "c", Action: "ADD", DataType: "TEXT"}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}

func TestBackfillCommand(t *testing.T) {
	if (BackfillCommand{}).Type() != "mapping.backfill" {
		t.Fatal("type")
	}
	if err := (BackfillCommand{TargetTable: "t"}).Validate(); err == nil {
		t.Fatal("target_column required")
	}
	if err := (BackfillCommand{TargetTable: "t", TargetColumn: "c"}).Validate(); err != nil {
		t.Errorf("ok: %v", err)
	}
}
