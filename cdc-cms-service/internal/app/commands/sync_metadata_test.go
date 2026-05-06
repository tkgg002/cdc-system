// sync_metadata_test.go — guard tests for the T3.4 sync metadata
// commands (Create/Update mapping rule, Create/Reject master, Create/
// Patch wizard). Focus on Type() correctness + Validate() boundaries +
// nil-dep guards. End-to-end DB-touching paths are deferred to the API
// integration suite.
package commands

import (
	"context"
	"strings"
	"testing"
)

// ---------- UpdateMappingRule ----------

func TestUpdateMappingRule_TypeAndValidate(t *testing.T) {
	if (UpdateMappingRuleCommand{}).Type() != "mapping.update-status" {
		t.Fatalf("type")
	}
	cases := []struct {
		name    string
		cmd     UpdateMappingRuleCommand
		wantErr string
	}{
		{"missing id", UpdateMappingRuleCommand{Status: "approved"}, "id required"},
		{"missing status", UpdateMappingRuleCommand{ID: 1}, "status required"},
		{"ok", UpdateMappingRuleCommand{ID: 1, Status: "approved"}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}

func TestUpdateMappingRuleHandler_TypeMismatch(t *testing.T) {
	h := NewUpdateMappingRuleHandler(nil, nil, nil)
	_, err := h.Handle(context.Background(), wrongCmd{})
	if err == nil || !strings.Contains(err.Error(), "type mismatch") {
		t.Fatalf("expected type mismatch, got %v", err)
	}
}

// ---------- CreateMappingRule ----------

func TestCreateMappingRule_TypeAndValidate(t *testing.T) {
	if (CreateMappingRuleCommand{}).Type() != "mapping.create" {
		t.Fatalf("type")
	}
	cases := []struct {
		name    string
		cmd     CreateMappingRuleCommand
		wantErr string
	}{
		{"missing source_field", CreateMappingRuleCommand{TargetColumn: "tc", DataType: "TEXT"}, "required"},
		{"missing target_column", CreateMappingRuleCommand{SourceField: "sf", DataType: "TEXT"}, "required"},
		{"missing data_type", CreateMappingRuleCommand{SourceField: "sf", TargetColumn: "tc"}, "required"},
		{"ok", CreateMappingRuleCommand{SourceField: "sf", TargetColumn: "tc", DataType: "TEXT"}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}

// ---------- RejectMaster ----------

func TestRejectMaster_TypeAndValidate(t *testing.T) {
	if (RejectMasterCommand{}).Type() != "master.reject" {
		t.Fatalf("type")
	}
	cases := []struct {
		name    string
		cmd     RejectMasterCommand
		wantErr string
	}{
		{"missing name", RejectMasterCommand{Reason: "abcdefghij"}, "invalid_master_name"},
		{"short reason", RejectMasterCommand{Name: "users", Reason: "short"}, "reason_required"},
		{"ok", RejectMasterCommand{Name: "users", Reason: "this is a valid reason"}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}

// ---------- CreateMaster ----------

func TestCreateMaster_TypeAndValidate(t *testing.T) {
	if (CreateMasterCommand{}).Type() != "master.create" {
		t.Fatalf("type")
	}
	cases := []struct {
		name    string
		cmd     CreateMasterCommand
		wantErr string
	}{
		{"bad master name", CreateMasterCommand{MasterName: "Bad Name", TransformType: "filter", Reason: "this is fine reason"}, "invalid_master_name"},
		{"bad transform", CreateMasterCommand{MasterName: "users", TransformType: "weird", Reason: "this is fine reason"}, "invalid_transform_type"},
		{"short reason", CreateMasterCommand{MasterName: "users", TransformType: "filter", Reason: "short"}, "reason_required"},
		{"bad schema", CreateMasterCommand{MasterName: "users", MasterSchema: "BadSchema!", TransformType: "filter", Reason: "this is fine reason"}, "invalid_master_schema"},
		{"ok", CreateMasterCommand{MasterName: "users", TransformType: "filter", Reason: "this is fine reason"}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}

// ---------- CreateWizard ----------

func TestCreateWizard_TypeAndValidate(t *testing.T) {
	if (CreateWizardCommand{}).Type() != "wizard.create" {
		t.Fatalf("type")
	}
	// Validate is permissive — empty body is allowed (FE may post {}).
	if err := (CreateWizardCommand{}).Validate(); err != nil {
		t.Errorf("expected nil err, got %v", err)
	}
}

// ---------- PatchWizard ----------

func TestPatchWizard_TypeAndValidate(t *testing.T) {
	if (PatchWizardCommand{}).Type() != "wizard.patch" {
		t.Fatalf("type")
	}
	statusDraft := "draft"
	statusBogus := "weird"
	step := 2
	cases := []struct {
		name    string
		cmd     PatchWizardCommand
		wantErr string
	}{
		{"missing id", PatchWizardCommand{Status: &statusDraft}, "id required"},
		{"bad status", PatchWizardCommand{ID: "u1", Status: &statusBogus}, "invalid status"},
		{"empty patch", PatchWizardCommand{ID: "u1"}, "nothing to update"},
		{"ok status", PatchWizardCommand{ID: "u1", Status: &statusDraft}, ""},
		{"ok step", PatchWizardCommand{ID: "u1", CurrentStep: &step}, ""},
	}
	for _, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected err %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: got %v want %q", tc.name, err, tc.wantErr)
		}
	}
}
