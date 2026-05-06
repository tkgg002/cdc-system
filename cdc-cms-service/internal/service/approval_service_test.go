// approval_service_test.go — guard tests for the request shapes
// + small pure helpers. The Approve/Reject DB+NATS path stays in
// deploy-time E2E (project convention: no sqlmock).
//
// We pin the JSON wire contract here because the FE schema-approval
// modal posts ApproveRequest verbatim; field name drift (e.g.
// renaming target_column_name) silently breaks the form.
package service

import (
	"encoding/json"
	"testing"
)

func TestApproveRequest_JSONFieldNames(t *testing.T) {
	in := ApproveRequest{
		TargetColumnName: "loyalty_points",
		FinalType:        "INTEGER",
		ApprovalNotes:    "approved",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["target_column_name"] != "loyalty_points" {
		t.Errorf("target_column_name field name drifted: %v", got)
	}
	if got["final_type"] != "INTEGER" {
		t.Errorf("final_type field name drifted: %v", got)
	}
	if got["approval_notes"] != "approved" {
		t.Errorf("approval_notes field name drifted: %v", got)
	}
}

func TestRejectRequest_JSONFieldNames(t *testing.T) {
	raw, err := json.Marshal(RejectRequest{RejectionReason: "not for MVP"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	if got["rejection_reason"] != "not for MVP" {
		t.Errorf("rejection_reason field drifted: %v", got)
	}
}

func TestStrPtr(t *testing.T) {
	p := strPtr("hello")
	if p == nil || *p != "hello" {
		t.Errorf("strPtr lost value: %v", p)
	}
	// Nil-vs-empty-string distinction matters for *string DB columns.
	empty := strPtr("")
	if empty == nil {
		t.Error("strPtr(\"\") must return a non-nil pointer to empty string")
	}
	if *empty != "" {
		t.Errorf("strPtr(\"\") deref: %q", *empty)
	}
}
