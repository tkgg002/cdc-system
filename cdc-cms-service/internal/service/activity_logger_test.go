// activity_logger_test.go — guard tests for the audit-log helper.
//
// Project convention: this package keeps validation/shape tests in unit
// scope; transactional behaviour ships to deploy-time E2E (no sqlmock /
// no testcontainers in go.sum). The buildRow() pure-fn coverage here is
// what we actually need — it owns the wire shape that downstream
// FE filters depend on (`triggered_by` defaulting, error pointer, JSON
// details). The two nil-receiver tests guard the LogAsync hot path
// from panicking when a wiring oversight hands in a nil logger.
package service

import (
	"context"
	"encoding/json"
	"testing"
)

func TestActivityLogger_NilReceiverIsSafe(t *testing.T) {
	var l *ActivityLogger
	// Must not panic and must not block.
	l.LogAsync(ActivityEntry{Operation: "noop"})
	if err := l.Log(context.Background(), ActivityEntry{Operation: "noop"}); err != nil {
		t.Fatalf("nil Log returned err: %v", err)
	}
	if rows, err := l.ListActivityLogs(context.Background(), ActivityFilter{}); err != nil || rows != nil {
		t.Fatalf("nil ListActivityLogs: rows=%v err=%v", rows, err)
	}
}

func TestActivityLogger_BuildRow_DefaultsTriggeredBy(t *testing.T) {
	l := &ActivityLogger{}
	row := l.buildRow(ActivityEntry{Operation: "x", TargetTable: "y", Status: "ok"})
	if row.TriggeredBy != "manual" {
		t.Fatalf("TriggeredBy default: got %q want manual", row.TriggeredBy)
	}
	if row.ErrorMessage != nil {
		t.Fatalf("ErrorMessage should be nil when ErrorMsg empty; got %v", row.ErrorMessage)
	}
	if row.CompletedAt == nil || row.StartedAt.IsZero() {
		t.Fatalf("timestamps not set: started=%v completed=%v", row.StartedAt, row.CompletedAt)
	}
}

func TestActivityLogger_BuildRow_PreservesError(t *testing.T) {
	l := &ActivityLogger{}
	row := l.buildRow(ActivityEntry{Operation: "x", ErrorMsg: "boom", TriggeredBy: "scheduler"})
	if row.TriggeredBy != "scheduler" {
		t.Fatalf("TriggeredBy override lost: got %q", row.TriggeredBy)
	}
	if row.ErrorMessage == nil || *row.ErrorMessage != "boom" {
		t.Fatalf("ErrorMessage not propagated: got %v", row.ErrorMessage)
	}
}

func TestActivityLogger_BuildRow_DetailsRoundtrip(t *testing.T) {
	l := &ActivityLogger{}
	row := l.buildRow(ActivityEntry{
		Operation: "scan-fields",
		Details:   map[string]any{"user": "alice", "n": 3},
	})
	var got map[string]any
	if err := json.Unmarshal(row.Details, &got); err != nil {
		t.Fatalf("Details not valid JSON: %v (%q)", err, string(row.Details))
	}
	if got["user"] != "alice" {
		t.Fatalf("user lost in roundtrip: %v", got)
	}
}
