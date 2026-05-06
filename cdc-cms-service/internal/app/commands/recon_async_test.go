package commands

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReconHealCommand(t *testing.T) {
	if (ReconHealCommand{}).Type() != "recon.heal" {
		t.Fatal("type")
	}
	if err := (ReconHealCommand{}).Validate(); err == nil {
		t.Fatal("expected table required")
	}
	if err := (ReconHealCommand{Table: "orders"}).Validate(); err != nil {
		t.Fatalf("ok case: %v", err)
	}
	// Wire shape pinned: only `table`.
	b, _ := json.Marshal(ReconHealCommand{Table: "orders"})
	if got := string(b); got != `{"table":"orders"}` {
		t.Fatalf("wire=%s", got)
	}
}

func TestRetryFailedCommand_Validate(t *testing.T) {
	if (RetryFailedCommand{}).Type() != "recon.retry-failed" {
		t.Fatal("type")
	}
	cases := []struct {
		name    string
		cmd     RetryFailedCommand
		wantErr string
	}{
		{"no id", RetryFailedCommand{TargetTable: "t"}, "failed_log_id required"},
		{"no target", RetryFailedCommand{FailedLogID: 1}, "target_table required"},
		{"ok", RetryFailedCommand{FailedLogID: 1, TargetTable: "t"}, ""},
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

func TestRetryFailedCommand_NullableScopeWire(t *testing.T) {
	// Pre-P3 builder used `stringOrNil(scope.X)` — `*string` nil → JSON
	// null. Verify struct field nil mirrors that exactly.
	c := RetryFailedCommand{FailedLogID: 7, TargetTable: "t", RecordID: "r1", RawJSON: "{}"}
	b, _ := json.Marshal(c)
	got := string(b)
	for _, want := range []string{
		`"source_database":null`,
		`"source_table":null`,
		`"shadow_schema":null`,
		`"shadow_table":null`,
		`"scope_ambiguous":false`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("wire missing %q in %s", want, got)
		}
	}
}

func TestDebeziumSignalCommand(t *testing.T) {
	if (DebeziumSignalCommand{}).Type() != "debezium.signal" {
		t.Fatal("type")
	}
	if err := (DebeziumSignalCommand{}).Validate(); err == nil {
		t.Fatal("expected type required")
	}
	cmd := DebeziumSignalCommand{Type_: "signal-snapshot", Database: "db", Collection: "c"}
	if err := cmd.Validate(); err != nil {
		t.Fatalf("ok: %v", err)
	}
	b, _ := json.Marshal(cmd)
	if got := string(b); got != `{"type":"signal-snapshot","database":"db","collection":"c"}` {
		t.Fatalf("wire=%s", got)
	}
}

func TestDebeziumSnapshotCommand(t *testing.T) {
	if (DebeziumSnapshotCommand{}).Type() != "debezium.snapshot" {
		t.Fatal("type")
	}
	if err := (DebeziumSnapshotCommand{}).Validate(); err == nil {
		t.Fatal("expected table required")
	}
	if err := (DebeziumSnapshotCommand{Table: "t"}).Validate(); err != nil {
		t.Fatalf("ok: %v", err)
	}
}

func TestReconBackfillSourceTsCommand(t *testing.T) {
	if (ReconBackfillSourceTsCommand{}).Type() != "recon.backfill-source-ts" {
		t.Fatal("type")
	}
	if err := (ReconBackfillSourceTsCommand{}).Validate(); err == nil {
		t.Fatal("expected run_id required")
	}
	if err := (ReconBackfillSourceTsCommand{RunID: "u"}).Validate(); err != nil {
		t.Fatalf("ok empty table: %v", err)
	}
	// Empty Table is the legitimate "all tables" fallback dispatched by
	// reconciliation_handler.TriggerBackfillSourceTs when body is empty.
	b, _ := json.Marshal(ReconBackfillSourceTsCommand{RunID: "u-1", BatchSize: 250})
	got := string(b)
	if !strings.Contains(got, `"table":""`) || !strings.Contains(got, `"run_id":"u-1"`) || !strings.Contains(got, `"batch_size":250`) {
		t.Fatalf("wire=%s", got)
	}
}
