package commands

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAckAlertCommand_TypeAndValidate(t *testing.T) {
	if (AckAlertCommand{}).Type() != "alert.ack" {
		t.Fatalf("type")
	}
	cases := []struct {
		name    string
		cmd     AckAlertCommand
		wantErr string
	}{
		{"missing fingerprint", AckAlertCommand{User: "alice"}, "fingerprint required"},
		{"blank fingerprint", AckAlertCommand{Fingerprint: "  ", User: "alice"}, "fingerprint required"},
		{"missing user", AckAlertCommand{Fingerprint: "fp"}, "user required"},
		{"ok", AckAlertCommand{Fingerprint: "fp", User: "alice"}, ""},
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

func TestAckAlertHandler_NilManager(t *testing.T) {
	h := NewAckAlertHandler(nil)
	_, err := h.Handle(context.Background(), AckAlertCommand{Fingerprint: "fp", User: "alice"})
	if err == nil || !strings.Contains(err.Error(), "alert manager not ready") {
		t.Fatalf("expected manager-not-ready, got %v", err)
	}
}

func TestAckAlertHandler_TypeMismatch(t *testing.T) {
	h := NewAckAlertHandler(nil)
	type other struct{}
	_, err := h.Handle(context.Background(), wrongCmd{})
	if err == nil || !errors.Is(err, errCmdTypeMismatch) {
		// Wrapped error: just check the message contains the expected text.
		if err == nil || !strings.Contains(err.Error(), "type mismatch") {
			t.Fatalf("expected type-mismatch, got %v", err)
		}
	}
}

// wrongCmd satisfies ports.Command with the wrong concrete type.
type wrongCmd struct{}

func (wrongCmd) Type() string    { return "alert.ack" }
func (wrongCmd) Validate() error { return nil }

// errCmdTypeMismatch is unused — kept to satisfy errors.Is path; the
// actual handler returns a flat errors.New, so we fall back to string
// matching above.
var errCmdTypeMismatch = errors.New("alert.ack: command type mismatch")

func TestReconCheckCommand(t *testing.T) {
	if (ReconCheckCommand{}).Type() != "recon.check" {
		t.Fatal("type")
	}
	cases := []struct {
		cmd     ReconCheckCommand
		wantErr string
	}{
		{ReconCheckCommand{Tier: "1", Table: "t"}, ""},
		{ReconCheckCommand{Tier: "1"}, "table required"},
		{ReconCheckCommand{Table: "t"}, "tier required"},
	}
	for i, tc := range cases {
		err := tc.cmd.Validate()
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%d: unexpected %v", i, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%d: got %v want %q", i, err, tc.wantErr)
		}
	}
}
