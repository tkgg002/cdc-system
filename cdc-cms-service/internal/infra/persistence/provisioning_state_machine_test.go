// provisioning_state_machine_test.go — pure-fn guard tests.
//
// Project convention: validation/shape tests in unit scope; the CAS
// transition behaviour itself is owned by the centralized-data-service
// integration suite. The three predicates here are the wire contract
// the orchestrator depends on (CanAdvance gates command publish,
// IsPending gates finalize sweep, IsTerminal gates archive UI).
//
// Critical invariant: the two `_provisioning_state_machine.go` copies
// (cms + centralized-data-service) MUST agree on these predicates —
// these tests pin the cms side.
package persistence

import "testing"

func TestProvisioningCanAdvance(t *testing.T) {
	cases := map[ProvisioningState]bool{
		StateDraft:           true,
		StateShadowActive:    true,
		StateMasterActive:    true,
		StateMappingReady:    true,
		StateShadowPending:   false, // pending — finalize first
		StateMasterPending:   false,
		StateMappingPending:  false,
		StateSchedulePending: false,
		StateRunning:         false, // terminal-ish for the cron loop
		StatePaused:          false,
		StateFailed:          false,
		StateArchived:        false,
		StateProvisioned:     false, // D4 legacy terminal
	}
	for s, want := range cases {
		if got := ProvisioningCanAdvance(s); got != want {
			t.Errorf("CanAdvance(%q): got %v want %v", s, got, want)
		}
	}
}

func TestProvisioningIsPending(t *testing.T) {
	pending := []ProvisioningState{
		StateShadowPending, StateMasterPending,
		StateMappingPending, StateSchedulePending,
	}
	for _, s := range pending {
		if !ProvisioningIsPending(s) {
			t.Errorf("IsPending(%q): want true", s)
		}
	}
	notPending := []ProvisioningState{
		StateDraft, StateShadowActive, StateRunning,
		StatePaused, StateFailed, StateArchived, StateProvisioned,
	}
	for _, s := range notPending {
		if ProvisioningIsPending(s) {
			t.Errorf("IsPending(%q): want false", s)
		}
	}
}

func TestProvisioningIsTerminal(t *testing.T) {
	if !ProvisioningIsTerminal(StateArchived) {
		t.Error("Archived must be terminal")
	}
	if !ProvisioningIsTerminal(StateProvisioned) {
		t.Error("Provisioned must be terminal (D4 legacy)")
	}
	for _, s := range []ProvisioningState{StateDraft, StateRunning, StateFailed, StatePaused} {
		if ProvisioningIsTerminal(s) {
			t.Errorf("IsTerminal(%q): want false", s)
		}
	}
}

func TestProvisioningTransitions_PendingTargetsMatchFinalizer(t *testing.T) {
	// Every (advance) NextPending must round-trip through the
	// finalize map back to the same NextOnSuccess. Catches drift
	// between the two maps when someone adds a new step.
	for from, step := range ProvisioningTransitions {
		final, ok := ProvisioningPendingToFinalize[step.NextPending]
		if !ok {
			t.Errorf("%q advance → %q has no finalize entry", from, step.NextPending)
			continue
		}
		if final != step.NextOnSuccess {
			t.Errorf("%q: finalize %q → %q, expected %q",
				from, step.NextPending, final, step.NextOnSuccess)
		}
	}
}
