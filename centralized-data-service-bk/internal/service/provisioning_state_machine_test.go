package service

import "testing"

func TestProvisioningStateMachine_FullChainAdvanceable(t *testing.T) {
	advanceable := []ProvisioningState{
		StateDraft, StateShadowActive, StateMasterActive, StateMappingReady,
	}
	for _, s := range advanceable {
		if !CanAdvance(s) {
			t.Fatalf("expected %s advanceable, got false", s)
		}
		desc := Transitions[s]
		if desc.Step == "" || desc.CmdSubject == "" {
			t.Fatalf("state %s has incomplete StepDescriptor: %+v", s, desc)
		}
		if !IsPending(desc.NextPending) {
			t.Fatalf("state %s NextPending=%s not registered in PendingToFinalize",
				s, desc.NextPending)
		}
		if PendingToFinalize[desc.NextPending] != desc.NextOnSuccess {
			t.Fatalf("state %s mismatch: NextOnSuccess=%s but PendingToFinalize[%s]=%s",
				s, desc.NextOnSuccess, desc.NextPending,
				PendingToFinalize[desc.NextPending])
		}
	}
}

func TestProvisioningStateMachine_TerminalNotAdvanceable(t *testing.T) {
	// `running` is reached only after schedule_enable succeeds and is
	// terminal-active for the orchestrator (transmute_scheduler picks it
	// up separately). `paused`/`failed` are NOT terminal — explicit
	// Resume/Retry handle them — but they're not advanceable either.
	notAdvanceable := []ProvisioningState{
		StateRunning, StatePaused, StateFailed, StateArchived, StateProvisioned,
		StateShadowPending, StateMasterPending, StateMappingPending, StateSchedulePending,
	}
	for _, s := range notAdvanceable {
		if CanAdvance(s) {
			t.Fatalf("state %s must not be advanceable", s)
		}
	}
}

func TestProvisioningStateMachine_PendingMappings(t *testing.T) {
	if len(PendingToFinalize) != 4 {
		t.Fatalf("expected exactly 4 pending->final mappings (D2 plan §P4), got %d",
			len(PendingToFinalize))
	}
	expected := map[ProvisioningState]ProvisioningState{
		StateShadowPending:   StateShadowActive,
		StateMasterPending:   StateMasterActive,
		StateMappingPending:  StateMappingReady,
		StateSchedulePending: StateRunning,
	}
	for k, v := range expected {
		if got := PendingToFinalize[k]; got != v {
			t.Fatalf("PendingToFinalize[%s]=%s, expected %s", k, got, v)
		}
	}
}

func TestProvisioningStateMachine_IsTerminal(t *testing.T) {
	terminal := []ProvisioningState{StateArchived, StateProvisioned}
	for _, s := range terminal {
		if !IsTerminal(s) {
			t.Fatalf("expected %s terminal", s)
		}
	}
	notTerminal := []ProvisioningState{
		StateDraft, StateRunning, StatePaused, StateFailed,
		StateShadowPending, StateShadowActive,
	}
	for _, s := range notTerminal {
		if IsTerminal(s) {
			t.Fatalf("expected %s NOT terminal", s)
		}
	}
}

func TestProvisioningStateMachine_NoOrphanTransitionTargets(t *testing.T) {
	// Every NextPending value must appear as a key in PendingToFinalize
	// AND every NextOnSuccess must either be advanceable or a known
	// terminal-active state. Catches typos when extending the table.
	knownTerminalActive := map[ProvisioningState]bool{
		StateRunning:     true,
		StateProvisioned: true, // legacy-only, not reached by Transitions
	}
	for s, desc := range Transitions {
		if _, ok := PendingToFinalize[desc.NextPending]; !ok {
			t.Errorf("state %s NextPending=%s missing from PendingToFinalize", s, desc.NextPending)
		}
		if !CanAdvance(desc.NextOnSuccess) && !knownTerminalActive[desc.NextOnSuccess] {
			t.Errorf("state %s NextOnSuccess=%s is neither advanceable nor terminal-active",
				s, desc.NextOnSuccess)
		}
	}
}
