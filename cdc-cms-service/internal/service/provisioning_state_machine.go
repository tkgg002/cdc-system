// Package service — provisioning_state_machine.go (CMS port)
//
// CMS-side copy of the pure state machine that lives in
// `centralized-data-service/internal/service/provisioning_state_machine.go`.
//
// The two services are separate Go modules with no `replace` directive,
// so the file is duplicated rather than imported. Both copies MUST stay
// byte-equivalent (modulo this header) — edit one, edit both. CAS
// guarantees correctness even if the two copies briefly diverge: the
// DB is the only source of truth for state.
//
// Architect rulings (workspace feature-cdc-integration /
// 04_decisions_provisioning_mode.md) apply identically:
//   D4 — `provisioned` is terminal legacy-only, NOT in Transitions.
//   D6 — All UPDATE callers must pair the `From` value with a
//        WHERE provisioning_state = 'expected' CAS guard.
package service

type ProvisioningState string

const (
	StateDraft           ProvisioningState = "draft"
	StateShadowPending   ProvisioningState = "shadow_pending"
	StateShadowActive    ProvisioningState = "shadow_active"
	StateMasterPending   ProvisioningState = "master_pending"
	StateMasterActive    ProvisioningState = "master_active"
	StateMappingPending  ProvisioningState = "mapping_pending"
	StateMappingReady    ProvisioningState = "mapping_ready"
	StateSchedulePending ProvisioningState = "schedule_pending"
	StateRunning         ProvisioningState = "running"
	StatePaused          ProvisioningState = "paused"
	StateFailed          ProvisioningState = "failed"
	StateArchived        ProvisioningState = "archived"
	StateProvisioned     ProvisioningState = "provisioned" // D4 — legacy backfill terminal
)

type StepDescriptor struct {
	Step          string
	CmdSubject    string
	NextPending   ProvisioningState
	NextOnSuccess ProvisioningState
}

var ProvisioningTransitions = map[ProvisioningState]StepDescriptor{
	StateDraft:        {"shadow_bind", "cdc.cmd.shadow.bind", StateShadowPending, StateShadowActive},
	StateShadowActive: {"master_bind", "cdc.cmd.master.bind", StateMasterPending, StateMasterActive},
	StateMasterActive: {"discover", "cdc.cmd.discover", StateMappingPending, StateMappingReady},
	StateMappingReady: {"schedule_enable", "cdc.cmd.schedule.enable", StateSchedulePending, StateRunning},
}

var ProvisioningPendingToFinalize = map[ProvisioningState]ProvisioningState{
	StateShadowPending:   StateShadowActive,
	StateMasterPending:   StateMasterActive,
	StateMappingPending:  StateMappingReady,
	StateSchedulePending: StateRunning,
}

func ProvisioningCanAdvance(s ProvisioningState) bool {
	_, ok := ProvisioningTransitions[s]
	return ok
}

func ProvisioningIsPending(s ProvisioningState) bool {
	_, ok := ProvisioningPendingToFinalize[s]
	return ok
}

func ProvisioningIsTerminal(s ProvisioningState) bool {
	switch s {
	case StateArchived, StateProvisioned:
		return true
	default:
		return false
	}
}
