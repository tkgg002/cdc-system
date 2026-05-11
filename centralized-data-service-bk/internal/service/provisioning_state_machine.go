// Package service — provisioning_state_machine.go
//
// Pure state machine for the Source Provisioning Mode flow
// (workspace: feature-cdc-integration / phase: provisioning_mode).
//
// This file is intentionally I/O-free so it can be exhaustively unit
// tested without a database, NATS connection, or worker boot. The
// orchestrator (provisioning_orchestrator.go) wires this table to
// Postgres + NATS.
//
// Architect rulings (04_decisions_provisioning_mode.md) apply:
//   D4 — `provisioned` is a terminal legacy-only state, NOT in
//        Transitions. Sources backfilled by migration 047 land here.
//   D6 — All UPDATE callers must pair the `From` value below with a
//        WHERE provisioning_state = 'expected' CAS guard.
package service

// ProvisioningState is a typed string so misuse (e.g. comparing to a
// raw literal) shows up at the call site, not inside an UPDATE.
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
	// D4 — terminal state for legacy sources backfilled by migration 047.
	// Distinct from StateRunning so audit/UI can tell them apart.
	StateProvisioned ProvisioningState = "provisioned"
)

// StepDescriptor captures everything Advance() needs in a single row:
// what to log, what to publish, where the row lands while waiting, and
// where it ends after a successful step_completed event.
type StepDescriptor struct {
	Step          string            // canonical step name written into provisioning_step_log
	CmdSubject    string            // NATS subject orchestrator publishes
	NextPending   ProvisioningState // intermediate "*_pending" state after dispatch
	NextOnSuccess ProvisioningState // final state after success event
}

// Transitions is the sole forward-flow source of truth. Adding a step
// = one row here. Anything not in this map is treated as terminal /
// non-advanceable by CanAdvance.
var Transitions = map[ProvisioningState]StepDescriptor{
	StateDraft:        {"shadow_bind", "cdc.cmd.shadow.bind", StateShadowPending, StateShadowActive},
	StateShadowActive: {"master_bind", "cdc.cmd.master.bind", StateMasterPending, StateMasterActive},
	StateMasterActive: {"discover", "cdc.cmd.discover", StateMappingPending, StateMappingReady},
	StateMappingReady: {"schedule_enable", "cdc.cmd.schedule.enable", StateSchedulePending, StateRunning},
}

// PendingToFinalize maps each *_pending back to the final state that
// HandleStepCompleted will write on success. Used as a CAS check
// (current must be one of these to be considered "in flight").
var PendingToFinalize = map[ProvisioningState]ProvisioningState{
	StateShadowPending:   StateShadowActive,
	StateMasterPending:   StateMasterActive,
	StateMappingPending:  StateMappingReady,
	StateSchedulePending: StateRunning,
}

// CanAdvance reports whether a state has a forward transition. Terminal
// states (running, paused, failed, archived, provisioned) and
// in-flight states (*_pending) return false — orchestrator must use
// Resume / Retry / pending-event handling instead.
func CanAdvance(s ProvisioningState) bool {
	_, ok := Transitions[s]
	return ok
}

// IsPending reports whether a state is an in-flight "*_pending" state.
// RecoveryLoop scans these for TTL timeouts (D3).
func IsPending(s ProvisioningState) bool {
	_, ok := PendingToFinalize[s]
	return ok
}

// IsTerminal reports whether a state allows no orchestrator action.
// `paused` and `failed` are NOT terminal — they have explicit Resume /
// Retry actions. `archived` and `provisioned` are terminal.
func IsTerminal(s ProvisioningState) bool {
	switch s {
	case StateArchived, StateProvisioned:
		return true
	default:
		return false
	}
}
