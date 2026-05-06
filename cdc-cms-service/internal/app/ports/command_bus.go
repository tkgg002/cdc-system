package ports

import (
	"context"
	"encoding/json"
)

// Command is the common contract every command struct satisfies. It is
// the parent of SyncCommand and AsyncCommand — handlers and call sites
// always work through one of those two narrower types so the bus can
// route via the type system, not a runtime registry switch.
type Command interface {
	// Type returns a short stable identifier (e.g. "master.swap"). The
	// CommandBus maps this to a NATS subject for async dispatch, or to
	// a registered SyncHandler for in-process execution.
	Type() string

	// Validate checks intrinsic invariants. Cross-cutting auth/rbac
	// happens at the API layer, not here.
	Validate() error
}

// SyncCommand runs in-process via bus.Execute. The bus persists a
// cdc_jobs row, runs the registered SyncHandler, closes the row, and
// returns the handler's RawMessage result for inline API response.
//
// Implementations declare themselves sync by embedding SyncCommandMixin
// from this package. The mixin grants the unexported marker method
// that seals the interface — commands cannot be passed to the wrong
// bus method by accident.
type SyncCommand interface {
	Command
	syncCommandKind()
}

// AsyncCommand runs out-of-process via bus.Dispatch. The bus persists
// a cdc_jobs row, publishes the mapped NATS subject, and returns the
// JobID. The worker emits cdc.evt.X.completed; JobMonitor (worker side)
// closes the row.
//
// Implementations embed AsyncCommandMixin to satisfy the marker.
type AsyncCommand interface {
	Command
	asyncCommandKind()
}

// SyncCommandMixin is the mixin embedded by every SyncCommand struct.
// Zero-size — no JSON output (encoding/json only promotes embedded
// types with exported fields). Embedding grants the unexported marker
// method that seals the SyncCommand interface across package boundaries.
type SyncCommandMixin struct{}

func (SyncCommandMixin) syncCommandKind() {}

// AsyncCommandMixin is the mixin embedded by every AsyncCommand struct.
// See SyncCommandMixin for the rationale.
type AsyncCommandMixin struct{}

func (AsyncCommandMixin) asyncCommandKind() {}

// SyncResult is the response of bus.Execute. ResultBody is the
// SyncHandler's return value verbatim — projected inline by the API
// handler as the HTTP response body.
type SyncResult struct {
	JobID      string
	ResultBody json.RawMessage
}

// AsyncResult is the response of bus.Dispatch. JobID is the UUID of
// the row written to cdc_system.cdc_jobs. Accepted=true means the
// NATS publish succeeded; the caller should poll GET /api/jobs/:id to
// learn the eventual outcome.
type AsyncResult struct {
	JobID    string
	Accepted bool
}

// CommandBus dispatches commands. Two methods give type safety:
//
//	Execute  — in-process sync handlers, returns the handler's body.
//	Dispatch — NATS-published async commands, returns just the JobID.
//
// Idempotent retries land on the same row when the Command exposes an
// IdempotencyKey via the optional `interface{ IdempotencyKey() string }`.
type CommandBus interface {
	Execute(ctx context.Context, c SyncCommand) (SyncResult, error)
	Dispatch(ctx context.Context, c AsyncCommand) (AsyncResult, error)
}
