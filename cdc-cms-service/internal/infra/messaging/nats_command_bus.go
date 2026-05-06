// Package messaging — NATS-backed CommandBus (Phase 2 v2 / P3).
//
// nats_command_bus.go is the single C-side dispatcher used by every
// API handler. The bus exposes two methods so callers route through
// the type system, not a runtime switch:
//
//  1. Execute(ctx, SyncCommand)  → SyncResult  — for cheap metadata
//     writes (e.g. ack alert, create mapping rule). Bus persists a
//     cdc_jobs row, runs the registered handler, closes the row, and
//     returns the handler's RawMessage result. No NATS round-trip.
//
//  2. Dispatch(ctx, AsyncCommand) → AsyncResult — for worker-bound
//     heavy work (e.g. master.swap, recon.check, transmute). Bus
//     persists the job row, publishes raw `json.Marshal(cmd)` on the
//     mapped subject + NATS headers carrying job_id / correlation,
//     and returns the JobID. The worker emits cdc.evt.X.completed;
//     JobMonitor closes the row.
//
// Adding a new command = one wiring line at boot. See server.go.
package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/job"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// SyncHandler is the in-process counterpart to a NATS subject. The
// returned RawMessage is stored verbatim on `cdc_jobs.result`.
type SyncHandler interface {
	Handle(ctx context.Context, c ports.Command) (json.RawMessage, error)
}

// ctxKey is unexported to prevent collisions with other packages
// stuffing values into the same context.
type ctxKey int

const (
	keyCreatedBy ctxKey = iota
	keyCorrelationID
	keyIdempotencyKey
)

// WithMetadata stamps dispatch-time metadata onto ctx. Handlers do
// `ctx := messaging.WithMetadata(c.UserContext(), username, traceID, "")`
// before calling bus.Dispatch / bus.Execute.
func WithMetadata(ctx context.Context, createdBy, correlationID, idempotencyKey string) context.Context {
	if createdBy != "" {
		ctx = context.WithValue(ctx, keyCreatedBy, createdBy)
	}
	if correlationID != "" {
		ctx = context.WithValue(ctx, keyCorrelationID, correlationID)
	}
	if idempotencyKey != "" {
		ctx = context.WithValue(ctx, keyIdempotencyKey, idempotencyKey)
	}
	return ctx
}

func ctxString(ctx context.Context, k ctxKey) string {
	if v := ctx.Value(k); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// natsCommandBus is the production ports.CommandBus.
//
// Type→handler/subject lookups are registries (not switches) so a new
// command is one line wired at boot. Sync vs async routing happens at
// the type system level — Execute only accepts SyncCommand, Dispatch
// only accepts AsyncCommand.
type natsCommandBus struct {
	nc       *nats.Conn
	jobRepo  ports.JobRepo
	log      *zap.Logger
	sync     map[string]SyncHandler
	subjects map[string]string
}

// NewNATSCommandBus constructs the bus. nc may be nil in unit tests
// that only exercise sync handlers; Dispatch returns a clear error if
// an async-only command type is invoked without a NATS connection.
func NewNATSCommandBus(nc *nats.Conn, jobRepo ports.JobRepo, log *zap.Logger) *natsCommandBus {
	if log == nil {
		log = zap.NewNop()
	}
	return &natsCommandBus{
		nc:       nc,
		jobRepo:  jobRepo,
		log:      log,
		sync:     make(map[string]SyncHandler),
		subjects: make(map[string]string),
	}
}

// RegisterSync wires an in-process handler for the given command type.
// Calling RegisterSync twice for the same type overwrites the prior
// registration (last-write-wins, matches Go map semantics).
func (b *natsCommandBus) RegisterSync(cmdType string, h SyncHandler) {
	b.sync[cmdType] = h
}

// RegisterSubject maps an async command type to the NATS subject the
// worker subscribes. Bus publishes raw command bytes + headers on
// Dispatch (see buildCommandMsg).
func (b *natsCommandBus) RegisterSubject(cmdType, subject string) {
	b.subjects[cmdType] = subject
}

// natsHeader* keys carry job correlation OUT-OF-BAND so the wire
// payload stays byte-identical to the legacy `natsClient.Publish`
// shape that existing workers (centralized-data-service) parse.
//
// Why headers, not envelope: pre-P3 workers do
// `json.Unmarshal(msg.Data, &payload)` directly into typed structs (see
// recon_handler.go:82 — `{Tier, Table}`). Wrapping the payload in
// `{job_id, payload:{...}}` would leave their fields empty and trigger
// silent "all-tables" fallbacks. Headers reach the same workers as
// inert metadata they ignore today; when JobMonitor lands (T3.6-T3.9)
// workers read the header to emit `cdc.evt.X.completed` with the right
// job_id without changing the on-wire payload contract.
const (
	natsHeaderJobID         = "Cdc-Job-Id"
	natsHeaderCorrelationID = "Cdc-Correlation-Id"
	natsHeaderCreatedBy     = "Cdc-Created-By"
	natsHeaderCommandType   = "Cdc-Command-Type"
)

// Execute runs an in-process sync command and projects the handler's
// result body inline. Used by API handlers that respond 200 with the
// handler's body (e.g. ack alert, create mapping rule).
//
// Flow:
//  1. Validate the command.
//  2. Persist a cdc_jobs row (status=pending). Idempotent retries
//     short-circuit when the row is already non-pending.
//  3. Run the registered SyncHandler.
//  4. Close the row (success or failed) and return the body.
func (b *natsCommandBus) Execute(ctx context.Context, c ports.SyncCommand) (ports.SyncResult, error) {
	j, payload, short, err := b.prepare(ctx, c)
	if err != nil {
		return ports.SyncResult{}, err
	}
	if short {
		// Idempotent hit: prior dispatch already finished. Replay the
		// stored result so the API caller sees the same body.
		return ports.SyncResult{JobID: j.ID, ResultBody: json.RawMessage(j.Result)}, nil
	}
	_ = payload // keep symmetry with Dispatch — payload is on the row.

	h, ok := b.sync[c.Type()]
	if !ok {
		return ports.SyncResult{JobID: j.ID},
			fmt.Errorf("no sync handler for command type %q", c.Type())
	}
	body, herr := b.runSync(ctx, h, c, j)
	if herr != nil {
		return ports.SyncResult{JobID: j.ID}, herr
	}
	return ports.SyncResult{JobID: j.ID, ResultBody: body}, nil
}

// Dispatch publishes an async command to NATS for worker processing.
// Used by API handlers that respond 202 + JobID; the caller polls
// GET /api/jobs/:id to learn the eventual outcome.
//
// Flow:
//  1. Validate the command.
//  2. Persist a cdc_jobs row (status=pending). Idempotent retries
//     short-circuit when the row is already non-pending.
//  3. Publish raw `json.Marshal(cmd)` on the mapped subject + NATS
//     headers carrying job_id / correlation. If publish fails, mark
//     the row failed so it doesn't dangle in pending forever.
func (b *natsCommandBus) Dispatch(ctx context.Context, c ports.AsyncCommand) (ports.AsyncResult, error) {
	j, payload, short, err := b.prepare(ctx, c)
	if err != nil {
		return ports.AsyncResult{}, err
	}
	if short {
		// Idempotent hit: original dispatch already published; do not
		// re-publish (worker may have processed already).
		return ports.AsyncResult{JobID: j.ID, Accepted: true}, nil
	}

	subj, ok := b.subjects[c.Type()]
	if !ok {
		return ports.AsyncResult{JobID: j.ID, Accepted: false},
			fmt.Errorf("no subject registered for command type %q", c.Type())
	}
	if b.nc == nil {
		return ports.AsyncResult{JobID: j.ID, Accepted: false},
			fmt.Errorf("nats not configured for %s", c.Type())
	}
	createdBy := ctxString(ctx, keyCreatedBy)
	if createdBy == "" {
		createdBy = "system"
	}
	correlationID := ctxString(ctx, keyCorrelationID)
	msg := buildCommandMsg(subj, c.Type(), j.ID, correlationID, createdBy, payload)
	if perr := b.nc.PublishMsg(msg); perr != nil {
		_ = b.jobRepo.UpdateStatus(ctx, j.ID, job.StatusFailed, "", "publish: "+perr.Error())
		return ports.AsyncResult{JobID: j.ID, Accepted: false},
			fmt.Errorf("publish %s: %w", subj, perr)
	}
	return ports.AsyncResult{JobID: j.ID, Accepted: true}, nil
}

// prepare validates + persists the job row. Returns:
//   - j: persisted row.
//   - payload: marshaled command bytes (caller may publish to NATS).
//   - short: true when an idempotent retry hit a finished row — the
//     caller should NOT execute or publish again.
//   - err: validation or persistence failure.
func (b *natsCommandBus) prepare(ctx context.Context, c ports.Command) (*job.Job, []byte, bool, error) {
	if c == nil {
		return nil, nil, false, errors.New("command is nil")
	}
	if err := c.Validate(); err != nil {
		return nil, nil, false, fmt.Errorf("validate %s: %w", c.Type(), err)
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, nil, false, fmt.Errorf("marshal %s: %w", c.Type(), err)
	}
	createdBy := ctxString(ctx, keyCreatedBy)
	if createdBy == "" {
		createdBy = "system"
	}
	correlationID := ctxString(ctx, keyCorrelationID)
	j := job.New(c.Type(), payload, createdBy, correlationID)

	// Optional IdempotencyKey: command-level wins; fall back to ctx.
	if k, ok := c.(interface{ IdempotencyKey() string }); ok {
		j.IdempotencyKey = k.IdempotencyKey()
	}
	if j.IdempotencyKey == "" {
		j.IdempotencyKey = ctxString(ctx, keyIdempotencyKey)
	}

	if err := b.jobRepo.Create(ctx, j); err != nil {
		return nil, nil, false, fmt.Errorf("persist job: %w", err)
	}
	short := j.Status != job.StatusPending
	return j, payload, short, nil
}

// runSync executes the in-process handler and closes the job row.
// Returns the handler's RawMessage result (for inline API response)
// plus any error. On error the row is marked failed; the caller sees
// 4xx/5xx immediately.
func (b *natsCommandBus) runSync(ctx context.Context, h SyncHandler, c ports.Command, j *job.Job) (json.RawMessage, error) {
	res, herr := h.Handle(ctx, c)
	if herr != nil {
		if uerr := b.jobRepo.UpdateStatus(ctx, j.ID, job.StatusFailed, "", herr.Error()); uerr != nil {
			b.log.Warn("close failed sync job",
				zap.String("type", c.Type()),
				zap.String("job_id", j.ID),
				zap.Error(uerr))
		}
		return nil, herr
	}
	if uerr := b.jobRepo.UpdateStatus(ctx, j.ID, job.StatusSuccess, string(res), ""); uerr != nil {
		// Handler succeeded but row update failed. Log loudly — the
		// caller will see the body but GET /api/jobs/:id may show
		// stale pending. Self-healing on next /jobs read would need
		// extra wiring; keeping it visible in logs is enough for now.
		b.log.Warn("close success sync job",
			zap.String("type", c.Type()),
			zap.String("job_id", j.ID),
			zap.Error(uerr))
	}
	return res, nil
}

// buildCommandMsg shapes the on-wire NATS message for an async command.
// Extracted as a free function so unit tests can assert
// header/payload contract without spinning a real nats.Conn.
func buildCommandMsg(subject, cmdType, jobID, correlationID, createdBy string, payload []byte) *nats.Msg {
	msg := &nats.Msg{
		Subject: subject,
		Header:  nats.Header{},
		Data:    payload,
	}
	msg.Header.Set(natsHeaderJobID, jobID)
	msg.Header.Set(natsHeaderCommandType, cmdType)
	if correlationID != "" {
		msg.Header.Set(natsHeaderCorrelationID, correlationID)
	}
	if createdBy != "" {
		msg.Header.Set(natsHeaderCreatedBy, createdBy)
	}
	return msg
}

// Compile-time port adherence check.
var _ ports.CommandBus = (*natsCommandBus)(nil)
