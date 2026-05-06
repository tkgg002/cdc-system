package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/job"
)

// stubJobRepo is the smallest port impl that lets us assert the bus
// flow without touching Postgres. Every method captures its inputs so
// tests can verify ordering + idempotency hits.
type stubJobRepo struct {
	createCalls  int
	createInput  *job.Job
	createReturn func(*job.Job) error // hook for idempotent rehydrate

	updates []stubUpdate
}

type stubUpdate struct {
	id     string
	status job.Status
	result string
	errMsg string
}

func (s *stubJobRepo) Create(_ context.Context, j *job.Job) error {
	s.createCalls++
	s.createInput = j
	if s.createReturn != nil {
		return s.createReturn(j)
	}
	if j.ID == "" {
		j.ID = "stub-uuid-1"
	}
	return nil
}

func (s *stubJobRepo) GetByID(context.Context, string) (*job.Job, error) {
	return nil, errors.New("not used")
}

func (s *stubJobRepo) UpdateStatus(_ context.Context, id string, st job.Status, result, errMsg string) error {
	s.updates = append(s.updates, stubUpdate{id, st, result, errMsg})
	return nil
}

func (s *stubJobRepo) ListPending(context.Context, string, int) ([]job.Job, error) {
	return nil, nil
}

// fakeSyncCmd is a minimal SyncCommand for bus.Execute tests.
type fakeSyncCmd struct {
	ports.SyncCommandMixin
	T   string
	Err error
	Idk string
}

func (c fakeSyncCmd) Type() string                 { return c.T }
func (c fakeSyncCmd) Validate() error              { return c.Err }
func (c fakeSyncCmd) IdempotencyKey() string       { return c.Idk }
func (c fakeSyncCmd) MarshalJSON() ([]byte, error) { return []byte(`{"k":"v"}`), nil }

// fakeAsyncCmd is a minimal AsyncCommand for bus.Dispatch tests.
type fakeAsyncCmd struct {
	ports.AsyncCommandMixin
	T   string
	Err error
	Idk string
}

func (c fakeAsyncCmd) Type() string                 { return c.T }
func (c fakeAsyncCmd) Validate() error              { return c.Err }
func (c fakeAsyncCmd) IdempotencyKey() string       { return c.Idk }
func (c fakeAsyncCmd) MarshalJSON() ([]byte, error) { return []byte(`{"k":"v"}`), nil }

type stubSyncHandler struct {
	called int
	body   json.RawMessage
	err    error
}

func (s *stubSyncHandler) Handle(context.Context, ports.Command) (json.RawMessage, error) {
	s.called++
	return s.body, s.err
}

func TestDispatch_ValidationError(t *testing.T) {
	repo := &stubJobRepo{}
	bus := NewNATSCommandBus(nil, repo, nil)
	_, err := bus.Dispatch(context.Background(), fakeAsyncCmd{T: "x", Err: errors.New("bad")})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if repo.createCalls != 0 {
		t.Fatalf("validate should fail before persist, calls=%d", repo.createCalls)
	}
}

func TestDispatch_NilCommand(t *testing.T) {
	bus := NewNATSCommandBus(nil, &stubJobRepo{}, nil)
	if _, err := bus.Dispatch(context.Background(), nil); err == nil {
		t.Fatal("expected error on nil command")
	}
}

func TestExecute_NilCommand(t *testing.T) {
	bus := NewNATSCommandBus(nil, &stubJobRepo{}, nil)
	if _, err := bus.Execute(context.Background(), nil); err == nil {
		t.Fatal("expected error on nil command")
	}
}

func TestExecute_SyncHappyPath(t *testing.T) {
	repo := &stubJobRepo{}
	bus := NewNATSCommandBus(nil, repo, nil)
	h := &stubSyncHandler{body: json.RawMessage(`{"ok":true}`)}
	bus.RegisterSync("test.sync", h)

	res, err := bus.Execute(context.Background(), fakeSyncCmd{T: "test.sync"})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	if h.called != 1 {
		t.Fatalf("handler calls=%d want 1", h.called)
	}
	if string(res.ResultBody) != `{"ok":true}` {
		t.Fatalf("ResultBody=%s", res.ResultBody)
	}
	if got := repo.updates; len(got) != 1 || got[0].status != job.StatusSuccess {
		t.Fatalf("expected one Success update, got %+v", got)
	}
}

func TestExecute_SyncHandlerError(t *testing.T) {
	repo := &stubJobRepo{}
	bus := NewNATSCommandBus(nil, repo, nil)
	h := &stubSyncHandler{err: errors.New("boom")}
	bus.RegisterSync("test.sync", h)

	_, err := bus.Execute(context.Background(), fakeSyncCmd{T: "test.sync"})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected handler err, got %v", err)
	}
	if got := repo.updates; len(got) != 1 || got[0].status != job.StatusFailed {
		t.Fatalf("expected Failed update, got %+v", got)
	}
}

func TestExecute_NoHandler(t *testing.T) {
	bus := NewNATSCommandBus(nil, &stubJobRepo{}, nil)
	if _, err := bus.Execute(context.Background(), fakeSyncCmd{T: "unknown"}); err == nil {
		t.Fatal("expected no-handler error")
	}
}

func TestDispatch_AsyncWithoutNATS(t *testing.T) {
	bus := NewNATSCommandBus(nil, &stubJobRepo{}, nil)
	bus.RegisterSubject("test.async", "cdc.cmd.test")
	if _, err := bus.Dispatch(context.Background(), fakeAsyncCmd{T: "test.async"}); err == nil {
		t.Fatal("expected nats-not-configured error")
	}
}

func TestDispatch_NoSubject(t *testing.T) {
	bus := NewNATSCommandBus(nil, &stubJobRepo{}, nil)
	if _, err := bus.Dispatch(context.Background(), fakeAsyncCmd{T: "unknown"}); err == nil {
		t.Fatal("expected no-subject error")
	}
}

func TestExecute_IdempotentReturnsExisting(t *testing.T) {
	repo := &stubJobRepo{
		createReturn: func(j *job.Job) error {
			// Simulate idempotency hit: existing row already success.
			j.ID = "existing-uuid"
			j.Status = job.StatusSuccess
			j.Result = json.RawMessage(`{"replay":true}`)
			return nil
		},
	}
	bus := NewNATSCommandBus(nil, repo, nil)
	h := &stubSyncHandler{}
	bus.RegisterSync("test.sync", h)

	res, err := bus.Execute(context.Background(), fakeSyncCmd{T: "test.sync", Idk: "k1"})
	if err != nil {
		t.Fatalf("execute err: %v", err)
	}
	if res.JobID != "existing-uuid" {
		t.Fatalf("idempotent res=%+v", res)
	}
	if h.called != 0 {
		t.Fatalf("handler should not run on idempotent hit, calls=%d", h.called)
	}
	if string(res.ResultBody) != `{"replay":true}` {
		t.Fatalf("idempotent should replay stored body, got %s", res.ResultBody)
	}
}

func TestDispatch_IdempotentReturnsExisting(t *testing.T) {
	repo := &stubJobRepo{
		createReturn: func(j *job.Job) error {
			j.ID = "existing-uuid"
			j.Status = job.StatusSuccess
			return nil
		},
	}
	bus := NewNATSCommandBus(nil, repo, nil)
	bus.RegisterSubject("test.async", "cdc.cmd.test")

	res, err := bus.Dispatch(context.Background(), fakeAsyncCmd{T: "test.async", Idk: "k1"})
	if err != nil {
		t.Fatalf("dispatch err: %v", err)
	}
	if res.JobID != "existing-uuid" || !res.Accepted {
		t.Fatalf("idempotent res=%+v", res)
	}
}

func TestBuildCommandMsg_RawPayloadAndHeaders(t *testing.T) {
	// Wire payload MUST equal the raw marshaled command bytes (legacy
	// worker contract). Correlation/job_id ride headers — invisible to
	// pre-P3 workers, machine-readable for JobMonitor (T3.6-T3.9).
	raw := []byte(`{"tier":"1","table":"orders"}`)
	msg := buildCommandMsg("cdc.cmd.recon-check", "recon.check", "job-uuid-1", "trace-9", "alice", raw)

	if msg.Subject != "cdc.cmd.recon-check" {
		t.Fatalf("subject=%q", msg.Subject)
	}
	if string(msg.Data) != string(raw) {
		t.Fatalf("data=%s want raw payload", msg.Data)
	}
	if got := msg.Header.Get("Cdc-Job-Id"); got != "job-uuid-1" {
		t.Errorf("Cdc-Job-Id=%q", got)
	}
	if got := msg.Header.Get("Cdc-Command-Type"); got != "recon.check" {
		t.Errorf("Cdc-Command-Type=%q", got)
	}
	if got := msg.Header.Get("Cdc-Correlation-Id"); got != "trace-9" {
		t.Errorf("Cdc-Correlation-Id=%q", got)
	}
	if got := msg.Header.Get("Cdc-Created-By"); got != "alice" {
		t.Errorf("Cdc-Created-By=%q", got)
	}
}

func TestBuildCommandMsg_OmitsEmptyOptionalHeaders(t *testing.T) {
	msg := buildCommandMsg("cdc.cmd.test", "test.t", "j1", "", "", []byte(`{}`))
	if msg.Header.Get("Cdc-Job-Id") != "j1" {
		t.Errorf("required job-id missing")
	}
	if msg.Header.Get("Cdc-Command-Type") != "test.t" {
		t.Errorf("required cmd-type missing")
	}
	if _, ok := msg.Header["Cdc-Correlation-Id"]; ok {
		t.Errorf("Cdc-Correlation-Id should be omitted when empty")
	}
	if _, ok := msg.Header["Cdc-Created-By"]; ok {
		t.Errorf("Cdc-Created-By should be omitted when empty")
	}
}

func TestWithMetadata(t *testing.T) {
	ctx := WithMetadata(context.Background(), "alice", "trace-1", "idem-9")
	if got := ctxString(ctx, keyCreatedBy); got != "alice" {
		t.Errorf("createdBy=%q", got)
	}
	if got := ctxString(ctx, keyCorrelationID); got != "trace-1" {
		t.Errorf("correlationID=%q", got)
	}
	if got := ctxString(ctx, keyIdempotencyKey); got != "idem-9" {
		t.Errorf("idempotencyKey=%q", got)
	}
	if got := ctxString(context.Background(), keyCreatedBy); got != "" {
		t.Errorf("empty ctx should yield empty: %q", got)
	}
}
