// master_swap_test.go — guard tests for the validation gate that
// fronts SwapAsync. The two-RENAME TX itself + the Postgres-only
// `payload->>'master_name'` partial-state probe are exercised at
// deploy-time E2E (real cdc_dw container) — no portable test harness
// for those in this repo today.
package persistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.uber.org/zap"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/job"
)

// stubJobRepoForSwap captures Create/UpdateStatus calls. Returning nil
// from Create simulates a fresh insert (no idempotency hit).
type stubJobRepoForSwap struct {
	createCalls int
	updates     []string
}

func (s *stubJobRepoForSwap) Create(_ context.Context, j *job.Job) error {
	s.createCalls++
	if j.ID == "" {
		j.ID = "stub-uuid"
	}
	return nil
}
func (s *stubJobRepoForSwap) GetByID(context.Context, string) (*job.Job, error) {
	return nil, errors.New("not used")
}
func (s *stubJobRepoForSwap) UpdateStatus(_ context.Context, _ string, st job.Status, _, _ string) error {
	s.updates = append(s.updates, string(st))
	return nil
}
func (s *stubJobRepoForSwap) ListPending(context.Context, string, int) ([]job.Job, error) {
	return nil, nil
}

func TestSwapAsync_RejectsInvalidMasterName(t *testing.T) {
	repo := &stubJobRepoForSwap{}
	// db nil ok — validation runs before any DB call.
	s := &MasterSwap{db: nil, jobRepo: repo, logger: zap.NewNop()}
	_, err := s.SwapAsync(context.Background(), "Bad-Name", "good_table", "reason long enough", "alice", "", "")
	if err == nil {
		t.Fatal("expected validation error for bad master_name")
	}
	if !strings.Contains(err.Error(), "invalid master_name") {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.createCalls != 0 {
		t.Fatalf("validation must run before persist, calls=%d", repo.createCalls)
	}
}

func TestSwapAsync_RejectsInvalidNewTableName(t *testing.T) {
	repo := &stubJobRepoForSwap{}
	s := &MasterSwap{db: nil, jobRepo: repo, logger: zap.NewNop()}
	_, err := s.SwapAsync(context.Background(), "good_master", "Has Space", "reason long enough", "alice", "", "")
	if err == nil {
		t.Fatal("expected validation error for bad new_table_name")
	}
	if !strings.Contains(err.Error(), "invalid new_table_name") {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.createCalls != 0 {
		t.Fatalf("validation must run before persist, calls=%d", repo.createCalls)
	}
}

// Compile-time check: MasterSwap.jobRepo satisfies the JobRepo port.
var _ ports.JobRepo = (*stubJobRepoForSwap)(nil)
