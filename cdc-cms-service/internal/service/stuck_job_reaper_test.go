// stuck_job_reaper_test.go — guard tests for the per-type timeout map
// + constructor defaults. The actual UPDATE sweep needs a Postgres
// container to verify the `interval '1 second' * (CASE ... END)`
// expression; that's deferred to deploy-time E2E.
package service

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestDefaultJobTimeouts_ContainsCriticalTypes(t *testing.T) {
	m := DefaultJobTimeouts()
	// Boss G1 mandate: master.swap legitimately bounded ≤30s by the
	// goroutine ctx; reaper at 60s gives a generous cushion. recon.check
	// on a 50GB shadow is the classic false-positive case — needs ≥5min.
	if got, ok := m["master.swap"]; !ok || got < 30*time.Second {
		t.Errorf("master.swap timeout = %v want ≥30s", got)
	}
	if got, ok := m["recon.check"]; !ok || got < 5*time.Minute {
		t.Errorf("recon.check timeout = %v want ≥5m (50GB shadow scenario)", got)
	}
	if got, ok := m["transmute"]; !ok || got < 5*time.Minute {
		t.Errorf("transmute timeout = %v want ≥5m (long batch)", got)
	}
}

func TestNewStuckJobReaper_AppliesDefaults(t *testing.T) {
	r := NewStuckJobReaper(nil, zap.NewNop(), 0, nil)
	if r.interval != 30*time.Second {
		t.Errorf("interval=%v want 30s when zero passed", r.interval)
	}
	if len(r.timeouts) == 0 {
		t.Errorf("nil timeouts must fall back to DefaultJobTimeouts")
	}
	if r.defaultTO != 30*time.Second {
		t.Errorf("defaultTO=%v want 30s", r.defaultTO)
	}
}

func TestNewStuckJobReaper_AcceptsCustom(t *testing.T) {
	custom := map[string]time.Duration{"x.y": 90 * time.Second}
	r := NewStuckJobReaper(nil, zap.NewNop(), 5*time.Second, custom)
	if r.interval != 5*time.Second {
		t.Errorf("interval=%v want 5s", r.interval)
	}
	if got, ok := r.timeouts["x.y"]; !ok || got != 90*time.Second {
		t.Errorf("custom timeout dropped: %v", r.timeouts)
	}
	if _, has := r.timeouts["master.swap"]; has {
		t.Errorf("custom map should NOT inherit defaults")
	}
}
