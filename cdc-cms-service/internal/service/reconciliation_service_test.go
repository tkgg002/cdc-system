// reconciliation_service_test.go — guard tests for the post-Airbyte
// no-op service. Plan v2 §R3 retired the Airbyte reconcile loop; the
// type stayed because callers in server.go + cmd/server/main.go still
// invoke Start/Stop. We pin the no-op contract so a future re-enablement
// does not silently break ctx propagation or the close-once invariant.
package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestReconciliationService_StartReturnsOnContextCancel(t *testing.T) {
	s := &ReconciliationService{stopCh: make(chan struct{}), logger: zap.NewNop()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
		// expected
	case <-time.After(time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

func TestReconciliationService_StopIsIdempotent(t *testing.T) {
	s := &ReconciliationService{stopCh: make(chan struct{})}
	s.Stop()
	// Second Stop must not panic on closing an already-closed channel.
	s.Stop()
	// The select-default branch in Stop ensures this.
}
