//go:build integration
// +build integration

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// openOrchestratorTestDB connects to the CDC system DB used for live
// integration tests. Skips (not fails) when the DSN env var is unset
// so `go test ./...` without -tags=integration stays green.
//
// PROVISIONING_TEST_DSN is the canonical override; falls back to the
// local docker-compose default (gpay-postgres-cdc:5433).
func openOrchestratorTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("PROVISIONING_TEST_DSN")
	if dsn == "" {
		dsn = "host=localhost port=5433 user=gpay_admin password=gpay_pass dbname=cdc_dw sslmode=disable"
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("skipping integration: cannot open Postgres (%v)", err)
	}
	// Sanity ping: bail fast if the function helper isn't installed.
	var n int
	if err := db.Raw(`SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
	                  WHERE n.nspname='cdc_system' AND p.proname='append_step_log_capped'`).Row().Scan(&n); err != nil {
		t.Skipf("skipping integration: append_step_log_capped not installed (apply migration 048): %v", err)
	}
	return db
}

// seedTestSource inserts a unique row in source_object_registry for one
// test case and returns its id. Cleans up on t.Cleanup.
func seedTestSource(t *testing.T, db *gorm.DB, mode string, state ProvisioningState) int64 {
	t.Helper()
	suffix := time.Now().UnixNano()
	objectCode := "prov_test_" + t.Name() + "_" + timeSuffix(suffix)
	res := db.Exec(`
		INSERT INTO cdc_system.source_object_registry (
		  object_code, source_connection_id, source_engine_type,
		  source_object_name, source_object_type, source_locator_json,
		  normalized_source_key, primary_key_field, cdc_mode, sync_engine,
		  is_active, profile_status,
		  provisioning_mode, provisioning_state, provisioning_step_log
		) VALUES (
		  ?, 1, 'postgresql', ?, 'table', '{}'::jsonb,
		  ?, 'id', 'incremental', 'debezium',
		  true, 'active',
		  ?, ?, '[]'::jsonb
		) RETURNING id`,
		objectCode, objectCode, "key_"+timeSuffix(suffix), mode, string(state))
	if res.Error != nil {
		t.Fatalf("seed source: %v", res.Error)
	}
	var id int64
	if err := db.Raw(`SELECT id FROM cdc_system.source_object_registry WHERE object_code = ?`,
		objectCode).Row().Scan(&id); err != nil {
		t.Fatalf("read seeded source id: %v", err)
	}
	t.Cleanup(func() {
		db.Exec(`DELETE FROM cdc_system.source_object_registry WHERE id = ?`, id)
	})
	return id
}

func timeSuffix(n int64) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, 16)
	for n > 0 {
		out = append(out, hex[n&0xF])
		n >>= 4
	}
	return string(out)
}

// TestOrchestrator_CAS_Concurrent — D6 invariant.
// Two goroutines call Advance() on the same `draft` source. Exactly one
// must succeed (state -> shadow_pending), the other returns ErrConflict.
// Final step_log length must be 1 (no double-step).
func TestOrchestrator_CAS_Concurrent(t *testing.T) {
	db := openOrchestratorTestDB(t)
	logger := zap.NewNop()
	// Use a no-op nats conn — we only assert DB CAS here, not publish.
	conn := stubNatsConn(t)
	o := NewProvisioningOrchestrator(db, conn, logger)
	id := seedTestSource(t, db, "manual", StateDraft)

	var wg sync.WaitGroup
	results := make(chan error, 2)
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			results <- o.Advance(context.Background(), id, "test-race")
		}()
	}
	wg.Wait()
	close(results)

	var nOK, nConflict int
	for err := range results {
		switch {
		case err == nil:
			nOK++
		case errors.Is(err, ErrConflict):
			nConflict++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if nOK != 1 || nConflict != 1 {
		t.Fatalf("expected 1 OK + 1 conflict, got %d OK / %d conflict", nOK, nConflict)
	}

	// Verify final state and log length.
	var state string
	var logLen int
	if err := db.Raw(`SELECT provisioning_state, jsonb_array_length(provisioning_step_log)
	                    FROM cdc_system.source_object_registry WHERE id = ?`, id).
		Row().Scan(&state, &logLen); err != nil {
		t.Fatalf("read final: %v", err)
	}
	if state != string(StateShadowPending) {
		t.Fatalf("final state=%q, want shadow_pending", state)
	}
	if logLen != 1 {
		t.Fatalf("step_log length=%d, want 1 (CAS prevented double-step)", logLen)
	}
}

// TestOrchestrator_LogCap_Trim50 — D7 invariant.
// Force 60 successive Pause/Resume flips to push step_log beyond cap.
// Final length must equal ProvisioningStepLogMaxEntries; first entry's
// seq must be entries past the trim threshold.
func TestOrchestrator_LogCap_Trim50(t *testing.T) {
	db := openOrchestratorTestDB(t)
	logger := zap.NewNop()
	conn := stubNatsConn(t)
	o := NewProvisioningOrchestrator(db, conn, logger)

	prev := ProvisioningStepLogMaxEntries
	ProvisioningStepLogMaxEntries = 50
	t.Cleanup(func() { ProvisioningStepLogMaxEntries = prev })

	id := seedTestSource(t, db, "manual", StateRunning)
	ctx := context.Background()
	// 30 pause/resume pairs = 60 entries.
	for i := 0; i < 30; i++ {
		if err := o.Pause(ctx, id, "log-cap-test"); err != nil {
			t.Fatalf("pause %d: %v", i, err)
		}
		if err := o.Resume(ctx, id, "log-cap-test"); err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
	}

	var raw []byte
	var n int
	if err := db.Raw(`SELECT provisioning_step_log::text, jsonb_array_length(provisioning_step_log)
	                    FROM cdc_system.source_object_registry WHERE id = ?`, id).
		Row().Scan(&raw, &n); err != nil {
		t.Fatalf("read log: %v", err)
	}
	if n != 50 {
		t.Fatalf("step_log length=%d, want 50 (FIFO trim)", n)
	}
	var arr []map[string]any
	if err := json.Unmarshal(raw, &arr); err != nil {
		t.Fatalf("unmarshal log: %v", err)
	}
	if len(arr) != 50 {
		t.Fatalf("decoded length=%d, want 50", len(arr))
	}
	// 60 entries total, kept newest 50 -> first kept = entry #11.
	first := arr[0]
	if seqVal, ok := first["seq"].(float64); !ok || int(seqVal) != 11 {
		t.Fatalf("first kept entry seq=%v, want 11", first["seq"])
	}
}

// TestOrchestrator_TracePropagation — D8 invariant.
// Inject an active OTel span into ctx, call Advance, and assert the
// published NATS payload carries the same trace_id.
func TestOrchestrator_TracePropagation(t *testing.T) {
	db := openOrchestratorTestDB(t)
	logger := zap.NewNop()

	// Spin up a temp NATS server connection. If unavailable, skip.
	natsURL := os.Getenv("NATS_TEST_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL // 127.0.0.1:4222
	}
	conn, err := nats.Connect(natsURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("skipping: cannot connect NATS at %s (%v)", natsURL, err)
	}
	t.Cleanup(func() { conn.Close() })

	// Subscriber captures payload synchronously.
	var (
		gotPayload []byte
		gotMu      sync.Mutex
		recv       = make(chan struct{}, 1)
	)
	sub, err := conn.Subscribe("cdc.cmd.shadow.bind", func(msg *nats.Msg) {
		gotMu.Lock()
		gotPayload = append([]byte(nil), msg.Data...)
		gotMu.Unlock()
		select {
		case recv <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Unsubscribe() })
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	o := NewProvisioningOrchestrator(db, conn, logger)
	id := seedTestSource(t, db, "manual", StateDraft)

	// Build an active span on ctx.
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	tracer := tp.Tracer("provisioning_test")
	ctx, span := tracer.Start(context.Background(), "advance-test")
	defer span.End()
	wantTraceID := span.SpanContext().TraceID().String()

	if err := o.Advance(ctx, id, "trace-test"); err != nil {
		t.Fatalf("advance: %v", err)
	}
	select {
	case <-recv:
	case <-time.After(3 * time.Second):
		t.Fatalf("did not receive published msg within 3s")
	}

	gotMu.Lock()
	defer gotMu.Unlock()
	var payload map[string]any
	if err := json.Unmarshal(gotPayload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got := payload["trace_id"]; got != wantTraceID {
		t.Fatalf("trace_id=%v, want %s", got, wantTraceID)
	}
	if _, ok := payload["span_id"].(string); !ok {
		t.Fatalf("span_id missing or wrong type: %v", payload["span_id"])
	}
}

// stubNatsConn returns a real NATS connection. Tests need a working
// publish path because Advance() returns the publish error after CAS
// commits — without it we can't tell "CAS lost" from "publish failed".
// Skip if no broker is reachable.
func stubNatsConn(t *testing.T) *nats.Conn {
	t.Helper()
	natsURL := os.Getenv("NATS_TEST_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	conn, err := nats.Connect(natsURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("skipping: cannot connect NATS at %s (%v)", natsURL, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
