// source_object_v2_sync_test.go — guard tests for the V2 sync entry
// points. The two-INSERT transactional behavior (source_object_registry
// → shadow_binding rollback on FK / constraint failure) is exercised at
// deploy-time E2E against a real cdc_dw container; no portable mock
// harness lives in this repo today (project convention — see
// master_swap_test.go header for prior art).
package service

import (
	"context"
	"strings"
	"testing"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
)

func stubTableRegistryEntry() *model.TableRegistry {
	return &model.TableRegistry{
		ID:              42,
		SourceDB:        "goopay_source",
		SourceType:      "postgres",
		SourceTable:     "orders",
		TargetTable:     "orders",
		SyncEngine:      "debezium",
		PrimaryKeyField: "id",
		IsActive:        true,
	}
}

func TestSyncFromLegacy_NilEntry(t *testing.T) {
	s := &SourceObjectV2SyncService{db: nil, logger: zap.NewNop()}
	if err := s.SyncFromLegacy(context.Background(), nil); err != nil {
		t.Fatalf("nil entry must be a no-op, got error: %v", err)
	}
}

func TestSyncFromLegacyTx_NilEntry(t *testing.T) {
	s := &SourceObjectV2SyncService{db: nil, logger: zap.NewNop()}
	if err := s.SyncFromLegacyTx(context.Background(), nil, nil); err != nil {
		t.Fatalf("nil entry must short-circuit before tx check, got: %v", err)
	}
}

// SyncFromLegacyTx must surface a clear error if a caller forgets to
// pass the transaction handle. Without this fail-fast, a nil tx would
// nil-deref deep inside Raw() with a less-actionable trace.
func TestSyncFromLegacyTx_NilTxFailsFast(t *testing.T) {
	s := &SourceObjectV2SyncService{db: nil, logger: zap.NewNop()}
	stub := stubTableRegistryEntry()
	err := s.SyncFromLegacyTx(context.Background(), nil, stub)
	if err == nil {
		t.Fatal("expected explicit error for nil tx, got nil")
	}
	if !strings.Contains(err.Error(), "nil transaction") {
		t.Fatalf("error should name the missing tx, got: %v", err)
	}
}
