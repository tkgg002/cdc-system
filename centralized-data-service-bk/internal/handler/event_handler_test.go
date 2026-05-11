package handler

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
)

// mockRegistry is a minimal MetadataRegistry stub for unit tests.
type mockRegistry struct {
	routes []*service.ResolvedSourceRoute
}

func (m *mockRegistry) ResolveSourceRoute(sourceDB, sourceTable string) *service.ResolvedSourceRoute {
	if len(m.routes) == 0 {
		return nil
	}
	return m.routes[0]
}
func (m *mockRegistry) ResolveSourceRoutes(sourceDB, sourceTable string) []*service.ResolvedSourceRoute {
	return m.routes
}
func (m *mockRegistry) ResolveTargetRoute(targetTable string) *service.ResolvedSourceRoute { return nil }
func (m *mockRegistry) ReloadAll(_ context.Context) error                                  { return nil }
func (m *mockRegistry) GetTableConfigByID(id uint) *model.TableRegistry                   { return nil }
func (m *mockRegistry) GetTableConfig(t string) *model.TableRegistry                      { return nil }
func (m *mockRegistry) GetTableConfigBySource(t string) *model.TableRegistry              { return nil }
func (m *mockRegistry) ListTableConfigs() []model.TableRegistry                            { return nil }
func (m *mockRegistry) GetMappingRules(t string) []model.MappingRule                      { return nil }
func (m *mockRegistry) GetDebeziumTables() []string                                        { return nil }

func TestExtractSourceAndTable_FromSubject(t *testing.T) {
	tests := []struct {
		subject    string
		source     string
		wantDB     string
		wantTable  string
	}{
		{"cdc.goopay.goopay_wallet.wallet_transactions", "", "goopay_wallet", "wallet_transactions"},
		{"cdc.goopay.goopay_payment.payments", "", "goopay_payment", "payments"},
		{"cdc.goopay.goopay_legacy.legacy_refunds", "", "goopay_legacy", "legacy_refunds"},
		// Fallback: parse from source
		{"short.subject", "/debezium/mongodb/goopay_main/users", "goopay_main", "users"},
		// Unknown
		{"x", "y", "unknown", "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			db, table := extractSourceAndTable(tt.subject, tt.source)
			if db != tt.wantDB {
				t.Errorf("sourceDB = %q, want %q", db, tt.wantDB)
			}
			if table != tt.wantTable {
				t.Errorf("table = %q, want %q", table, tt.wantTable)
			}
		})
	}
}

func TestExtractPrimaryKey_MongoObjectId(t *testing.T) {
	data := map[string]interface{}{
		"_id": map[string]interface{}{"$oid": "65f1a2b3c4d5e6f7a8b9c0d1"},
	}
	pk := extractPrimaryKey(data, "_id", "mongodb")
	if pk != "65f1a2b3c4d5e6f7a8b9c0d1" {
		t.Errorf("pk = %q, want ObjectId string", pk)
	}
}

func TestExtractPrimaryKey_StringID(t *testing.T) {
	data := map[string]interface{}{"id": "user-123"}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "user-123" {
		t.Errorf("pk = %q, want %q", pk, "user-123")
	}
}

func TestExtractPrimaryKey_NumericID(t *testing.T) {
	data := map[string]interface{}{"id": float64(42)}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "42" {
		t.Errorf("pk = %q, want %q", pk, "42")
	}
}

func TestExtractPrimaryKey_Missing(t *testing.T) {
	data := map[string]interface{}{"name": "test"}
	pk := extractPrimaryKey(data, "id", "mysql")
	if pk != "" {
		t.Errorf("pk = %q, want empty for missing field", pk)
	}
}

// TestHandleDelete_FirstTouch_TombstoneInsert verifies P1.1 (G3):
// handleDelete must emit INSERT ... ON CONFLICT ... DO UPDATE (tombstone-first UPSERT)
// so that a delete event for a row absent from shadow still creates a tombstone.
func TestHandleDelete_FirstTouch_TombstoneInsert(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer sqlDB.Close()

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	h := &EventHandler{
		db:     gormDB,
		logger: zap.NewNop(),
	}

	route := &service.ResolvedSourceRoute{
		ShadowBinding: &model.ShadowBinding{
			ShadowSchema: "shadow_test",
			ShadowTable:  "orders",
		},
		TableConfig: &model.TableRegistry{
			TargetTable:     "orders",
			PrimaryKeyField: "id",
			SourceType:      "postgres",
		},
	}

	event := &model.CDCEvent{
		Data: model.CDCEventData{
			Op:     "d",
			Before: map[string]interface{}{"id": float64(999)},
		},
	}

	// Expect: INSERT INTO "shadow_test"."orders" ... ON CONFLICT ... DO UPDATE SET _deleted = TRUE
	mock.ExpectExec(`INSERT INTO "shadow_test"\."orders".*ON CONFLICT.*DO UPDATE SET`).
		WithArgs("999", "999").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = h.handleDelete(context.Background(), event, []*service.ResolvedSourceRoute{route})
	if err != nil {
		t.Fatalf("handleDelete returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet mock expectations: %v", err)
	}
}

// TestHandleDelete_NilBefore_SkipRoute verifies A2 fix (P1.1/G3):
// When Before==nil (e.g. REPLICA IDENTITY DEFAULT), handleDelete must NOT hard-fail.
// It should warn-skip the route (no SQL exec) and return nil.
func TestHandleDelete_NilBefore_SkipRoute(t *testing.T) {
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer sqlDB.Close()

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	h := &EventHandler{
		db:     gormDB,
		logger: zap.NewNop(),
	}

	route := &service.ResolvedSourceRoute{
		ShadowBinding: &model.ShadowBinding{
			ShadowSchema: "shadow_test",
			ShadowTable:  "orders",
		},
		TableConfig: &model.TableRegistry{
			TargetTable:     "orders",
			PrimaryKeyField: "id",
			SourceType:      "postgres",
		},
	}

	// Before=nil — simulates delete event without REPLICA IDENTITY FULL
	event := &model.CDCEvent{
		Data: model.CDCEventData{
			Op:     "d",
			Before: nil,
		},
	}

	// No SQL exec expected — route must be skipped with warn log
	err = h.handleDelete(context.Background(), event, []*service.ResolvedSourceRoute{route})
	if err != nil {
		t.Fatalf("handleDelete returned error for nil Before (want nil): %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected SQL was executed: %v", err)
	}
}
