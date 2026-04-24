//go:build integration
// +build integration

package handler

import (
	"encoding/json"
	"os"
	"testing"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openReconIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := os.Getenv("DB_SINK_URL")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=user password=password dbname=goopay_dw sslmode=disable"
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("skipping integration: cannot open Postgres (%v)", err)
	}
	return db
}

func TestReconHandlerRetryUsesSanitizedSQLArgs(t *testing.T) {
	db := openReconIntegrationDB(t)
	logger := zap.NewNop()

	targetTable := "it_recon_retry_sanitize"
	sourceTable := "it_recon_retry_sanitize_src"

	db.Exec(`DROP TABLE IF EXISTS "it_recon_retry_sanitize"`)
	if err := db.Exec(`CREATE TABLE "it_recon_retry_sanitize" (
		"id" TEXT PRIMARY KEY,
		"phone" TEXT,
		"email" TEXT,
		"name" TEXT
	)`).Error; err != nil {
		t.Fatalf("create test table: %v", err)
	}
	t.Cleanup(func() {
		db.Exec(`DROP TABLE IF EXISTS "it_recon_retry_sanitize"`)
		db.Exec(`DELETE FROM cdc_table_registry WHERE target_table = ?`, targetTable)
	})

	sensitiveFields, _ := json.Marshal([]string{"phone", "email"})
	registry := model.TableRegistry{
		SourceDB:        "mongo",
		SourceType:      "mongodb",
		SourceTable:     sourceTable,
		TargetTable:     targetTable,
		PrimaryKeyField: "id",
		PrimaryKeyType:  "TEXT",
		IsActive:        true,
		SensitiveFields: sensitiveFields,
	}
	db.Exec(`DELETE FROM cdc_table_registry WHERE target_table = ?`, targetTable)
	if err := db.Create(&registry).Error; err != nil {
		t.Fatalf("create registry row: %v", err)
	}

	schema := service.NewSchemaAdapter(db, logger)
	if err := schema.PrepareForCDCInsert(targetTable, "id"); err != nil {
		t.Fatalf("prepare schema: %v", err)
	}

	masking := service.NewMaskingService(db, logger)
	handler := NewReconHandler(nil, db, nil, schema, logger).WithMaskingService(masking)

	payload, _ := json.Marshal(map[string]interface{}{
		"failed_log_id": uint64(999999999),
		"target_table":  targetTable,
		"record_id":     "recon-it-1",
		"raw_json":      `{"id":"recon-it-1","phone":"0901234567","email":"alice@example.com","name":"Alice"}`,
	})

	handler.HandleRetryFailed(&nats.Msg{Data: payload})

	type row struct {
		ID      string
		Phone   string
		Email   string
		Name    string
		RawData []byte `gorm:"column:_raw_data"`
	}
	var inserted row
	if err := db.Raw(`SELECT id, phone, email, name, _raw_data FROM "`+targetTable+`" WHERE id = ?`, "recon-it-1").
		Scan(&inserted).Error; err != nil {
		t.Fatalf("query inserted row: %v", err)
	}

	if inserted.ID != "recon-it-1" {
		t.Fatalf("unexpected id: %#v", inserted.ID)
	}
	if inserted.Phone != "***" {
		t.Fatalf("phone arg should be sanitized before SQL upsert, got %#v", inserted.Phone)
	}
	if inserted.Email != "***" {
		t.Fatalf("email arg should be sanitized before SQL upsert, got %#v", inserted.Email)
	}
	if inserted.Name != "Alice" {
		t.Fatalf("non-sensitive value changed unexpectedly, got %#v", inserted.Name)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(inserted.RawData, &raw); err != nil {
		t.Fatalf("unmarshal _raw_data: %v", err)
	}
	if raw["phone"] != "***" {
		t.Fatalf("_raw_data phone should be masked, got %#v", raw["phone"])
	}
	if raw["email"] != "***" {
		t.Fatalf("_raw_data email should be masked, got %#v", raw["email"])
	}
	if raw["name"] != "Alice" {
		t.Fatalf("_raw_data non-sensitive field changed unexpectedly, got %#v", raw["name"])
	}
}
