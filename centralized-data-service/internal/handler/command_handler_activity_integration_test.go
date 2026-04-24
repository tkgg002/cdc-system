//go:build integration
// +build integration

package handler

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"centralized-data-service/internal/model"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openCommandHandlerIntegrationDB(t *testing.T) *gorm.DB {
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

func TestCommandHandlerPublishResultWithSubjectSanitizesActivityLog(t *testing.T) {
	db := openCommandHandlerIntegrationDB(t)
	logger := zap.NewNop()
	handler := NewCommandHandler(db, nil, nil, nil, logger)

	targetTable := "it_cmd_activity_log_security"
	db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, targetTable)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, targetTable)
	})

	handler.publishResultWithSubject(&nats.Msg{}, "cdc.result.sync-register", CommandResult{
		Command:     "sync-register",
		TargetTable: targetTable,
		Status:      "error",
		Error:       "sync failed\nemail=alice@example.com phone=0901234567 secret=token-123",
	})

	var row model.ActivityLog
	if err := db.Where("target_table = ? AND operation = ?", targetTable, "cmd-sync-register").
		Order("id DESC").
		First(&row).Error; err != nil {
		t.Fatalf("fetch activity log row: %v", err)
	}

	if row.ErrorMessage == nil {
		t.Fatal("expected sanitized error_message in activity log")
	}
	assertActivityLogRedacted(t, *row.ErrorMessage)

	var details map[string]interface{}
	if err := json.Unmarshal(row.Details, &details); err != nil {
		t.Fatalf("unmarshal details: %v", err)
	}

	if details["command"] != "sync-register" {
		t.Fatalf("unexpected command in details: %#v", details["command"])
	}
	if details["status"] != "error" {
		t.Fatalf("unexpected status in details: %#v", details["status"])
	}
	errField, _ := details["error"].(string)
	if errField == "" {
		t.Fatalf("expected sanitized error field in details, got %#v", details["error"])
	}
	assertActivityLogRedacted(t, errField)
}

func TestCommandHandlerWriteActivitySanitizesDetailsBeforeDBInsert(t *testing.T) {
	db := openCommandHandlerIntegrationDB(t)
	logger := zap.NewNop()
	handler := NewCommandHandler(db, nil, nil, nil, logger)

	targetTable := "it_cmd_activity_log_details"
	db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, targetTable)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, targetTable)
	})

	handler.writeActivity("scan-fields", targetTable, "error", 0, map[string]interface{}{
		"command":      "scan-fields",
		"target_table": targetTable,
		"status":       "error",
		"new_fields":   []interface{}{"phone", "email"},
		"payload": map[string]interface{}{
			"phone": "0901234567",
		},
		"error": "sample failed email=alice@example.com api_key=xyz",
	}, "trace secret=token-123 phone=0901234567")

	var row model.ActivityLog
	if err := db.Where("target_table = ? AND operation = ?", targetTable, "scan-fields").
		Order("id DESC").
		First(&row).Error; err != nil {
		t.Fatalf("fetch activity log row: %v", err)
	}

	if row.ErrorMessage == nil {
		t.Fatal("expected sanitized error_message in activity log")
	}
	assertActivityLogRedacted(t, *row.ErrorMessage)

	var details map[string]interface{}
	if err := json.Unmarshal(row.Details, &details); err != nil {
		t.Fatalf("unmarshal details: %v", err)
	}
	if _, ok := details["payload"]; ok {
		t.Fatalf("non-allowlisted payload should not be persisted: %#v", details)
	}
	if fields, ok := details["new_fields"].([]interface{}); !ok || len(fields) != 2 {
		t.Fatalf("expected sanitized new_fields slice, got %#v", details["new_fields"])
	}
	errField, _ := details["error"].(string)
	assertActivityLogRedacted(t, errField)
}

func assertActivityLogRedacted(t *testing.T, value string) {
	t.Helper()

	if strings.Contains(value, "\n") || strings.Contains(value, "\r") {
		t.Fatalf("activity log value still contains line breaks: %q", value)
	}
	for _, needle := range []string{"alice@example.com", "0901234567", "token-123", "xyz"} {
		if strings.Contains(value, needle) {
			t.Fatalf("activity log value still contains sensitive text %q: %q", needle, value)
		}
	}
	if strings.Contains(value, "email=") && !strings.Contains(value, "email=***") {
		t.Fatalf("activity log email should be redacted: %q", value)
	}
	if strings.Contains(value, "phone=") && !strings.Contains(value, "phone=***") {
		t.Fatalf("activity log phone should be redacted: %q", value)
	}
	if strings.Contains(strings.ToLower(value), "secret=") && !strings.Contains(value, "secret=***") {
		t.Fatalf("activity log secret should be redacted: %q", value)
	}
	if strings.Contains(strings.ToLower(value), "api_key=") && !strings.Contains(value, "api_key=***") {
		t.Fatalf("activity log api_key should be redacted: %q", value)
	}
}
