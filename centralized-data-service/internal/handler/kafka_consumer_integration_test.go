//go:build integration
// +build integration

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openKafkaConsumerIntegrationDB(t *testing.T) *gorm.DB {
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

func TestKafkaConsumerWriteDLQSanitizesFailedSyncLogRow(t *testing.T) {
	db := openKafkaConsumerIntegrationDB(t)
	kc := NewKafkaConsumer(KafkaConsumerConfig{}, nil, nil, db, zap.NewNop())
	kc.SetMaskingService(service.NewMaskingService(nil, nil, "phone", "email", "balance"))

	targetTable := "customer_profiles"
	recordID := "kafka-dlq-it-1"
	db.Exec(`DELETE FROM failed_sync_logs WHERE target_table = ? AND record_id = ?`, targetTable, recordID)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM failed_sync_logs WHERE target_table = ? AND record_id = ?`, targetTable, recordID)
	})

	msg := kafka.Message{
		Topic: "cdc.goopay.db.customer_profiles",
		Key:   []byte(`{"id":"kafka-dlq-it-1"}`),
		Value: []byte(`{
			"op":"u",
			"after":{
				"_id":"kafka-dlq-it-1",
				"phone":"0901234567",
				"email":"alice@example.com",
				"metadata":{"balance":125000,"name":"Alice"}
			}
		}`),
	}
	procErr := errors.New("processing failed email=alice@example.com phone=0901234567 secret=token-123")

	if err := kc.writeDLQ(context.Background(), msg, procErr); err != nil {
		t.Fatalf("writeDLQ returned error: %v", err)
	}

	var row model.FailedSyncLog
	if err := db.Where("target_table = ? AND record_id = ?", targetTable, recordID).
		Order("id DESC").
		First(&row).Error; err != nil {
		t.Fatalf("fetch failed_sync_logs row: %v", err)
	}

	for _, needle := range []string{"alice@example.com", "0901234567", "token-123"} {
		if strings.Contains(row.ErrorMessage, needle) {
			t.Fatalf("ErrorMessage still contains %q: %q", needle, row.ErrorMessage)
		}
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(row.RawJSON, &payload); err != nil {
		t.Fatalf("unmarshal raw_json: %v", err)
	}
	after := payload["after"].(map[string]interface{})
	if after["phone"] != "***" {
		t.Fatalf("after.phone should be masked, got %#v", after["phone"])
	}
	if after["email"] != "***" {
		t.Fatalf("after.email should be masked, got %#v", after["email"])
	}
	metadata := after["metadata"].(map[string]interface{})
	if metadata["balance"] != "***" {
		t.Fatalf("after.metadata.balance should be masked, got %#v", metadata["balance"])
	}
	if metadata["name"] != "Alice" {
		t.Fatalf("non-sensitive value changed unexpectedly, got %#v", metadata["name"])
	}
}
