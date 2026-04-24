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
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/natsconn"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openDLQIntegrationDB(t *testing.T) *gorm.DB {
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

func openIntegrationNATS(t *testing.T) *natsconn.NatsClient {
	t.Helper()

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://cdc_worker:worker_secret_2026@localhost:14222"
	}
	conn, err := nats.Connect(url, nats.Name("cdc-integration-test"))
	if err != nil {
		t.Skipf("skipping integration: cannot connect NATS (%v)", err)
	}
	t.Cleanup(conn.Close)
	return &natsconn.NatsClient{Conn: conn}
}

func TestDLQHandlerSendToDLQSanitizesFailedSyncLogAndPublishedMessage(t *testing.T) {
	db := openDLQIntegrationDB(t)
	nc := openIntegrationNATS(t)
	logger := zap.NewNop()

	handler := NewDLQHandler(nc, logger, db)
	handler.SetMaskingService(service.NewMaskingService(nil, nil, "phone", "email", "balance"))

	targetTable := "customer_profiles"
	subject := "cdc.goopay.db.customer_profiles"
	recordID := "dlq-it-1"
	db.Exec(`DELETE FROM failed_sync_logs WHERE target_table = ? AND record_id = ?`, targetTable, recordID)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM failed_sync_logs WHERE target_table = ? AND record_id = ?`, targetTable, recordID)
	})

	sub, err := nc.Conn.SubscribeSync(DLQSubject)
	if err != nil {
		t.Fatalf("subscribe dlq subject: %v", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Conn.Flush(); err != nil {
		t.Fatalf("flush nats subscribe: %v", err)
	}

	rawPayload := []byte(`{
		"_id":"dlq-it-1",
		"phone":"0901234567",
		"email":"alice@example.com",
		"metadata":{"balance":125000,"name":"Alice"}
	}`)
	procErr := errors.New("sink rejected email=alice@example.com phone=0901234567 secret=token-123")

	if err := handler.sendToDLQ(context.Background(), subject, rawPayload, targetTable, procErr); err != nil {
		t.Fatalf("sendToDLQ returned error: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("read dlq message from nats: %v", err)
	}

	var published DLQMessage
	if err := json.Unmarshal(msg.Data, &published); err != nil {
		t.Fatalf("unmarshal dlq message: %v", err)
	}
	assertNoSensitiveText(t, published.Error)

	var publishedRaw map[string]interface{}
	if err := json.Unmarshal(published.OriginalData, &publishedRaw); err != nil {
		t.Fatalf("unmarshal published original data: %v", err)
	}
	assertMaskedJSONMap(t, publishedRaw)

	var row model.FailedSyncLog
	if err := db.Where("target_table = ? AND record_id = ?", targetTable, recordID).
		Order("id DESC").
		First(&row).Error; err != nil {
		t.Fatalf("fetch failed_sync_logs row: %v", err)
	}
	assertNoSensitiveText(t, row.ErrorMessage)

	var persisted map[string]interface{}
	if err := json.Unmarshal(row.RawJSON, &persisted); err != nil {
		t.Fatalf("unmarshal failed_sync_logs.raw_json: %v", err)
	}
	assertMaskedJSONMap(t, persisted)
}

func assertMaskedJSONMap(t *testing.T, payload map[string]interface{}) {
	t.Helper()

	if payload["phone"] != "***" {
		t.Fatalf("phone should be masked, got %#v", payload["phone"])
	}
	if payload["email"] != "***" {
		t.Fatalf("email should be masked, got %#v", payload["email"])
	}
	metadata := payload["metadata"].(map[string]interface{})
	if metadata["balance"] != "***" {
		t.Fatalf("balance should be masked, got %#v", metadata["balance"])
	}
	if metadata["name"] != "Alice" {
		t.Fatalf("non-sensitive value changed unexpectedly, got %#v", metadata["name"])
	}
}

func assertNoSensitiveText(t *testing.T, value string) {
	t.Helper()

	for _, needle := range []string{"alice@example.com", "0901234567", "token-123"} {
		if strings.Contains(value, needle) {
			t.Fatalf("value still contains sensitive text %q: %q", needle, value)
		}
	}
	for _, expected := range []string{"email=***", "phone=***", "secret=***"} {
		if strings.Contains(strings.ToLower(value), strings.Split(expected, "=")[0]+"=") && !strings.Contains(value, expected) {
			t.Fatalf("value missing expected redaction %q: %q", expected, value)
		}
	}
}
