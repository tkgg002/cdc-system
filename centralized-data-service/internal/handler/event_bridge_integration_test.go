//go:build integration
// +build integration

package handler

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"centralized-data-service/pkgs/natsconn"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func openEventBridgeIntegrationNATS(t *testing.T) *natsconn.NatsClient {
	t.Helper()

	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://cdc_worker:worker_secret_2026@localhost:14222"
	}
	conn, err := nats.Connect(url, nats.Name("event-bridge-integration-test"))
	if err != nil {
		t.Skipf("skipping integration: cannot connect NATS (%v)", err)
	}
	t.Cleanup(conn.Close)
	return &natsconn.NatsClient{Conn: conn}
}

func TestEventBridgePublishEventPublishesMetadataOnlyPayload(t *testing.T) {
	nc := openEventBridgeIntegrationNATS(t)
	bridge := NewEventBridge(nil, nc, nil, zap.NewNop())

	subject := "moleculer.event.customer_profiles"
	sub, err := nc.Conn.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribe event bridge subject: %v", err)
	}
	defer sub.Unsubscribe()
	if err := nc.Conn.Flush(); err != nil {
		t.Fatalf("flush nats subscribe: %v", err)
	}

	bridge.publishEvent("customer_profiles", `{
		"table":"customer_profiles",
		"op":"u",
		"source":"postgres",
		"after":{
			"_id":"evt-it-1",
			"phone":"0901234567",
			"email":"alice@example.com",
			"metadata":{"balance":125000},
			"name":"Alice"
		}
	}`)

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("read event bridge message from nats: %v", err)
	}
	if strings.Contains(string(msg.Data), "0901234567") || strings.Contains(string(msg.Data), "alice@example.com") || strings.Contains(string(msg.Data), "125000") {
		t.Fatalf("published event still contains sensitive values: %s", string(msg.Data))
	}

	var event MoleculerEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		t.Fatalf("unmarshal event bridge message: %v", err)
	}

	if event.Data.Table != "customer_profiles" {
		t.Fatalf("unexpected table: %#v", event.Data.Table)
	}
	if event.Data.After["record_id"] != "evt-it-1" {
		t.Fatalf("record_id mismatch: %#v", event.Data.After["record_id"])
	}
	if _, ok := event.Data.After["phone"]; ok {
		t.Fatalf("sensitive field should not be forwarded in event payload: %#v", event.Data.After)
	}
	if _, ok := event.Data.After["email"]; ok {
		t.Fatalf("sensitive field should not be forwarded in event payload: %#v", event.Data.After)
	}
	if _, ok := event.Data.After["after"]; ok {
		t.Fatalf("raw row image should not be forwarded: %#v", event.Data.After)
	}
	if _, ok := event.Data.After["changed_fields"]; !ok {
		t.Fatalf("expected changed_fields metadata in event payload: %#v", event.Data.After)
	}
}
