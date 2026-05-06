package commands

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRestartDebeziumCommand(t *testing.T) {
	if (RestartDebeziumCommand{}).Type() != "debezium.restart" {
		t.Fatal("type")
	}
	if err := (RestartDebeziumCommand{}).Validate(); err == nil {
		t.Fatal("expected connector_name required")
	}
	if err := (RestartDebeziumCommand{ConnectorName: " "}).Validate(); err == nil {
		t.Fatal("expected connector_name required (whitespace)")
	}
	if err := (RestartDebeziumCommand{ConnectorName: "src-orders"}).Validate(); err != nil {
		t.Errorf("ok: %v", err)
	}
	// Wire shape: legacy worker reads connector_name; both fields ride along.
	b, _ := json.Marshal(RestartDebeziumCommand{
		ConnectorName: "src-orders", KafkaConnectURL: "http://kc:8083",
	})
	got := string(b)
	if !strings.Contains(got, `"connector_name":"src-orders"`) {
		t.Errorf("missing connector_name: %s", got)
	}
	if !strings.Contains(got, `"kafka_connect_url":"http://kc:8083"`) {
		t.Errorf("missing kafka_connect_url: %s", got)
	}
}
