package commands

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
)

// RestartDebeziumCommand publishes cdc.cmd.restart-debezium.
// Wire (raw): {"connector_name":"<>","kafka_connect_url":"<>"}
// Worker only reads ConnectorName; KafkaConnectURL is preserved for
// future use and to keep the byte-for-byte legacy shape until removed
// jointly with the worker side.
type RestartDebeziumCommand struct {
	ports.AsyncCommandMixin
	ConnectorName   string `json:"connector_name"`
	KafkaConnectURL string `json:"kafka_connect_url"`
}

func (RestartDebeziumCommand) Type() string { return "debezium.restart" }
func (c RestartDebeziumCommand) Validate() error {
	if strings.TrimSpace(c.ConnectorName) == "" {
		return errors.New("debezium.restart: connector_name required")
	}
	return nil
}
