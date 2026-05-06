package commands

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
)

// TransmuteRunCommand fires an immediate transmute on a master table.
// Async — bus.Dispatch persists a cdc_jobs row and publishes the
// json.Marshal(cmd) bytes on `cdc.cmd.transmute`. Wire shape MUST match
// the legacy payload (master_table / triggered_by / correlation_id) so
// existing TransmuteHandler.HandleTransmute keeps working byte-for-byte.
type TransmuteRunCommand struct {
	ports.AsyncCommandMixin
	MasterTable   string `json:"master_table"`
	TriggeredBy   string `json:"triggered_by"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func (TransmuteRunCommand) Type() string { return "transmute.run" }

func (c TransmuteRunCommand) Validate() error {
	if strings.TrimSpace(c.MasterTable) == "" {
		return errors.New("master_table required")
	}
	return nil
}
