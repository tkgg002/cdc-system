package commands

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
)

// ReconCheckCommand is the canonical ASYNC command. Dispatch writes a
// `cdc_jobs` row, publishes the raw marshaled struct on
// `cdc.cmd.recon-check`, and returns 202 + JobID. Job correlation rides
// NATS headers (`Cdc-Job-Id`, `Cdc-Correlation-Id`, etc.) so existing
// workers that `json.Unmarshal(msg.Data, &payload)` keep working
// byte-for-byte. When JobMonitor lands (T3.6-T3.9) workers will read
// the header to emit `cdc.evt.recon-check.completed`.
//
// Pattern reference for every NATS command (P3.T3.5):
//
//   type XCommand struct { ... json tagged fields matching legacy payload ... }
//   func (XCommand) Type() string  { return "x.action" }
//   func (c XCommand) Validate() error { /* intrinsic invariants */ }
//
// No handler needed — the bus publishes via the registered subject.
// Wire shape == raw `json.Marshal(cmd)`. If the legacy publisher used
// extra/missing fields, mirror EXACTLY (keeps worker compat).
type ReconCheckCommand struct {
	ports.AsyncCommandMixin
	Tier  string `json:"tier"`
	Table string `json:"table"`
}

func (c ReconCheckCommand) Type() string { return "recon.check" }

func (c ReconCheckCommand) Validate() error {
	if strings.TrimSpace(c.Table) == "" {
		return errors.New("table required")
	}
	if strings.TrimSpace(c.Tier) == "" {
		return errors.New("tier required")
	}
	return nil
}
