// Package commands — write-side use cases (CQRS C-side).
//
// ack_alert.go is the canonical SYNC command. It executes in-process
// against `persistence.AlertManager` and returns immediately. The bus still
// writes a `cdc_jobs` row for audit + idempotency, then closes it on
// success. No NATS round-trip.
//
// Pattern reference for the other 6 sync metadata commands (P3.T3.4):
// every sync handler conforms to this shape — Command struct, Handler
// struct with one dependency, Handle returns json.RawMessage.
package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/infra/persistence"
)

// AckAlertCommand is POST /api/alerts/:fingerprint/ack expressed as
// a Command. The user is captured at the API layer (JWT) and pinned
// onto the struct so the bus can serialise the full payload to the
// `cdc_jobs.payload` column for audit.
type AckAlertCommand struct {
	ports.SyncCommandMixin
	Fingerprint string `json:"fingerprint"`
	User        string `json:"user"`
	Reason      string `json:"reason,omitempty"`
}

// Type satisfies ports.Command.
func (c AckAlertCommand) Type() string { return "alert.ack" }

// Validate satisfies ports.Command.
func (c AckAlertCommand) Validate() error {
	if strings.TrimSpace(c.Fingerprint) == "" {
		return errors.New("fingerprint required")
	}
	if strings.TrimSpace(c.User) == "" {
		return errors.New("user required")
	}
	return nil
}

// AckAlertHandler is the in-process counterpart. Wired into the bus
// at server bootstrap via `RegisterSync("alert.ack", h)`.
type AckAlertHandler struct {
	am *persistence.AlertManager
}

func NewAckAlertHandler(am *persistence.AlertManager) *AckAlertHandler {
	return &AckAlertHandler{am: am}
}

// Handle satisfies messaging.SyncHandler. Returns `{"ok":true}` on
// success — preserves the legacy 200 body so FE keeps working when
// the API handler projects ResultBody back inline.
func (h *AckAlertHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(AckAlertCommand)
	if !ok {
		// Defensive: registration mismatch. Should never trip in prod.
		return nil, errors.New("alert.ack: command type mismatch")
	}
	if h.am == nil {
		return nil, errors.New("alert manager not ready")
	}
	if err := h.am.Ack(ctx, cmd.Fingerprint, cmd.User); err != nil {
		return nil, err
	}
	return json.RawMessage(`{"ok":true}`), nil
}
