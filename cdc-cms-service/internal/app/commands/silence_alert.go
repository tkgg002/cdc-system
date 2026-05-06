package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/service"
)

// SilenceAlertCommand mutes an alert until a deadline. Sync — single
// write through AlertManager.Silence.
type SilenceAlertCommand struct {
	ports.SyncCommandMixin
	Fingerprint string    `json:"fingerprint"`
	User        string    `json:"user"`
	Until       time.Time `json:"until"`
	Reason      string    `json:"reason"`
}

func (SilenceAlertCommand) Type() string { return "alert.silence" }

func (c SilenceAlertCommand) Validate() error {
	if strings.TrimSpace(c.Fingerprint) == "" {
		return errors.New("fingerprint required")
	}
	if strings.TrimSpace(c.User) == "" {
		return errors.New("user required")
	}
	if c.Until.IsZero() {
		return errors.New("'until' is required (RFC3339)")
	}
	if strings.TrimSpace(c.Reason) == "" {
		return errors.New("'reason' is required")
	}
	return nil
}

type SilenceAlertHandler struct {
	am *service.AlertManager
}

func NewSilenceAlertHandler(am *service.AlertManager) *SilenceAlertHandler {
	return &SilenceAlertHandler{am: am}
}

func (h *SilenceAlertHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(SilenceAlertCommand)
	if !ok {
		return nil, errors.New("alert.silence: command type mismatch")
	}
	if h.am == nil {
		return nil, errors.New("alert manager not ready")
	}
	if err := h.am.Silence(ctx, cmd.Fingerprint, cmd.User, cmd.Until, cmd.Reason); err != nil {
		return nil, err
	}
	return json.RawMessage(`{"ok":true}`), nil
}
