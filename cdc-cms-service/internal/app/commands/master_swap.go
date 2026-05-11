package commands

import (
	"errors"

	"cdc-cms-service/internal/app/ports"
)

// MasterSwapCommand kicks off an atomic RENAME swap of a master table.
//
// P3: Dispatched asynchronously to the worker via NATS. The worker executes
// the two-RENAME TX so a slow ALTER (lock contention) cannot block the
// CMS Control Plane.
type MasterSwapCommand struct {
	ports.AsyncCommandMixin

	MasterName   string `json:"master_name"`
	NewTableName string `json:"new_table_name"`
	Reason       string `json:"reason"`
}

func (c MasterSwapCommand) Type() string { return "master.swap" }

func (c MasterSwapCommand) Validate() error {
	if c.MasterName == "" {
		return errors.New("master_name is required")
	}
	if c.NewTableName == "" {
		return errors.New("new_table_name is required")
	}
	if c.Reason == "" {
		return errors.New("reason is required")
	}
	if len(c.Reason) < 10 {
		return errors.New("reason_required_min_10_chars")
	}
	return nil
}
