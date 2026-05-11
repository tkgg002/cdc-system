package commands

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
)

// recon_async.go — async commands grouped by reconciliation domain.
// Each struct mirrors the EXACT wire payload shape the
// centralized-data-service worker handlers parse today (see
// internal/handler/recon_handler.go in that repo). Adding/removing
// fields here without coordinating with the worker breaks dispatch.

// ReconHealCommand publishes cdc.cmd.recon-heal.
// Wire (raw): {"table":"<shadow>"}
type ReconHealCommand struct {
	ports.AsyncCommandMixin
	Table string `json:"table"`
}

func (ReconHealCommand) Type() string { return "recon.heal" }
func (c ReconHealCommand) Validate() error {
	if strings.TrimSpace(c.Table) == "" {
		return errors.New("recon.heal: table required")
	}
	return nil
}

// RetryFailedCommand publishes cdc.cmd.retry-failed.
// Wire (raw): mirrors the legacy `map[string]interface{}` payload —
// nullable scope fields use `*string` so omitted values serialize as
// `null`, matching the pre-P3 `stringOrNil(...)` helper output.
//
// FailedLogID is uint64 to match the worker handler in
// centralized-data-service (recon_handler.HandleRetryFailed).
type RetryFailedCommand struct {
	ports.AsyncCommandMixin
	FailedLogID    uint64  `json:"failed_log_id"`
	TargetTable    string  `json:"target_table"`
	RecordID       string  `json:"record_id"`
	RawJSON        string  `json:"raw_json"`
	SourceDatabase *string `json:"source_database"`
	SourceTable    *string `json:"source_table"`
	ShadowSchema   *string `json:"shadow_schema"`
	ShadowTable    *string `json:"shadow_table"`
	ScopeAmbiguous bool    `json:"scope_ambiguous"`
}

func (RetryFailedCommand) Type() string { return "recon.retry-failed" }
func (c RetryFailedCommand) Validate() error {
	if c.FailedLogID == 0 {
		return errors.New("retry-failed: failed_log_id required")
	}
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("retry-failed: target_table required")
	}
	return nil
}

// DebeziumSignalCommand publishes cdc.cmd.debezium-signal.
// Wire (raw): {"type":"signal-snapshot","database":"<>","collection":"<>"}
type DebeziumSignalCommand struct {
	ports.AsyncCommandMixin
	Type_      string `json:"type"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

func (DebeziumSignalCommand) Type() string { return "debezium.signal" }
func (c DebeziumSignalCommand) Validate() error {
	if strings.TrimSpace(c.Type_) == "" {
		return errors.New("debezium.signal: type required")
	}
	if strings.TrimSpace(c.Database) == "" {
		return errors.New("debezium.signal: database required")
	}
	if strings.TrimSpace(c.Collection) == "" {
		return errors.New("debezium.signal: collection required")
	}
	return nil
}

// DebeziumSnapshotCommand publishes cdc.cmd.debezium-snapshot.
// Wire (raw): {"table":"<>","database":"<>","collection":"<>"}
type DebeziumSnapshotCommand struct {
	ports.AsyncCommandMixin
	Table      string `json:"table"`
	Database   string `json:"database"`
	Collection string `json:"collection"`
}

func (DebeziumSnapshotCommand) Type() string { return "debezium.snapshot" }
func (c DebeziumSnapshotCommand) Validate() error {
	if strings.TrimSpace(c.Table) == "" {
		return errors.New("debezium.snapshot: table required")
	}
	return nil
}

// ReconBackfillSourceTsCommand publishes cdc.cmd.recon-backfill-source-ts.
// Wire (raw): {"table":"<>","run_id":"<uuid>","batch_size":N}
// Empty Table dispatches the all-tables fallback in the worker.
type ReconBackfillSourceTsCommand struct {
	ports.AsyncCommandMixin
	Table     string `json:"table"`
	RunID     string `json:"run_id"`
	BatchSize int    `json:"batch_size"`
}

func (ReconBackfillSourceTsCommand) Type() string { return "recon.backfill-source-ts" }
func (c ReconBackfillSourceTsCommand) Validate() error {
	if strings.TrimSpace(c.RunID) == "" {
		return errors.New("backfill-source-ts: run_id required")
	}
	return nil
}
