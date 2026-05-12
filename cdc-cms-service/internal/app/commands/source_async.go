package commands

import (
	"errors"
	"strings"

	"cdc-cms-service/internal/app/ports"
)

// source_async.go — async commands for source-object lifecycle actions
// (create-default-columns, standardize, scan-fields, detect-timestamp-field,
// batch-transform). Wire shape mirrors the worker handlers in
// centralized-data-service/internal/handler/command_handler.go +
// recon_handler.go. Shared field set covers BOTH the legacy V1 publishers
// (registry_handler.go — `registry_id`-anchored) AND the V2 publishers
// (source_object_actions_handler.go — `source_object_id`-anchored). Callers
// populate the relevant subset; worker `json.Unmarshal` ignores unknown
// fields and missing fields stay zero — so the same struct serves both.

// CreateDefaultColumnsCommand publishes cdc.cmd.create-default-columns.
type CreateDefaultColumnsCommand struct {
	ports.AsyncCommandMixin
	RegistryID      uint   `json:"registry_id,omitempty"`
	SourceObjectID  int64  `json:"source_object_id,omitempty"`
	ShadowSchema    string `json:"shadow_schema,omitempty"`
	TargetTable     string `json:"target_table"`
	SourceTable     string `json:"source_table,omitempty"`
	PrimaryKeyField string `json:"primary_key_field,omitempty"`
	PrimaryKeyType  string `json:"primary_key_type,omitempty"`
}

func (CreateDefaultColumnsCommand) Type() string { return "source.create-default-columns" }
func (c CreateDefaultColumnsCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("create-default-columns: target_table required")
	}
	if c.RegistryID == 0 && c.SourceObjectID == 0 {
		return errors.New("create-default-columns: registry_id or source_object_id required")
	}
	return nil
}

// StandardizeCommand publishes cdc.cmd.standardize.
type StandardizeCommand struct {
	ports.AsyncCommandMixin
	RegistryID     uint   `json:"registry_id,omitempty"`
	SourceObjectID int64  `json:"source_object_id,omitempty"`
	TargetTable    string `json:"target_table"`
	ShadowSchema   string `json:"shadow_schema,omitempty"`
}

func (StandardizeCommand) Type() string { return "source.standardize" }
func (c StandardizeCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("standardize: target_table required")
	}
	return nil
}

// ScanFieldsCommand publishes cdc.cmd.scan-fields. Legacy V1 publisher
// also included `source_db` (registry_handler.go) — worker ignores
// unknown fields but keeping it preserves byte-identical wire shape.
type ScanFieldsCommand struct {
	ports.AsyncCommandMixin
	RegistryID     uint   `json:"registry_id,omitempty"`
	SourceObjectID int64  `json:"source_object_id,omitempty"`
	TargetTable    string `json:"target_table"`
	SourceTable    string `json:"source_table,omitempty"`
	SourceDB       string `json:"source_db,omitempty"`
	SyncEngine     string `json:"sync_engine,omitempty"`
	SourceType     string `json:"source_type,omitempty"`
	LegacySourceID string `json:"legacy_source_id,omitempty"`
}

func (ScanFieldsCommand) Type() string { return "source.scan-fields" }
func (c ScanFieldsCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("scan-fields: target_table required")
	}
	return nil
}

// DetectTimestampFieldCommand publishes cdc.cmd.detect-timestamp-field.
// Worker reads `registry_id` + `target_table`; the V2 publisher also
// included `source_object_id, source_table, source_db, source_type`
// for forward compat with the V2 metadata path. Keep the extra fields
// here so the wire shape stays byte-aligned with the legacy publisher.
type DetectTimestampFieldCommand struct {
	ports.AsyncCommandMixin
	RegistryID     uint   `json:"registry_id,omitempty"`
	SourceObjectID int64  `json:"source_object_id,omitempty"`
	TargetTable    string `json:"target_table"`
	SourceTable    string `json:"source_table,omitempty"`
	SourceDB       string `json:"source_db,omitempty"`
	SourceType     string `json:"source_type,omitempty"`
}

func (DetectTimestampFieldCommand) Type() string { return "source.detect-timestamp-field" }
func (c DetectTimestampFieldCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("detect-timestamp-field: target_table required")
	}
	return nil
}

// NOTE: cdc.cmd.batch-transform is intentionally NOT migrated to the
// CommandBus here. The legacy publisher (registry_handler.go) emits the
// bare table name as `[]byte(targetTable)` (no JSON), and the worker
// reads `string(msg.Data)` verbatim. Wrapping in a MarshalJSON override
// can't produce bare bytes — encoding/json rejects non-JSON output. A
// proper fix needs the worker to accept JSON-quoted input first; that
// belongs to the worker workspace alongside T3.6-T3.9. Until then the
// API handler keeps using `natsClient.Conn.Publish` directly for this
// one subject.

// AlterColumnCommand publishes cdc.cmd.alter-column.
type AlterColumnCommand struct {
	ports.AsyncCommandMixin
	TargetTable string `json:"target_table"`
	ColumnName  string `json:"column_name"`
	DataType    string `json:"data_type"`
	Action      string `json:"action"`
}

func (AlterColumnCommand) Type() string { return "mapping.alter-column" }
func (c AlterColumnCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("alter-column: target_table required")
	}
	if strings.TrimSpace(c.ColumnName) == "" {
		return errors.New("alter-column: column_name required")
	}
	if strings.TrimSpace(c.Action) == "" {
		return errors.New("alter-column: action required")
	}
	return nil
}

// BackfillCommand publishes cdc.cmd.backfill.
type BackfillCommand struct {
	ports.AsyncCommandMixin
	RegistryID   uint   `json:"registry_id,omitempty"`
	TargetTable  string `json:"target_table"`
	SourceField  string `json:"source_field"`
	TargetColumn string `json:"target_column"`
	DataType     string `json:"data_type"`
}

func (BackfillCommand) Type() string { return "mapping.backfill" }
func (c BackfillCommand) Validate() error {
	if strings.TrimSpace(c.TargetTable) == "" {
		return errors.New("backfill: target_table required")
	}
	if strings.TrimSpace(c.TargetColumn) == "" {
		return errors.New("backfill: target_column required")
	}
	return nil
}

