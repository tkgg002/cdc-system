// Package queries — bridge_status_reader.go owns the read port for
// "bridge progress" probes (TransformStatus on registry, TransformStatusV2
// on source_object) and dispatch-scope resolution by source_object_id.
//
// The legacy handlers issued these as inline db.Raw calls inside the
// API tier; lifting them here keeps API → reader port → adapter clean
// and lets utils.PgIdent own identifier safety in one place.
package queries

import (
	"context"
	"errors"
	"time"
)

// BridgeStatusProbe is the projection used by TransformStatus +
// TransformStatusV2: physical-table existence, total rows, and bridge
// (_raw_data) progress. Empty Exists means "table not yet created";
// callers translate that into the `table_not_created` UI status.
type BridgeStatusProbe struct {
	Exists      bool
	TotalRows   int64
	HasRawData  bool
	RawDataRows int64
}

// DispatchScope is the resolution result for source-object actions
// (transform / re-detect / V2 transform-status). The shape mirrors the
// legacy `sourceObjectDispatchScope` struct in
// source_object_actions_handler.go so JSON tags and downstream command
// fields stay byte-identical.
type DispatchScope struct {
	SourceObjectID  int64  `gorm:"column:source_object_id"`
	TargetTable     string `gorm:"column:target_table"`
	ShadowSchema    string `gorm:"column:shadow_schema"`
	SourceDatabase  string `gorm:"column:source_database"`
	SourceTable     string `gorm:"column:source_table"`
	SourceType      string `gorm:"column:source_type"`
	PrimaryKeyField string `gorm:"column:primary_key_field"`
	PrimaryKeyType  string `gorm:"column:primary_key_type"`
}

// ErrAmbiguousDispatchScope signals that
// ResolveDispatchScopeBySourceObjectID matched more than one active
// shadow_binding for the same source_object. The HTTP layer maps this
// to 409 Conflict.
var ErrAmbiguousDispatchScope = errors.New("ambiguous_source_object_scope")

// ErrSourceObjectNoActiveShadow signals the resolved row exists but its
// shadow_binding row is missing/empty — handler maps to 409.
var ErrSourceObjectNoActiveShadow = errors.New("source_object_has_no_active_shadow_binding")

// BridgeStatusReader is the read port for transform-status + dispatch
// scope resolution. Adapter encapsulates information_schema probes plus
// the safe-identifier COUNT queries.
type BridgeStatusReader interface {
	// ProbeBridgeStatus returns physical-table existence and bridge
	// progress for `<schema>.<table>`. Schema "" is treated as public.
	// Identifier safety is handled internally via utils.PgIdent.
	ProbeBridgeStatus(ctx context.Context, schema, table string) (BridgeStatusProbe, error)

	// ResolveDispatchScopeBySourceObjectID joins source_object_registry
	// + shadow_binding for the given id. Returns
	// gorm.ErrRecordNotFound when absent, ErrAmbiguousDispatchScope on
	// >1 active binding, ErrSourceObjectNoActiveShadow when the matched
	// row has no active shadow_binding row.
	ResolveDispatchScopeBySourceObjectID(ctx context.Context, id int64) (*DispatchScope, error)

	// ListDispatchActivity returns up to 50 cdc_activity_log rows
	// whose target_table matches the given value, optionally narrowed
	// by operation and a started_at lower bound. Empty operation /
	// zero `since` are treated as "no filter" — preserves the legacy
	// SourceObjectActionsHandler.DispatchStatusV2 behaviour.
	ListDispatchActivity(ctx context.Context, table, operation string, since time.Time) ([]map[string]interface{}, error)
}
