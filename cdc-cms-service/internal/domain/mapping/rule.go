// Package mapping holds the mapping-rule aggregate of the CDC control plane.
//
// A Rule represents the projection of one source field onto one target column
// in the destination warehouse. Status is the workflow state and is the
// state machine the operator drives via the cdc-cms-web UI.
//
// This file defines the pure domain entity. GORM tags and DB column names
// live in `internal/infra/persistence/mapping_rule_repo_gorm.go`.
package mapping

import "time"

// Status is the workflow state of a Rule.
type Status string

const (
	StatusPending  Status = "pending"
	StatusApproved Status = "approved"
	StatusRejected Status = "rejected"
)

// RuleType discriminates how the rule was authored.
type RuleType string

const (
	RuleTypeSystem     RuleType = "system"
	RuleTypeDiscovered RuleType = "discovered"
	RuleTypeMapping    RuleType = "mapping"
)

// Rule is the immutable domain representation of one mapping rule row
// (cdc_system.mapping_rule_v2 + JOINed source_object_registry +
// shadow_binding context).
type Rule struct {
	ID                 int64
	SourceObjectID     int64
	MasterBindingID    *int64
	SourceDatabase     *string
	SourceSchema       *string
	SourceNamespace    *string
	SourceTable        string
	ShadowSchema       *string
	ShadowTable        *string
	SourceField        string
	SourcePath         *string
	TargetTable        string
	TargetColumn       string
	DataType           string
	SourceFormat       string
	TransformFn        *string
	IsActive           bool
	IsEnriched         bool
	IsNullable         bool
	DefaultValue       *string
	EnrichmentFunction *string
	Status             Status
	RuleType           RuleType
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CreatedBy          *string
	UpdatedBy          *string
	Notes              *string
}

// Filter narrows a List query against the mapping_rule_v2 table.
// Empty fields mean "no filter on that dimension".
type Filter struct {
	Status         Status
	RuleType       RuleType
	TargetTable    string
	SourceObjectID int64
	SourceDatabase string
	SourceTable    string
	ShadowSchema   string
	ShadowTable    string
	IsActive       *bool
}
