// Package master holds the master-binding aggregate.
//
// A master Binding is the contract between one or more source objects and
// the materialised master table in the destination warehouse. Schema status
// is the human-driven workflow (pending_review → approved → ...). The
// physical swap (ALTER TABLE RENAME) is dispatched to the worker as a
// `cdc.cmd.master-swap` command (Phase 2 v2 / P3).
package master

import "time"

// SchemaStatus is the workflow state of a Binding's schema.
type SchemaStatus string

const (
	SchemaPendingReview SchemaStatus = "pending_review"
	SchemaApproved      SchemaStatus = "approved"
	SchemaRejected      SchemaStatus = "rejected"
	SchemaFailed        SchemaStatus = "failed"
)

// Binding is the domain entity for `cdc_system.master_binding`.
type Binding struct {
	ID             int64
	Name           string
	MasterTable    string
	ShadowSchema   string
	ShadowTable    string
	SourceObjectID int64
	SchemaStatus   SchemaStatus
	IsActive       bool
	ApprovedBy     *string
	ApprovedAt     *time.Time
	RejectedBy     *string
	RejectedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Notes          *string
}

// Filter narrows binding lookup.
type Filter struct {
	IsActive     *bool
	SchemaStatus SchemaStatus
}
