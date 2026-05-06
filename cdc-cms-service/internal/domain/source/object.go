// Package source holds the source-object aggregate.
//
// A source Object is one external table (Postgres / MongoDB collection) that
// CDC ingests. The V2 row lives in `cdc_system.source_object_registry`; the
// legacy V1 row lives in `public.cdc_source_object_registry`. CMS reads V2;
// writes go through the V2 sync command (Phase 2 v2 / P3).
package source

import "time"

// ProvisioningState is the lifecycle of a registered source.
type ProvisioningState string

const (
	StateRegistered  ProvisioningState = "registered"
	StateProvisioned ProvisioningState = "provisioned"
	StateActive      ProvisioningState = "active"
	StateDisabled    ProvisioningState = "disabled"
	StateFailed      ProvisioningState = "failed"
)

// Scope identifies an object inside a source database.
type Scope struct {
	Database  string
	Table     string
	SourceDB  string // logical source DB id (e.g. "goopay_source")
}

// Object is the domain entity for one registered source.
type Object struct {
	ID                int64
	RegistryID        int64 // V1 cross-link
	ObjectCode        string
	SourceType        string // "postgres" | "mongodb"
	ConnectionCode    string
	Scope             Scope
	PrimaryKeyField   string
	PrimaryKeyType    string
	TimestampField    *string
	TargetTable       string
	IsActive          bool
	ProvisioningState ProvisioningState
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Notes             *string
}

// Filter narrows source object lookup.
type Filter struct {
	IsActive          *bool
	ProvisioningState ProvisioningState
	SourceType        string
	ConnectionCode    string
}
