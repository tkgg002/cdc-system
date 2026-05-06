// Package ports declares the interfaces (a.k.a. "secondary ports" in
// hexagonal architecture) the application layer depends on. Concrete
// implementations live under `internal/infra/`.
//
// This file groups every repository interface in one place to make the
// contract surface obvious to reviewers. Each repo wraps exactly one
// aggregate (mapping rule, source object, master binding, ...). Cross-
// aggregate joins go through use-case-specific Query handlers in
// `internal/app/queries/`, NOT through repos.
package ports

import (
	"context"

	"cdc-cms-service/internal/domain/job"
	"cdc-cms-service/internal/domain/mapping"
	"cdc-cms-service/internal/domain/master"
	"cdc-cms-service/internal/domain/reconciliation"
	"cdc-cms-service/internal/domain/source"
	"cdc-cms-service/internal/model"
)

// MappingRuleRepo wraps `cdc_system.mapping_rule_v2`.
type MappingRuleRepo interface {
	List(ctx context.Context, f mapping.Filter) ([]mapping.Rule, error)
	ListPaginated(ctx context.Context, f mapping.Filter, page, pageSize int) ([]mapping.Rule, int64, error)
	GetByID(ctx context.Context, id int64) (*mapping.Rule, error)
	Save(ctx context.Context, r *mapping.Rule) error
	UpdateStatus(ctx context.Context, id int64, status mapping.Status) error
	BatchUpdateStatus(ctx context.Context, ids []int64, status mapping.Status) (int64, error)
}

// SourceRepo wraps `cdc_system.source_object_registry` + `shadow_binding`.
type SourceRepo interface {
	List(ctx context.Context, f source.Filter) ([]source.Object, error)
	GetByID(ctx context.Context, id int64) (*source.Object, error)
	GetByRegistryID(ctx context.Context, rid int64) (*source.Object, error)
	Save(ctx context.Context, o *source.Object) error
	ResolveScope(ctx context.Context, id int64) (source.Scope, error)
}

// MasterRepo wraps `cdc_system.master_binding`.
type MasterRepo interface {
	List(ctx context.Context, f master.Filter) ([]master.Binding, error)
	GetByName(ctx context.Context, name string) (*master.Binding, error)
	Save(ctx context.Context, b *master.Binding) error
	UpdateSchemaStatus(ctx context.Context, name string, s master.SchemaStatus) error
}

// JobRepo wraps `cdc_system.cdc_jobs` (created in Phase 2 v2 / P3 migration 036).
type JobRepo interface {
	Create(ctx context.Context, j *job.Job) error
	GetByID(ctx context.Context, id string) (*job.Job, error)
	UpdateStatus(ctx context.Context, id string, s job.Status, result, errMsg string) error
	ListPending(ctx context.Context, jtype string, limit int) ([]job.Job, error)
}

// ReconReportRepo wraps `cdc_reconciliation_report` (read-only from CMS).
type ReconReportRepo interface {
	Latest(ctx context.Context, table string) (*reconciliation.Report, error)
	List(ctx context.Context, f reconciliation.Filter) ([]reconciliation.Report, error)
}

// FailedSyncLogRepo wraps `failed_sync_logs`.
type FailedSyncLogRepo interface {
	List(ctx context.Context, f reconciliation.LogFilter) ([]reconciliation.FailedLog, error)
	GetByID(ctx context.Context, id int64) (*reconciliation.FailedLog, error)
	UpdateStatus(ctx context.Context, id int64, status reconciliation.FailedLogStatus) error
}

// SchemaLogRepo wraps `schema_change_logs`. The domain is shallow
// (audit-only), so the model type is reused rather than promoted to
// `internal/domain/...` — see ADR-CMS-HEX §3 "audit aggregates skip
// the domain layer until they grow behaviour".
type SchemaLogRepo interface {
	Create(ctx context.Context, log *model.SchemaChangeLog) error
	GetByTable(ctx context.Context, tableName *string, sourceDB *string) ([]model.SchemaChangeLog, error)
}

// PendingFieldRepo wraps `pending_fields`. Same audit-skip rationale as
// SchemaLogRepo: the model is a thin row carrier consumed by the
// approval flow, not an aggregate with behaviour. CMS only reads &
// updates rows; ingest (Worker) keeps its own write path.
type PendingFieldRepo interface {
	GetByID(ctx context.Context, id uint) (*model.PendingField, error)
	GetByStatus(ctx context.Context, status string, sourceDB *string, tableName *string, page, pageSize int) ([]model.PendingField, int64, error)
	Update(ctx context.Context, pf *model.PendingField) error
}

// WizardRepo wraps `cdc_system.cdc_wizard_sessions`. Backs the Source
// → Master automation flow (Create / Patch / Execute commands +
// session/progress queries). Update accepts an allow-listed map so the
// handler stays in control of which user-supplied fields persist.
// AppendProgress is JSONB-array-push via raw SQL (atomic, idempotent
// per-call) — extracted so commands don't carry persistence concerns.
type WizardRepo interface {
	Create(ctx context.Context, s *model.WizardSession) error
	Get(ctx context.Context, id string) (*model.WizardSession, error)
	Update(ctx context.Context, id string, updates map[string]interface{}) error
	AppendProgress(ctx context.Context, id string, entry map[string]interface{}) error
}

// SystemConnectorRepo wraps the Connection-Fingerprint registry table
// (`cdc_sources`). Distinct from `SourceRepo` above — that one wraps
// the V2 source-object registry; this one is the Debezium / Kafka
// Connect connector audit row that backs the system-connectors UI
// dropdown.
//
// Both `Upsert` and `MarkDeleted` are best-effort: connector lifecycle
// is the source of truth (Kafka Connect REST). The legacy
// `GetByConnectorName` is dropped — zero callers in CMS.
type SystemConnectorRepo interface {
	Upsert(ctx context.Context, s *model.Source) error
	List(ctx context.Context) ([]model.Source, error)
	GetByID(ctx context.Context, id int64) (*model.Source, error)
	MarkDeleted(ctx context.Context, connectorName string) error
}
