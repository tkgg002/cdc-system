// Package queries — read-side use cases (CQRS Q-side).
//
// Source-object reads cross three tables (V2 source_object_registry +
// shadow_binding + V1 cdc_table_registry) and the latest reconciliation
// row. The enrichment columns (sync_status, bridge_status,
// metadata_status, recon_drift) are computed in SQL (see
// `infra/persistence/source_object_read_repo_gorm.go`).
//
// The interface is colocated with its consumer in `queries/` (single
// caller), keeping the standard `ports/` package focused on
// aggregate-level write+read contracts.
package queries

import "context"

// SourceObjectListFilter narrows the V2 source-object list view.
type SourceObjectListFilter struct {
	SourceDB string
	IsActive *bool
}

// SourceObjectReader is the read-side port used by the source-object
// query handlers. It returns enriched read models, NOT domain
// entities — the join surface is too wide for one aggregate.
type SourceObjectReader interface {
	ListEnriched(ctx context.Context, f SourceObjectListFilter, page, pageSize int) ([]SourceObjectListItem, int64, error)
	GetMappingContextByRegistryID(ctx context.Context, registryID uint64) (*SourceObjectMappingContextReadModel, error)
}
