// Package queries — read-side use cases (CQRS Q-side).
package queries

import (
	"context"
	"encoding/json"
	"time"
)

// MasterListItem is the wire-shape projection of one row in
// GET /api/v1/masters. The list crosses 4 tables
// (master_binding ⨝ shadow_binding ⨝ source_object_registry ⨝
// connection_registry), so the type is owned by the query package
// rather than the master aggregate.
type MasterListItem struct {
	ID                   int64           `json:"id"`
	BindingCode          string          `json:"binding_code"`
	MasterName           string          `json:"master_name"`
	MasterSchema         string          `json:"master_schema"`
	MasterDatabase       *string         `json:"master_database,omitempty"`
	MasterConnectionCode *string         `json:"master_connection_code,omitempty"`
	SourceShadow         string          `json:"source_shadow"`
	SourceDatabase       *string         `json:"source_database,omitempty"`
	SourceSchema         *string         `json:"source_schema,omitempty"`
	SourceNamespace      *string         `json:"source_namespace,omitempty"`
	SourceTable          *string         `json:"source_table,omitempty"`
	ShadowBindingID      *int64          `json:"shadow_binding_id,omitempty"`
	ShadowSchema         *string         `json:"shadow_schema,omitempty"`
	ShadowTable          *string         `json:"shadow_table,omitempty"`
	PhysicalTableFQN     *string         `json:"physical_table_fqn,omitempty"`
	TransformType        string          `json:"transform_type"`
	Spec                 json.RawMessage `json:"spec"`
	IsActive             bool            `json:"is_active"`
	SchemaStatus         string          `json:"schema_status"`
	SchemaReviewedBy     *string         `json:"schema_reviewed_by,omitempty"`
	SchemaReviewedAt     *time.Time      `json:"schema_reviewed_at,omitempty"`
	RejectionReason      *string         `json:"rejection_reason,omitempty"`
	CreatedBy            *string         `json:"created_by,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

// MasterReader is the read-side port used by the master-binding query
// handlers. Colocated with its consumer (queries/) — single caller.
type MasterReader interface {
	ListEnriched(ctx context.Context) ([]MasterListItem, error)
}

// ListMastersQuery is the input for GET /api/v1/masters. The endpoint
// is currently unfiltered — the SQL ORDERs by master_schema, master_table.
type ListMastersQuery struct{}

func (q ListMastersQuery) Type() string { return "master.list" }

// ListMastersResult is what the handler returns.
type ListMastersResult struct {
	Data  []MasterListItem
	Count int
}

// ListMastersHandler resolves the query against an injected MasterReader.
type ListMastersHandler struct {
	reader MasterReader
}

func NewListMastersHandler(r MasterReader) *ListMastersHandler {
	return &ListMastersHandler{reader: r}
}

// Handle resolves the query.
func (h *ListMastersHandler) Handle(ctx context.Context, _ ListMastersQuery) (ListMastersResult, error) {
	rows, err := h.reader.ListEnriched(ctx)
	if err != nil {
		return ListMastersResult{}, err
	}
	return ListMastersResult{Data: rows, Count: len(rows)}, nil
}
