// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// GetSourceObjectMappingContextQuery is the input for
// GET /api/v1/source-objects/registry/{registry_id}.
type GetSourceObjectMappingContextQuery struct {
	RegistryID uint64
}

func (q GetSourceObjectMappingContextQuery) Type() string { return "source.mapping_context" }

// GetSourceObjectMappingContextHandler resolves the query against an
// injected SourceObjectReader.
type GetSourceObjectMappingContextHandler struct {
	reader SourceObjectReader
}

func NewGetSourceObjectMappingContextHandler(r SourceObjectReader) *GetSourceObjectMappingContextHandler {
	return &GetSourceObjectMappingContextHandler{reader: r}
}

// Handle returns the enriched mapping context, or `nil, nil` when the
// legacy registry id is unknown — the API layer translates that to
// HTTP 404.
func (h *GetSourceObjectMappingContextHandler) Handle(ctx context.Context, q GetSourceObjectMappingContextQuery) (*SourceObjectMappingContextReadModel, error) {
	return h.reader.GetMappingContextByRegistryID(ctx, q.RegistryID)
}
