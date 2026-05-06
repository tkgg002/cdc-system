// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// SyncHealthReader is the read-side port for GET /api/sync/health.
// It returns 5 aggregate counts: registry total/active/created +
// pending/approved mapping rules. Single-caller, colocated.
type SyncHealthReader interface {
	GetSyncHealth(ctx context.Context) (SyncHealthSnapshot, error)
}

// SyncHealthSnapshot is the wire shape of GET /api/sync/health.
// JSON tags must match the legacy handler in
// internal/api/registry_handler.go::SyncHealth — byte-identical
// surface is the contract.
//
// Field declaration order matters: legacy handler used fiber.Map
// (a Go map) which Go's encoding/json serializes in alphabetical
// key order. Mirror that order here so the wire stays byte-for-byte
// identical with the pre-CQRS payload.
type SyncHealthSnapshot struct {
	ActiveTables         int64 `json:"active_tables"`
	ApprovedMappingRules int64 `json:"approved_mapping_rules"`
	PendingMappingRules  int64 `json:"pending_mapping_rules"`
	TablesCreated        int64 `json:"tables_created"`
	TotalRegistryCMS     int64 `json:"total_registered_cms"`
}

// GetSyncHealthQuery is the input for GET /api/sync/health. The
// endpoint is unfiltered — system-wide aggregate counts.
type GetSyncHealthQuery struct{}

func (q GetSyncHealthQuery) Type() string { return "sync.health" }

// GetSyncHealthResult returns the snapshot.
type GetSyncHealthResult struct {
	Snapshot SyncHealthSnapshot
}

// GetSyncHealthHandler resolves the query against an injected reader.
type GetSyncHealthHandler struct {
	reader SyncHealthReader
}

func NewGetSyncHealthHandler(r SyncHealthReader) *GetSyncHealthHandler {
	return &GetSyncHealthHandler{reader: r}
}

// Handle resolves the query.
func (h *GetSyncHealthHandler) Handle(ctx context.Context, _ GetSyncHealthQuery) (GetSyncHealthResult, error) {
	snap, err := h.reader.GetSyncHealth(ctx)
	if err != nil {
		return GetSyncHealthResult{}, err
	}
	return GetSyncHealthResult{Snapshot: snap}, nil
}
