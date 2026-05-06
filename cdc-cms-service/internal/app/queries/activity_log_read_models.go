// Package queries — read-side use cases (CQRS Q-side).
//
// activity_log_read_models.go pins the projection shapes used by GET
// /api/activity-log and /api/activity-log/stats. JSON tags are the wire
// contract; the legacy handler in `internal/api/activity_log_handler.go`
// re-exports these via type aliases for Swagger compatibility.
package queries

// ActivityLogRow is the per-row projection enriched with V2 source/
// shadow scope (LEFT JOIN LATERAL on shadow_binding + source_object_
// registry). `scope_ambiguous=true` when more than one shadow_binding
// row matches the same target_table — that signals the row's scope is
// not uniquely determined and the FE should fall back to target_table
// only.
type ActivityLogRow struct {
	ID              uint64  `json:"id"`
	Operation       string  `json:"operation"`
	TargetTable     string  `json:"target_table"`
	SourceDatabase  *string `json:"source_database,omitempty"`
	SourceSchema    *string `json:"source_schema,omitempty"`
	SourceNamespace *string `json:"source_namespace,omitempty"`
	SourceTable     *string `json:"source_table,omitempty"`
	ShadowSchema    *string `json:"shadow_schema,omitempty"`
	ShadowTable     *string `json:"shadow_table,omitempty"`
	ScopeAmbiguous  bool    `json:"scope_ambiguous"`
	Status          string  `json:"status"`
	RowsAffected    int64   `json:"rows_affected"`
	DurationMs      *int    `json:"duration_ms"`
	Details         any     `json:"details"`
	ErrorMessage    *string `json:"error_message"`
	TriggeredBy     string  `json:"triggered_by"`
	StartedAt       string  `json:"started_at"`
	CompletedAt     *string `json:"completed_at"`
}

// OpStat is one operation bucket in the 24h aggregate (GET
// /api/activity-log/stats). Counts are cardinality, not row sums.
type OpStat struct {
	Operation string `json:"operation"`
	Total     int64  `json:"total"`
	Success   int64  `json:"success"`
	Error     int64  `json:"error"`
	Skipped   int64  `json:"skipped"`
}
