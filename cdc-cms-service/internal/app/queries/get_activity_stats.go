// Package queries — read-side use cases (CQRS Q-side).
package queries

import "context"

// ----- GetActivityStats --------------------------------------------

// GetActivityStatsQuery is GET /api/activity-log/stats. Unfiltered.
// The reader is expected to return both blocks in one call (the
// legacy handler sequenced two queries; the port lets implementations
// decide the strategy).
type GetActivityStatsQuery struct{}

func (q GetActivityStatsQuery) Type() string { return "activitylog.stats" }

// GetActivityStatsResult is the projected response. JSON tags on the
// API-level type alias preserve the exact wire shape (`stats_24h`,
// `recent_errors`).
type GetActivityStatsResult struct {
	Stats24h     []OpStat
	RecentErrors []ActivityLogRow
}

// GetActivityStatsHandler resolves the query against the reader.
type GetActivityStatsHandler struct {
	reader ActivityLogReader
}

func NewGetActivityStatsHandler(r ActivityLogReader) *GetActivityStatsHandler {
	return &GetActivityStatsHandler{reader: r}
}

func (h *GetActivityStatsHandler) Handle(ctx context.Context, _ GetActivityStatsQuery) (GetActivityStatsResult, error) {
	ops, errs, err := h.reader.Stats24h(ctx)
	if err != nil {
		return GetActivityStatsResult{}, err
	}
	return GetActivityStatsResult{Stats24h: ops, RecentErrors: errs}, nil
}
