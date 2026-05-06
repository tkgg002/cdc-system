package ports

import "context"

// Query is one read-side request. Implementations live in
// `internal/app/queries/`. Queries are always synchronous and never write.
type Query interface {
	Type() string
}

// QueryBus dispatches a Query to its registered handler. The concrete
// result type depends on the Query — callers use a type assertion. See
// `internal/app/queries/` for the per-query result types.
type QueryBus interface {
	Ask(ctx context.Context, q Query) (any, error)
}
