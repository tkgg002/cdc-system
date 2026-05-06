// Package queries holds read-side use cases (CQRS Q-side). Each Query has
// a struct describing the request shape and a Handler that returns a
// pure result. Handlers depend on `internal/app/ports` repositories only.
//
// Phase 2 v2 / P1 — placeholder. P2 fills in per-endpoint handlers.
package queries
