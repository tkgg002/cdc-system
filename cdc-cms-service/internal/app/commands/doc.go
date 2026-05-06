// Package commands holds write-side use cases (CQRS C-side). Each
// Command struct implements `ports.Command` and is dispatched via the
// CommandBus. The bus persists a Job row and publishes the matching
// NATS subject; the worker (`centralized-data-service`) does the heavy
// work and emits `cdc.evt.X.completed`.
//
// Phase 2 v2 / P1 — placeholder. P3 fills in per-endpoint commands.
package commands
