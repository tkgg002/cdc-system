// Package messaging hosts the NATS adapters: the Publisher, the
// CommandBus, and any subscriber wiring. CMS is publish-only — no
// subscriber lives here. The worker repo holds the consumer side.
//
// Phase 2 v2 / P1 — placeholder. P3 adds NATSCommandBus and the
// type-to-subject mapping.
package messaging
