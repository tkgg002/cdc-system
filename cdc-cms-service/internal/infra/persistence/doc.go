// Package persistence holds the GORM-backed implementations of the
// repository ports. This is the ONLY package allowed to contain raw SQL
// (`db.Raw`, `db.Exec`, schema-qualified table names). Upper layers
// (api, app, domain) must not touch *gorm.DB directly.
//
// Phase 2 v2 / P1 — placeholder. P4 (and parts of P2/P3) fill in concrete
// repos, migrating SQL out of the existing handlers.
package persistence
