package model

import (
	"encoding/json"
	"time"
)

// Alert mirrors one row of `cdc_alerts` (see migrations/013_alerts.sql).
//
// The table is the canonical state store for observability alerts. The
// fingerprint column is a stable hash of name + sorted(labels) and is the
// natural key used by `service.AlertManager` for dedup across collector ticks.
//
// JSON tags are deliberately stable — the FE alert banner consumes them
// verbatim via `GET /api/alerts/active`. Fields are ordered to match the
// column order of the migration for readability.
type Alert struct {
	ID              string          `gorm:"column:id;primaryKey"        json:"id"`
	Fingerprint     string          `gorm:"column:fingerprint;uniqueIndex" json:"fingerprint"`
	Name            string          `gorm:"column:name"                 json:"name"`
	Severity        string          `gorm:"column:severity"             json:"severity"`
	Labels          json.RawMessage `gorm:"column:labels;type:jsonb"    json:"labels,omitempty"`
	Description     string          `gorm:"column:description"          json:"description"`
	Status          string          `gorm:"column:status"               json:"status"`
	FiredAt         time.Time       `gorm:"column:fired_at"             json:"fired_at"`
	ResolvedAt      *time.Time      `gorm:"column:resolved_at"          json:"resolved_at,omitempty"`
	AckBy           *string         `gorm:"column:ack_by"               json:"ack_by,omitempty"`
	AckAt           *time.Time      `gorm:"column:ack_at"               json:"ack_at,omitempty"`
	SilencedBy      *string         `gorm:"column:silenced_by"          json:"silenced_by,omitempty"`
	SilencedUntil   *time.Time      `gorm:"column:silenced_until"       json:"silenced_until,omitempty"`
	SilenceReason   *string         `gorm:"column:silence_reason"       json:"silence_reason,omitempty"`
	OccurrenceCount int             `gorm:"column:occurrence_count"     json:"occurrence_count"`
	LastFiredAt     time.Time       `gorm:"column:last_fired_at"        json:"last_fired_at"`
}

// TableName binds the struct to the migrated table.
func (Alert) TableName() string { return "cdc_alerts" }

// Alert status values. These mirror the TEXT column domain; keep the list
// explicit rather than using an enum type so migrations remain cheap.
const (
	AlertStatusFiring       = "firing"
	AlertStatusResolved     = "resolved"
	AlertStatusAcknowledged = "acknowledged"
	AlertStatusSilenced     = "silenced"
)

// Severity values. Info is reserved for future non-actionable notifications;
// the collector currently emits only warning or critical.
const (
	AlertSeverityInfo     = "info"
	AlertSeverityWarning  = "warning"
	AlertSeverityCritical = "critical"
)
