package model

import (
	"encoding/json"
	"time"
)

type ReconciliationReport struct {
	ID           uint64          `gorm:"primaryKey" json:"id"`
	TargetTable  string          `gorm:"column:target_table;not null" json:"target_table"`
	SourceDB     string          `gorm:"column:source_db" json:"source_db"`
	// Migration 017 — SourceCount is pointer/nullable so we can distinguish
	// query failure (NULL) from actual empty window (0). Old call sites
	// that set 0 continue to work; NULL is only written when an error
	// prevents counting at all.
	SourceCount  *int64          `gorm:"column:source_count;default:0" json:"source_count"`
	DestCount    int64           `gorm:"column:dest_count;default:0" json:"dest_count"`
	Diff         int64           `gorm:"column:diff;default:0" json:"diff"`
	MissingCount int             `gorm:"column:missing_count;default:0" json:"missing_count"`
	MissingIDs   json.RawMessage `gorm:"column:missing_ids;type:jsonb" json:"missing_ids"`
	StaleCount   int             `gorm:"column:stale_count;default:0" json:"stale_count"`
	StaleIDs     json.RawMessage `gorm:"column:stale_ids;type:jsonb" json:"stale_ids"`
	CheckType    string          `gorm:"column:check_type;not null" json:"check_type"`
	Status       string          `gorm:"column:status;not null" json:"status"`
	Tier         int             `gorm:"column:tier;default:1" json:"tier"`
	DurationMs   *int            `gorm:"column:duration_ms" json:"duration_ms"`
	ErrorMessage *string         `gorm:"column:error_message" json:"error_message"`
	// Migration 017 — structured error category (SRC_TIMEOUT, SRC_CONNECTION,
	// SRC_FIELD_MISSING, etc.). Empty string / NULL for success rows.
	ErrorCode    string          `gorm:"column:error_code" json:"error_code"`
	CheckedAt    time.Time       `gorm:"column:checked_at" json:"checked_at"`
	HealedAt     *time.Time      `gorm:"column:healed_at" json:"healed_at"`
	HealedCount  int             `gorm:"column:healed_count;default:0" json:"healed_count"`
}

func (ReconciliationReport) TableName() string { return "cdc_reconciliation_report" }
