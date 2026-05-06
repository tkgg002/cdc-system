// Package reconciliation holds the reconciliation read-model aggregates.
//
// CMS only READS reconciliation reports — the actual scan/drift compute is
// done by the worker reconciliation handler (`cdc.cmd.recon-check`). This
// package therefore focuses on the read shape that the FE consumes.
package reconciliation

import "time"

// DriftStatus categorises a reconciliation report.
type DriftStatus string

const (
	DriftOK                    DriftStatus = "ok"
	DriftWarning               DriftStatus = "warning"
	DriftError                 DriftStatus = "error"
	DriftDestMissing           DriftStatus = "dest_missing"
	DriftSourceMissingOrStale  DriftStatus = "source_missing_or_stale"
	DriftOKEmpty               DriftStatus = "ok_empty"
	DriftDrift                 DriftStatus = "drift"
)

// Report mirrors `cdc_reconciliation_report`.
type Report struct {
	ID            int64
	TargetTable   string
	SourceDB      string
	SourceCount   int64
	DestCount     int64
	Diff          int64
	MissingCount  int
	StaleCount    int
	CheckType     string
	Status        DriftStatus
	Tier          int
	DurationMs    *int
	ErrorMessage  *string
	CheckedAt     time.Time
	HealedAt      *time.Time
	HealedCount   int
}

// Filter narrows report listing.
type Filter struct {
	TargetTable string
	Status      DriftStatus
	Tier        *int
	Since       *time.Time
	Limit       int
}
