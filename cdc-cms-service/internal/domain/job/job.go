// Package job holds the async-execution Job aggregate.
//
// A Job is created when CMS dispatches a command via NATS. The worker
// executes the heavy work and emits `cdc.evt.X.completed`; the JobMonitor
// service in the worker updates the row in `cdc_system.cdc_jobs` (created
// in Phase 2 v2 / P3 migration 036). CMS reads the row to surface progress
// to the FE via `GET /api/jobs/:id`.
package job

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle of a Job.
type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

// Job is the domain entity for `cdc_system.cdc_jobs`.
type Job struct {
	ID             string // UUID
	Type           string // e.g. "master.swap", "recon.check"
	Status         Status
	Payload        json.RawMessage
	Result         json.RawMessage
	ErrorMessage   string
	IdempotencyKey string
	CreatedBy      string
	CorrelationID  string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// New constructs a pending Job ready for the CommandBus to persist.
// CreatedAt is left zero so the persistence layer can stamp NOW() server-side.
func New(jtype string, payload json.RawMessage, createdBy, correlationID string) *Job {
	return &Job{
		Type:          jtype,
		Status:        StatusPending,
		Payload:       payload,
		CreatedBy:     createdBy,
		CorrelationID: correlationID,
	}
}
