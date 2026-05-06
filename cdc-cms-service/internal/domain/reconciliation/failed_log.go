package reconciliation

import (
	"encoding/json"
	"time"
)

// FailedLogStatus reflects the lifecycle of a failed_sync_logs row.
type FailedLogStatus string

const (
	FailedLogFailed   FailedLogStatus = "failed"
	FailedLogRetrying FailedLogStatus = "retrying"
	FailedLogResolved FailedLogStatus = "resolved"
)

// FailedLog is the domain projection of one row in `failed_sync_logs`.
type FailedLog struct {
	ID             int64
	TargetTable    string
	SourceTable    string
	SourceDB       string
	RecordID       string
	Operation      string
	RawJSON        json.RawMessage
	ErrorMessage   string
	ErrorType      string
	KafkaTopic     string
	KafkaPartition *int
	KafkaOffset    *int64
	RetryCount     int
	MaxRetries     int
	Status         FailedLogStatus
	CreatedAt      time.Time
	LastRetryAt    *time.Time
	ResolvedAt     *time.Time
	ResolvedBy     *string
}

// LogFilter narrows failed_sync_logs lookup.
type LogFilter struct {
	Status      FailedLogStatus
	TargetTable string
	Since       *time.Time
	Limit       int
}
