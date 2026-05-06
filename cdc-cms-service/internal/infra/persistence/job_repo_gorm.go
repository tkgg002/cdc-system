// Package persistence — GORM-backed job tracker adapter.
//
// job_repo_gorm.go implements ports.JobRepo against
// cdc_system.cdc_jobs (migration 052_create_cdc_jobs.sql).
// The CommandBus (internal/infra/messaging/nats_command_bus.go) writes
// rows here on Dispatch; the worker's JobMonitor closes them out via
// the wildcard `cdc.evt.*.completed` subscription (Phase 2 v2 / P3).
package persistence

import (
	"context"
	"errors"
	"time"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/job"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type jobRepoGorm struct {
	db *gorm.DB
}

// NewJobRepo constructs the GORM-backed adapter for ports.JobRepo.
func NewJobRepo(db *gorm.DB) ports.JobRepo {
	return &jobRepoGorm{db: db}
}

// jobRow is the flat scan target. Mirrors `cdc_system.cdc_jobs` 1:1; the
// domain entity is project'd by toDomain.
type jobRow struct {
	ID             string         `gorm:"column:id;primaryKey"`
	Type           string         `gorm:"column:type"`
	Status         string         `gorm:"column:status"`
	Payload        []byte         `gorm:"column:payload"`
	Result         []byte         `gorm:"column:result"`
	ErrorMessage   string         `gorm:"column:error_message"`
	IdempotencyKey *string        `gorm:"column:idempotency_key"`
	CreatedBy      string         `gorm:"column:created_by"`
	CorrelationID  *string        `gorm:"column:correlation_id"`
	CreatedAt      time.Time      `gorm:"column:created_at"`
	StartedAt      *time.Time     `gorm:"column:started_at"`
	FinishedAt     *time.Time     `gorm:"column:finished_at"`
}

func (jobRow) TableName() string { return "cdc_system.cdc_jobs" }

func toRow(j *job.Job) *jobRow {
	r := &jobRow{
		ID:           j.ID,
		Type:         j.Type,
		Status:       string(j.Status),
		Payload:      []byte(j.Payload),
		Result:       []byte(j.Result),
		ErrorMessage: j.ErrorMessage,
		CreatedBy:    j.CreatedBy,
		CreatedAt:    j.CreatedAt,
		StartedAt:    j.StartedAt,
		FinishedAt:   j.FinishedAt,
	}
	if j.IdempotencyKey != "" {
		k := j.IdempotencyKey
		r.IdempotencyKey = &k
	}
	if j.CorrelationID != "" {
		c := j.CorrelationID
		r.CorrelationID = &c
	}
	return r
}

func toDomain(r *jobRow) *job.Job {
	j := &job.Job{
		ID:           r.ID,
		Type:         r.Type,
		Status:       job.Status(r.Status),
		Payload:      r.Payload,
		Result:       r.Result,
		ErrorMessage: r.ErrorMessage,
		CreatedBy:    r.CreatedBy,
		CreatedAt:    r.CreatedAt,
		StartedAt:    r.StartedAt,
		FinishedAt:   r.FinishedAt,
	}
	if r.IdempotencyKey != nil {
		j.IdempotencyKey = *r.IdempotencyKey
	}
	if r.CorrelationID != nil {
		j.CorrelationID = *r.CorrelationID
	}
	return j
}

// Create persists a pending Job. If the caller did not assign an ID, we
// stamp a fresh UUID — the DB default would also produce one, but having
// it Go-side lets the dispatcher return the id immediately to the API.
//
// Idempotency (G2 race fix, P3.T3.13): when IdempotencyKey is set we
// emit `INSERT … ON CONFLICT (idempotency_key) DO NOTHING` so two
// concurrent callers with the same key cannot both INSERT — the second
// one no-ops at the storage layer. A second SELECT then rehydrates the
// in-memory Job from whichever row won (theirs or ours). This closes
// the read-then-create window the prior implementation had.
func (r *jobRepoGorm) Create(ctx context.Context, j *job.Job) error {
	if j == nil {
		return errors.New("job is nil")
	}
	if j.ID == "" {
		j.ID = uuid.NewString()
	}
	if j.Status == "" {
		j.Status = job.StatusPending
	}

	row := toRow(j)
	if row.CreatedAt.IsZero() {
		row.CreatedAt = time.Now().UTC()
	}

	if j.IdempotencyKey != "" {
		// Atomic upsert. DoNothing leaves `row` with the values we
		// passed in even on conflict; the Take below pulls back the
		// authoritative DB state.
		if err := r.db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "idempotency_key"}},
				DoNothing: true,
			}).
			Create(row).Error; err != nil {
			return err
		}
		var existing jobRow
		if err := r.db.WithContext(ctx).
			Where("idempotency_key = ?", j.IdempotencyKey).
			Take(&existing).Error; err != nil {
			return err
		}
		*j = *toDomain(&existing)
		return nil
	}

	if err := r.db.WithContext(ctx).Create(row).Error; err != nil {
		return err
	}
	*j = *toDomain(row)
	return nil
}

// GetByID returns the Job or gorm.ErrRecordNotFound. The query handler
// translates that to HTTP 404.
func (r *jobRepoGorm) GetByID(ctx context.Context, id string) (*job.Job, error) {
	var row jobRow
	if err := r.db.WithContext(ctx).Where("id = ?", id).Take(&row).Error; err != nil {
		return nil, err
	}
	return toDomain(&row), nil
}

// UpdateStatus is the close-loop transition called by JobMonitor when a
// `cdc.evt.*.completed` arrives. It also stamps finished_at when the
// transition is terminal (success/failed).
//
// Idempotent guard: only rows currently in pending|running are updated —
// duplicates from JetStream redelivery become a no-op.
func (r *jobRepoGorm) UpdateStatus(ctx context.Context, id string, s job.Status, result, errMsg string) error {
	updates := map[string]interface{}{
		"status":        string(s),
		"error_message": errMsg,
	}
	if len(result) > 0 {
		updates["result"] = []byte(result)
	}
	switch s {
	case job.StatusRunning:
		updates["started_at"] = time.Now().UTC()
	case job.StatusSuccess, job.StatusFailed:
		updates["finished_at"] = time.Now().UTC()
	}

	res := r.db.WithContext(ctx).
		Table("cdc_system.cdc_jobs").
		Where("id = ?", id).
		Where("status IN ?", []string{string(job.StatusPending), string(job.StatusRunning)}).
		Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		// Row missing OR already terminal — surface ErrRecordNotFound so
		// callers can distinguish from a transport error.
		return gorm.ErrRecordNotFound
	}
	return nil
}

// ListPending returns up to `limit` jobs of the given type that are still
// pending. Used by recovery cron to flag stuck jobs (worker crash before
// transition to running). Ordered oldest-first so retry hits the head.
func (r *jobRepoGorm) ListPending(ctx context.Context, jtype string, limit int) ([]job.Job, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var rows []jobRow
	q := r.db.WithContext(ctx).
		Table("cdc_system.cdc_jobs").
		Where("status = ?", string(job.StatusPending)).
		Order("created_at ASC").
		Limit(limit)
	if jtype != "" {
		q = q.Where("type = ?", jtype)
	}
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]job.Job, 0, len(rows))
	for i := range rows {
		out = append(out, *toDomain(&rows[i]))
	}
	return out, nil
}
