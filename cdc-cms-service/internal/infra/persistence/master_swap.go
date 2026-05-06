package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/domain/job"
)

// MasterSwap kicks off an atomic RENAME swap of a master table. Used by
// Wizard step 11: after a v2 master has been populated and validated,
// swap it into place under the canonical name, retaining the old row as
// <name>_old_<ts> for rollback.
//
// P3.T3.6 (boss Q1=(a)): the HTTP handler returns 202 + JobID; the
// actual two-RENAME TX runs in a detached goroutine so a slow ALTER
// (lock contention) cannot block the API thread. Job status lands in
// `cdc_system.cdc_jobs` for the FE poll path.
type MasterSwap struct {
	db      *gorm.DB
	jobRepo ports.JobRepo
	logger  *zap.Logger
}

func NewMasterSwap(db *gorm.DB, jobRepo ports.JobRepo, logger *zap.Logger) *MasterSwap {
	return &MasterSwap{db: db, jobRepo: jobRepo, logger: logger}
}

// SwapAsync persists a `cdc_jobs` row, kicks off a detached goroutine
// that runs the in-process swap TX, and returns the JobID immediately.
// The caller (HTTP handler) replies 202 + JobID; the FE polls
// `GET /api/jobs/:id` until terminal.
//
// detectPartialState refuses dispatch if another master.swap for the
// same masterName is still pending/running — guards against operator
// double-clicks and protects the goroutine from racing on the same
// physical table.
//
// idempotencyKey (P3.T3.14): when set, JobRepo.Create dedups via the
// UNIQUE(idempotency_key) atomic upsert. Replaying the same key after
// a 202 returns the original JobID and skips re-spawning the goroutine.
func (s *MasterSwap) SwapAsync(ctx context.Context, masterName, newTableName, reason, createdBy, correlationID, idempotencyKey string) (string, error) {
	if err := validateIdent(masterName); err != nil {
		return "", fmt.Errorf("invalid master_name: %w", err)
	}
	if err := validateIdent(newTableName); err != nil {
		return "", fmt.Errorf("invalid new_table_name: %w", err)
	}
	if err := s.detectPartialState(ctx, masterName); err != nil {
		return "", err
	}

	payload, _ := json.Marshal(map[string]string{
		"master_name":    masterName,
		"new_table_name": newTableName,
		"reason":         reason,
	})
	j := &job.Job{
		ID:             uuid.NewString(),
		Type:           "master.swap",
		Status:         job.StatusPending,
		Payload:        payload,
		CreatedBy:      createdBy,
		CorrelationID:  correlationID,
		IdempotencyKey: idempotencyKey,
	}
	if err := s.jobRepo.Create(ctx, j); err != nil {
		return "", fmt.Errorf("create job: %w", err)
	}
	// If the upsert hit an existing row (replay), Create rehydrated j
	// from the DB. Skip spawning a second goroutine — the original
	// dispatch is already running or finished.
	if j.Status != job.StatusPending {
		return j.ID, nil
	}

	// Detach ctx — the HTTP request ctx is canceled when Fiber writes
	// 202; the goroutine must outlive that. Correlation rides on logger
	// only (no propagation needed across in-process boundary).
	go s.runSwapGoroutine(j.ID, masterName, newTableName, reason)

	return j.ID, nil
}

// detectPartialState refuses if any master.swap job for the same
// masterName is still pending/running (boss P3.T3.6 mandate). The
// generic StuckJobReaper (T3.12) sweeps stale 'running' rows on its
// own clock; this gate only blocks fresh duplicates from racing.
func (s *MasterSwap) detectPartialState(ctx context.Context, masterName string) error {
	var n int64
	err := s.db.WithContext(ctx).Raw(`
        SELECT count(*) FROM cdc_system.cdc_jobs
         WHERE type = 'master.swap'
           AND status IN ('pending','running')
           AND payload->>'master_name' = ?
    `, masterName).Scan(&n).Error
	if err != nil {
		return fmt.Errorf("partial-state probe: %w", err)
	}
	if n > 0 {
		return fmt.Errorf("master_swap_in_flight: %d job(s) still pending/running for %s", n, masterName)
	}
	return nil
}

func (s *MasterSwap) runSwapGoroutine(jobID, masterName, newTableName, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("master swap panic",
				zap.String("job_id", jobID),
				zap.String("master", masterName),
				zap.Any("panic", r))
			_ = s.jobRepo.UpdateStatus(ctx, jobID, job.StatusFailed, "", fmt.Sprintf("panic: %v", r))
		}
	}()

	if err := s.jobRepo.UpdateStatus(ctx, jobID, job.StatusRunning, "", ""); err != nil {
		s.logger.Warn("mark running failed", zap.String("job_id", jobID), zap.Error(err))
	}

	if err := s.runSwapTX(ctx, masterName, newTableName, reason); err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "lock timeout") || strings.Contains(errMsg, "canceling statement") {
			errMsg = "lock_timeout: " + errMsg
		}
		s.logger.Error("master swap failed",
			zap.String("job_id", jobID),
			zap.String("master", masterName),
			zap.String("new_table", newTableName),
			zap.Error(err))
		_ = s.jobRepo.UpdateStatus(ctx, jobID, job.StatusFailed, "", errMsg)
		return
	}

	result, _ := json.Marshal(map[string]string{
		"master_name":    masterName,
		"new_table_name": newTableName,
	})
	if err := s.jobRepo.UpdateStatus(ctx, jobID, job.StatusSuccess, string(result), ""); err != nil {
		s.logger.Warn("mark success failed", zap.String("job_id", jobID), zap.Error(err))
	}
}

// runSwapTX wraps the two RENAMEs + activity log INSERT inside one
// Postgres TX. SET LOCAL lock_timeout='3s' bounds the blocking window
// so a long-held ACCESS SHARE can't hang the goroutine indefinitely.
func (s *MasterSwap) runSwapTX(ctx context.Context, masterName, newTableName, reason string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '3s'").Error; err != nil {
			return fmt.Errorf("set lock_timeout: %w", err)
		}

		ts := time.Now().Unix()
		oldName := fmt.Sprintf("%s_old_%d", masterName, ts)

		renameCur := fmt.Sprintf(`ALTER TABLE public."%s" RENAME TO "%s"`, masterName, oldName)
		if err := tx.Exec(renameCur).Error; err != nil {
			return fmt.Errorf("rename current: %w", err)
		}
		renameNew := fmt.Sprintf(`ALTER TABLE public."%s" RENAME TO "%s"`, newTableName, masterName)
		if err := tx.Exec(renameNew).Error; err != nil {
			return fmt.Errorf("rename new: %w", err)
		}

		details, _ := json.Marshal(map[string]string{
			"old_table": oldName,
			"new_table": newTableName,
			"reason":    reason,
		})
		return tx.Exec(
			`INSERT INTO cdc_activity_log
			    (operation, target_table, status, details, triggered_by, started_at, completed_at)
			 VALUES ('master_swap', ?, 'success', ?::jsonb, 'manual', NOW(), NOW())`,
			masterName, string(details),
		).Error
	})
}
