// activity_logger.go — single owner of cdc_activity_log writes/reads.
//
// Phase 2 T13 extracted this from per-handler inline `db.Create(&model.
// ActivityLog{...})` calls. Two reasons:
//  1. Removed the `model.ActivityLog{` literal from internal/api/* so the
//     handlers no longer reach into the persistence model directly.
//  2. Gave us one place to evolve the audit-log schema (e.g. the next
//     time we want to attach trace IDs or correlation headers, only this
//     file changes).
//
// Two write semantics on purpose:
//   - Log()      — synchronous, returns the gorm error so a caller that
//                  treats the audit row as part of the request contract
//                  (e.g. ReconciliationHandler.TriggerCheck) can surface
//                  failure to the operator.
//   - LogAsync() — fire-and-forget; spawns a goroutine, swallows the
//                  error after a Warn. Used by the hot path handlers
//                  where blocking on a write that lands in a partitioned
//                  table is not acceptable.
//
// The async path uses context.Background() so an HTTP request that
// returns 200 doesn't cancel the audit write while it's mid-flight. The
// caller is responsible for not handing in mutable state that races.
package persistence

import (
	"context"
	"encoding/json"
	"time"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ActivityEntry is the shape every caller fills in. TriggeredBy defaults
// to "manual" when empty so the existing wire contract stays stable
// (the FE filters by `triggered_by` and treats "manual" as the user-
// initiated case).
type ActivityEntry struct {
	Operation   string
	TargetTable string
	Status      string
	Details     map[string]any
	ErrorMsg    string
	TriggeredBy string
}

// ActivityFilter narrows ListActivityLogs queries. All fields optional;
// zero value means "no filter on this column".
type ActivityFilter struct {
	TargetTable string
	Operation   string
	Since       *time.Time
	Limit       int
}

// ActivityLogger is the single concrete impl. Constructed once at boot
// and shared across handlers (it is goroutine-safe because gorm.DB is).
type ActivityLogger struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewActivityLogger(db *gorm.DB, logger *zap.Logger) *ActivityLogger {
	return &ActivityLogger{db: db, logger: logger}
}

// Log writes synchronously and returns the gorm error. Callers who need
// to surface the audit failure to the operator (rare — recon dispatch is
// the canonical example) use this path.
func (l *ActivityLogger) Log(ctx context.Context, e ActivityEntry) error {
	if l == nil {
		return nil
	}
	row := l.buildRow(e)
	return l.db.WithContext(ctx).Create(&row).Error
}

// LogAsync writes in a goroutine using context.Background() so the audit
// row lands even if the originating HTTP request has already returned.
// Errors are logged at Warn — never returned. Use this on the hot path.
func (l *ActivityLogger) LogAsync(e ActivityEntry) {
	if l == nil {
		return
	}
	row := l.buildRow(e)
	go func() {
		if err := l.db.WithContext(context.Background()).Create(&row).Error; err != nil {
			l.logger.Warn("activity log write failed",
				zap.String("operation", e.Operation),
				zap.String("target_table", e.TargetTable),
				zap.Error(err))
		}
	}()
}

// ListActivityLogs powers DispatchStatus and any future operator-facing
// audit query. Returning the model type is fine here — it's a service-
// to-handler boundary, the wire contract is owned by the handler.
func (l *ActivityLogger) ListActivityLogs(ctx context.Context, f ActivityFilter) ([]model.ActivityLog, error) {
	if l == nil {
		return nil, nil
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	q := l.db.WithContext(ctx).Model(&model.ActivityLog{})
	if f.TargetTable != "" {
		q = q.Where("target_table = ?", f.TargetTable)
	}
	if f.Operation != "" {
		q = q.Where("operation = ?", f.Operation)
	}
	if f.Since != nil {
		q = q.Where("started_at >= ?", *f.Since)
	}
	var rows []model.ActivityLog
	if err := q.Order("started_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (l *ActivityLogger) buildRow(e ActivityEntry) model.ActivityLog {
	detailsJSON, _ := json.Marshal(e.Details)
	now := time.Now()
	triggeredBy := e.TriggeredBy
	if triggeredBy == "" {
		triggeredBy = "manual"
	}
	var errPtr *string
	if e.ErrorMsg != "" {
		s := e.ErrorMsg
		errPtr = &s
	}
	return model.ActivityLog{
		Operation:    e.Operation,
		TargetTable:  e.TargetTable,
		Status:       e.Status,
		Details:      detailsJSON,
		ErrorMessage: errPtr,
		TriggeredBy:  triggeredBy,
		StartedAt:    now,
		CompletedAt:  &now,
	}
}
