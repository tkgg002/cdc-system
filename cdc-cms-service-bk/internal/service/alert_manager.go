// Package service — alert_manager.go (Phase 6).
//
// Purpose: encapsulate the state machine behind `cdc_alerts` so the health
// collector can Fire/Resolve conditions while API handlers can Ack/Silence
// them, without touching raw SQL. Dedup & silence are the two things that
// prevent the FE banner from turning into a flapping nightmare during an
// incident; everything else in this file is bookkeeping around that.
//
// Key design choices:
//   - Fingerprint = sha256(name + sorted(labels)). Labels are normalized by
//     sorting keys + joining as k=v|k=v so reordering never produces a new
//     row. This is the natural key enforced by a UNIQUE index in migration 013.
//   - Fire() is idempotent: repeated calls in a short window only bump
//     `last_fired_at` + `occurrence_count`; optional Redis `alert:<fp>:last_fire`
//     key with TTL 5m gates downstream notification fan-out so we never emit
//     more than one webhook per 5 minutes per fingerprint.
//   - Silenced alerts whose deadline is in the future short-circuit in Fire()
//     *before* the write — we never overwrite a silence with a firing status.
//   - Ack keeps the row visible (status='acknowledged') so operators know
//     somebody owns it; the FE filters it out of the primary banner and shows
//     it in a secondary "Owned" panel.
//   - Resolve is driven by the collector when the condition clears. The BG
//     resolver (ResolveStale / ReopenSilences / ExpireResolved) keeps the
//     table honest when collectors or operators disappear mid-flight.
//
// Security:
//   - The package never returns the `silenced_by` / `ack_by` strings to
//     anonymous callers; handlers apply RBAC.
//   - Labels may contain sensitive data. The fingerprint is a one-way hash so
//     it is safe to embed in URLs; the labels JSONB itself is only returned to
//     authenticated callers (see api/alerts_handler.go).
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"cdc-cms-service/internal/model"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// FireRequest is the input to AlertManager.Fire. We pass a struct rather than
// a positional signature because the set of attributes will grow (annotations,
// runbook URL, ...) and positional churn is painful across call sites.
type FireRequest struct {
	Name        string            // e.g. "DebeziumConnectorFailed"
	Severity    string            // info | warning | critical
	Labels      map[string]string // low-cardinality dimensions (component, connector, ...)
	Description string            // human-readable sentence
}

// AlertManager owns the state machine.
type AlertManager struct {
	db             *gorm.DB
	redis          *rediscache.RedisCache // optional; nil => skip notification dedup
	logger         *zap.Logger
	notifyInterval time.Duration // dedup window for notifications
	autoHideAfter  time.Duration // resolved rows are hidden from active after this
}

// NewAlertManager wires the manager. `redis` may be nil (tests / low-mem envs);
// notification dedup then becomes a DB-only check on `last_fired_at`.
func NewAlertManager(db *gorm.DB, redis *rediscache.RedisCache, logger *zap.Logger) *AlertManager {
	return &AlertManager{
		db:             db,
		redis:          redis,
		logger:         logger,
		notifyInterval: 5 * time.Minute,
		autoHideAfter:  60 * time.Second,
	}
}

// NotifyInterval returns the dedup window used to gate outbound notifications.
func (m *AlertManager) NotifyInterval() time.Duration { return m.notifyInterval }

// AutoHideAfter returns the window after which a resolved alert is no longer
// rendered in the active list.
func (m *AlertManager) AutoHideAfter() time.Duration { return m.autoHideAfter }

// Fingerprint computes the stable hash for a (name, labels) tuple. Exported
// so tests + callers who already know the key can reuse the same function.
func Fingerprint(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	h.Write([]byte(name))
	for _, k := range keys {
		h.Write([]byte{0x1f}) // unit separator — avoids accidental concat collisions
		h.Write([]byte(k))
		h.Write([]byte{0x1e})
		h.Write([]byte(labels[k]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FireResult summarizes what Fire() did. Useful for tests + metrics.
type FireResult struct {
	Fingerprint      string
	Created          bool // true when the row was inserted for the first time
	Suppressed       bool // true when an active silence short-circuited the write
	NotifySuppressed bool // true when dedup window blocked notification (row still updated)
	Occurrences      int  // post-update count
}

// Fire ingests a condition. It returns a non-nil error only on hard failures
// (DB unreachable, marshal error). Silences are *not* errors — they are
// signaled via FireResult.Suppressed so callers can record metrics without
// spamming the error log.
func (m *AlertManager) Fire(ctx context.Context, req FireRequest) (FireResult, error) {
	if req.Name == "" || req.Severity == "" {
		return FireResult{}, errors.New("alert name and severity are required")
	}

	fp := Fingerprint(req.Name, req.Labels)
	res := FireResult{Fingerprint: fp}

	labelsJSON, err := marshalLabels(req.Labels)
	if err != nil {
		return res, fmt.Errorf("marshal labels: %w", err)
	}

	now := time.Now().UTC()

	// Load existing row (if any) under a row-level lock so concurrent Fires
	// don't race on occurrence_count.
	tx := m.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return res, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
	}()

	var existing model.Alert
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("fingerprint = ?", fp).
		First(&existing).Error

	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		// New alert → insert firing row.
		// ID is generated client-side so GORM doesn't send an empty-string
		// placeholder to the UUID column. The DB default gen_random_uuid()
		// would also work, but only if we omit the field from the INSERT —
		// GORM sends it regardless, hence explicit assignment.
		newRow := model.Alert{
			ID:              uuid.NewString(),
			Fingerprint:     fp,
			Name:            req.Name,
			Severity:        req.Severity,
			Labels:          labelsJSON,
			Description:     req.Description,
			Status:          model.AlertStatusFiring,
			FiredAt:         now,
			OccurrenceCount: 1,
			LastFiredAt:     now,
		}
		if err := tx.Create(&newRow).Error; err != nil {
			tx.Rollback()
			return res, fmt.Errorf("insert alert: %w", err)
		}
		res.Created = true
		res.Occurrences = 1

	case err != nil:
		tx.Rollback()
		return res, fmt.Errorf("lookup alert: %w", err)

	default:
		// Existing row. Silence check comes first — we never overwrite a live silence.
		if existing.Status == model.AlertStatusSilenced &&
			existing.SilencedUntil != nil && existing.SilencedUntil.After(now) {
			tx.Rollback()
			res.Suppressed = true
			return res, nil
		}

		// Transition back to firing (handles resolved→firing flap + ack→firing refire).
		updates := map[string]any{
			"status":           model.AlertStatusFiring,
			"last_fired_at":    now,
			"occurrence_count": existing.OccurrenceCount + 1,
			// Refresh metadata (severity could have escalated).
			"severity":    req.Severity,
			"description": req.Description,
			"labels":      labelsJSON,
		}
		if existing.Status == model.AlertStatusResolved {
			// Treat this as a fresh firing for the banner, but keep the same row
			// (fingerprint is unique). Re-stamp fired_at so operators see the
			// "new" onset timestamp.
			updates["fired_at"] = now
			updates["resolved_at"] = nil
			updates["ack_by"] = nil
			updates["ack_at"] = nil
		}
		if err := tx.Model(&model.Alert{}).
			Where("fingerprint = ?", fp).
			Updates(updates).Error; err != nil {
			tx.Rollback()
			return res, fmt.Errorf("update alert: %w", err)
		}
		res.Occurrences = existing.OccurrenceCount + 1
	}

	if err := tx.Commit().Error; err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}

	// Notification dedup (optional Redis). If the key is still present the
	// window hasn't elapsed and we tell the caller not to notify. If Redis is
	// absent we always allow notify (collector ticks are already ≥5s so
	// spam risk is bounded).
	if m.redis != nil {
		key := "alert:" + fp + ":last_fire"
		_, gerr := m.redis.Get(ctx, key)
		if gerr == nil {
			res.NotifySuppressed = true
		} else {
			// Set (overwrite) regardless of error class — if Get failed with a
			// transport error Set will too and we'll log but not fail Fire.
			if serr := m.redis.Set(ctx, key, "1", m.notifyInterval); serr != nil {
				m.logger.Debug("notification dedup cache write failed",
					zap.String("fingerprint", fp), zap.Error(serr))
			}
		}
	}
	return res, nil
}

// Resolve marks a firing/acknowledged alert as resolved. Calling Resolve on a
// row that is already resolved or absent is a no-op (return value indicates
// whether a row was touched).
func (m *AlertManager) Resolve(ctx context.Context, fingerprint string) (bool, error) {
	if fingerprint == "" {
		return false, errors.New("fingerprint required")
	}
	now := time.Now().UTC()
	res := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("fingerprint = ? AND status IN ?", fingerprint,
			[]string{model.AlertStatusFiring, model.AlertStatusAcknowledged}).
		Updates(map[string]any{
			"status":      model.AlertStatusResolved,
			"resolved_at": now,
		})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected > 0, nil
}

// Ack transitions a firing alert to acknowledged. The alert stays visible
// (in a separate bucket) so operators know it is owned.
func (m *AlertManager) Ack(ctx context.Context, fingerprint, user string) error {
	if fingerprint == "" || user == "" {
		return errors.New("fingerprint and user required")
	}
	now := time.Now().UTC()
	res := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("fingerprint = ? AND status = ?", fingerprint, model.AlertStatusFiring).
		Updates(map[string]any{
			"status": model.AlertStatusAcknowledged,
			"ack_by": user,
			"ack_at": now,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("alert not firing or not found")
	}
	return nil
}

// Silence suppresses an alert until `until`. A non-empty `reason` is required
// (enforced here rather than at the HTTP layer so every caller benefits).
func (m *AlertManager) Silence(ctx context.Context, fingerprint, user string, until time.Time, reason string) error {
	if fingerprint == "" || user == "" {
		return errors.New("fingerprint and user required")
	}
	if until.Before(time.Now()) {
		return errors.New("silence deadline must be in the future")
	}
	if reason == "" {
		return errors.New("silence reason required")
	}
	res := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("fingerprint = ?", fingerprint).
		Updates(map[string]any{
			"status":         model.AlertStatusSilenced,
			"silenced_by":    user,
			"silenced_until": until.UTC(),
			"silence_reason": reason,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("alert not found")
	}
	return nil
}

// ListActive returns firing + acknowledged alerts, ordered by severity
// (critical first) then recency. Resolved/silenced rows are excluded; the
// handler exposes those via dedicated routes.
//
// Resolved rows that slipped through `autoHideAfter` are *also* excluded so
// the FE doesn't briefly flash a resolved banner between collector ticks.
func (m *AlertManager) ListActive(ctx context.Context) ([]model.Alert, error) {
	var out []model.Alert
	err := m.db.WithContext(ctx).
		Where("status IN ?", []string{model.AlertStatusFiring, model.AlertStatusAcknowledged}).
		Order("CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END").
		Order("fired_at DESC").
		Find(&out).Error
	return out, err
}

// ListSilenced returns alerts whose silence is still active.
func (m *AlertManager) ListSilenced(ctx context.Context) ([]model.Alert, error) {
	var out []model.Alert
	err := m.db.WithContext(ctx).
		Where("status = ? AND silenced_until > NOW()", model.AlertStatusSilenced).
		Order("silenced_until DESC").
		Find(&out).Error
	return out, err
}

// ListHistory returns resolved/closed alerts within a time window. `from`/`to`
// are inclusive; either may be zero to mean "unbounded on that side".
func (m *AlertManager) ListHistory(ctx context.Context, from, to time.Time, limit int) ([]model.Alert, error) {
	q := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("status = ?", model.AlertStatusResolved)
	if !from.IsZero() {
		q = q.Where("resolved_at >= ?", from.UTC())
	}
	if !to.IsZero() {
		q = q.Where("resolved_at <= ?", to.UTC())
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var out []model.Alert
	err := q.Order("resolved_at DESC").Limit(limit).Find(&out).Error
	return out, err
}

// ReopenExpiredSilences finds silences whose deadline has passed and flips
// them back to firing so the next collector tick can re-evaluate the condition.
// Returns the number of rows updated.
func (m *AlertManager) ReopenExpiredSilences(ctx context.Context) (int64, error) {
	now := time.Now().UTC()
	res := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("status = ? AND silenced_until IS NOT NULL AND silenced_until <= ?",
			model.AlertStatusSilenced, now).
		Updates(map[string]any{
			"status":         model.AlertStatusFiring,
			"silenced_until": nil,
			"last_fired_at":  now,
		})
	return res.RowsAffected, res.Error
}

// ResolveStale auto-resolves firing alerts that haven't been re-fired for
// `olderThan` (default 24h). Used by the BG resolver so crashed collectors
// can't leave the banner stuck forever.
func (m *AlertManager) ResolveStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	if olderThan <= 0 {
		olderThan = 24 * time.Hour
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	res := m.db.WithContext(ctx).
		Model(&model.Alert{}).
		Where("status = ? AND last_fired_at < ?", model.AlertStatusFiring, cutoff).
		Updates(map[string]any{
			"status":         model.AlertStatusResolved,
			"resolved_at":    time.Now().UTC(),
			"silence_reason": "auto-resolved: stale",
		})
	return res.RowsAffected, res.Error
}

// RunBackgroundResolver ticks once a minute and performs the three
// housekeeping chores (reopen silences / resolve stale / the "hide after 60s"
// behaviour is implemented in ListActive via the status filter, so nothing
// is strictly required here for that — but we still bump metrics).
func (m *AlertManager) RunBackgroundResolver(ctx context.Context) {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("alert background resolver stopped")
			return
		case <-t.C:
			if n, err := m.ReopenExpiredSilences(ctx); err != nil {
				m.logger.Warn("reopen silences failed", zap.Error(err))
			} else if n > 0 {
				m.logger.Info("reopened expired silences", zap.Int64("count", n))
			}
			if n, err := m.ResolveStale(ctx, 24*time.Hour); err != nil {
				m.logger.Warn("resolve stale failed", zap.Error(err))
			} else if n > 0 {
				m.logger.Info("auto-resolved stale firing alerts", zap.Int64("count", n))
			}
		}
	}
}

// marshalLabels returns a JSON body for the labels column. Nil/empty map
// becomes JSON "{}" rather than SQL NULL so JSONB queries work without a
// COALESCE everywhere.
func marshalLabels(labels map[string]string) (json.RawMessage, error) {
	if labels == nil {
		return json.RawMessage(`{}`), nil
	}
	b, err := json.Marshal(labels)
	if err != nil {
		return nil, err
	}
	return b, nil
}
