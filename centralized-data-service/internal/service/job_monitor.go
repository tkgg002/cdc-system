// Package service — job_monitor.go
//
// Track D Hardening (P4 / D-39.A) — event-driven close-loop for
// cdc_system.transmute_schedule. Architect ruling: the TransmuteHandler
// MUST NOT update schedule rows directly. Instead it publishes
// cdc.evt.transmute.completed; this monitor subscribes and writes
// last_status / last_stats / last_error.
//
// Phase D (Q3) — additionally bridges the FIRST successful transmute
// tick to the provisioning orchestrator. When a tick reports
// status='success' for a master_table, every source whose
// provisioning_state='schedule_pending' AND is active-bound to that
// master gets a cdc.evt.provisioning.step_completed published, which
// the orchestrator finalizes schedule_pending → running.
//
// Idempotent: only updates schedule rows currently in 'running'; the
// orchestrator's own CAS ensures the schedule_pending → running flip
// happens at most once.
package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SubjectProvisioningStepCompleted re-declared locally to avoid an
// import cycle (handler -> service is allowed; service -> handler is
// not). Wire-level constant must match handler.SubjectProvisioningStepCompleted.
const subjectProvisioningStepCompleted = "cdc.evt.provisioning.step_completed"

// JobMonitor closes the loop on transmute_schedule rows AND (Phase D
// Q3) bridges the first successful transmute tick to the provisioning
// orchestrator.
type JobMonitor struct {
	db     *gorm.DB
	nats   *nats.Conn // optional — nil disables the schedule_enable bridge
	logger *zap.Logger
}

func NewJobMonitor(db *gorm.DB, logger *zap.Logger) *JobMonitor {
	return &JobMonitor{db: db, logger: logger}
}

// SetNATSConn injects the NATS connection used to publish
// cdc.evt.provisioning.step_completed (Phase D Q3 bridge). Optional —
// installations without provisioning enabled can leave it unset.
func (m *JobMonitor) SetNATSConn(conn *nats.Conn) {
	m.nats = conn
}

type transmuteCompletedEvent struct {
	ScheduleID    int64           `json:"schedule_id"`
	CorrelationID string          `json:"correlation_id"`
	MasterTable   string          `json:"master_table"`
	Status        string          `json:"status"`
	Stats         json.RawMessage `json:"stats"`
	Error         string          `json:"error"`
	CompletedAt   string          `json:"completed_at"`
}

// HandleCompleted is the NATS callback for cdc.evt.transmute.completed.
// Safe to register on multiple subscribers — UPDATE is idempotent via
// the WHERE last_status='running' guard.
func (m *JobMonitor) HandleCompleted(msg *nats.Msg) {
	var ev transmuteCompletedEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		m.logger.Warn("job monitor: bad payload", zap.Error(err))
		return
	}
	if ev.ScheduleID == 0 {
		// Ad-hoc transmute (manual API trigger, sinkworker hook, etc.)
		// — no schedule row to close.
		return
	}
	statsJSON := string(ev.Stats)
	if statsJSON == "" {
		statsJSON = "{}"
	}
	ctx := context.Background()
	if err := m.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.transmute_schedule
		    SET last_status = ?,
		        last_stats  = ?::jsonb,
		        last_error  = NULLIF(?, ''),
		        updated_at  = NOW()
		  WHERE id = ?
		    AND last_status = 'running'`,
		ev.Status, statsJSON, ev.Error, ev.ScheduleID).Error; err != nil {
		m.logger.Warn("job monitor: update failed",
			zap.Int64("schedule_id", ev.ScheduleID),
			zap.String("status", ev.Status),
			zap.Error(err))
		return
	}
	m.logger.Info("job monitor: schedule closed",
		zap.Int64("schedule_id", ev.ScheduleID),
		zap.String("status", ev.Status),
		zap.String("master", ev.MasterTable))

	// Phase D (Q3) — schedule_enable bridge. Only on FIRST success.
	if ev.Status == "success" {
		m.bridgeScheduleEnable(ctx, ev.MasterTable, ev.CorrelationID)
	}
}

// bridgeScheduleEnable finds sources awaiting schedule confirmation
// for this master_table and publishes step_completed for each. The
// orchestrator's CAS guard makes this idempotent — a second call when
// the source has already moved past schedule_pending is a no-op
// (orchestrator returns ErrConflict, which the subscriber logs INFO).
func (m *JobMonitor) bridgeScheduleEnable(ctx context.Context, masterTable, parentCorr string) {
	if m.nats == nil {
		return
	}
	if masterTable == "" {
		return
	}
	type pendingRow struct {
		SourceID int64 `gorm:"column:source_id"`
	}
	var rows []pendingRow
	if err := m.db.WithContext(ctx).Raw(
		`SELECT sor.id AS source_id
		   FROM cdc_system.source_object_registry sor
		   JOIN cdc_system.master_binding mb
		     ON mb.source_object_id = sor.id
		    AND mb.is_active = true
		  WHERE sor.provisioning_state = 'schedule_pending'
		    AND mb.master_table = ?
		  LIMIT 50`, masterTable).Scan(&rows).Error; err != nil {
		m.logger.Warn("job monitor: schedule_enable bridge query failed",
			zap.String("master", masterTable), zap.Error(err))
		return
	}
	if len(rows) == 0 {
		return
	}
	// Architect Q3 refine — multi-source merge into a single master
	// will fan-out to every pending source. CAS guard at the
	// orchestrator side absorbs duplicates, but we surface the count
	// so operators can spot a master with anomalous fan-out.
	m.logger.Info("job monitor: schedule_enable bridge fan-out",
		zap.String("master", masterTable),
		zap.Int("impacted_sources", len(rows)),
		zap.String("correlation_id", parentCorr))
	for _, r := range rows {
		payload := map[string]any{
			"source_id":      r.SourceID,
			"step":           "schedule_enable",
			"success":        true,
			"correlation_id": parentCorr,
			"actor":          "job_monitor",
			"completed_at":   time.Now().UTC().Format(time.RFC3339Nano),
		}
		body, _ := json.Marshal(payload)
		if pErr := m.nats.Publish(subjectProvisioningStepCompleted, body); pErr != nil {
			m.logger.Warn("job monitor: schedule_enable bridge publish failed",
				zap.Int64("source_id", r.SourceID),
				zap.String("master", masterTable),
				zap.Error(pErr))
			continue
		}
		m.logger.Info("job monitor: schedule_enable bridge fired",
			zap.Int64("source_id", r.SourceID),
			zap.String("master", masterTable),
			zap.String("correlation_id", parentCorr))
	}
}
