// Package service — provisioning_orchestrator.go
//
// Orchestrator for the Source Provisioning Mode flow.
// Wires the pure state machine (provisioning_state_machine.go) to
// Postgres + NATS. Architect rulings (04_decisions_provisioning_mode.md)
// govern every operation here:
//
//   D1 — SetMode manual→auto must kick Advance immediately.
//   D2 — Retry re-fires CURRENT step (uses last from_state in step_log).
//   D3 — Pending TTL = 10 minutes; RecoveryLoop marks TIMEOUT_EXCEEDED.
//   D4 — `provisioned` is legacy-only, NOT in Transitions. Don't advance it.
//   D6 — Every state UPDATE is CAS: WHERE provisioning_state = expected.
//   D7 — provisioning_step_log capped at 50 entries via PG helper
//        cdc_system.append_step_log_capped (migration 048).
//   D8 — NATS payload carries trace_id + span_id from the active OTel span.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"centralized-data-service/internal/naming"
)

// Public knobs (D3 / D7) — env-overridable at process boot.
var (
	ProvisioningPendingTTL        = 10 * time.Minute
	ProvisioningStepLogMaxEntries = 50
	ProvisioningRecoveryInterval  = time.Minute
)

func init() {
	if v := os.Getenv("PROVISIONING_PENDING_TTL_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ProvisioningPendingTTL = time.Duration(n) * time.Minute
		}
	}
	if v := os.Getenv("PROVISIONING_STEP_LOG_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ProvisioningStepLogMaxEntries = n
		}
	}
}

// ErrConflict — CAS guard rejected the UPDATE because another worker
// changed the row first. Callers (API handlers, NATS callbacks) treat
// this as a normal "lost the race" and either return 409 or no-op.
var ErrConflict = errors.New("provisioning: state changed concurrently")

// ErrInvalidTransition — caller asked for an action the state machine
// doesn't support from the current state (e.g. Pause from `archived`).
var ErrInvalidTransition = errors.New("provisioning: invalid transition for current state")

// stepLogEntry mirrors the JSON shape persisted into
// provisioning_step_log. Helper functions construct one of these per
// orchestrator action; the PG cap helper trims FIFO.
type stepLogEntry struct {
	Seq           int       `json:"seq"`
	Step          string    `json:"step"`
	FromState     string    `json:"from_state"`
	ToState       string    `json:"to_state"`
	Actor         string    `json:"actor"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	CompletedAt   time.Time `json:"completed_at"`
	Success       bool      `json:"success"`
	Error         string    `json:"error,omitempty"`
	Message       string    `json:"message,omitempty"`
	TraceID       string    `json:"trace_id,omitempty"`
	SpanID        string    `json:"span_id,omitempty"`
}

// ProvisioningOrchestrator drives state transitions and emits NATS
// commands. It holds no in-memory state (DB is the source of truth).
type ProvisioningOrchestrator struct {
	db     *gorm.DB
	nats   *nats.Conn
	logger *zap.Logger

	// publishMu serializes Publish/Update pairs only when needed for
	// tests (production runs lock-free against DB CAS). Currently unused
	// — kept for future "transactional outbox" upgrade if NATS publish
	// failure becomes a recurring problem.
	publishMu sync.Mutex
}

func NewProvisioningOrchestrator(db *gorm.DB, conn *nats.Conn, logger *zap.Logger) *ProvisioningOrchestrator {
	return &ProvisioningOrchestrator{db: db, nats: conn, logger: logger}
}

// --- Helpers ---------------------------------------------------------

// injectTraceContext writes trace_id + span_id into payload from the
// active OTel span on ctx. No-op when no active span (RecoveryLoop bg
// path uses a span-less context). D8.
func injectTraceContext(ctx context.Context, payload map[string]any) {
	if payload == nil {
		return
	}
	sp := trace.SpanFromContext(ctx)
	sc := sp.SpanContext()
	if !sc.IsValid() {
		return
	}
	payload["trace_id"] = sc.TraceID().String()
	payload["span_id"] = sc.SpanID().String()
}

// readState fetches the current provisioning_state for one row. Used
// by Retry / SetMode / Resume to decide what target state to CAS to.
func (o *ProvisioningOrchestrator) readState(ctx context.Context, sourceID int64) (ProvisioningState, error) {
	var s string
	row := o.db.WithContext(ctx).
		Raw(`SELECT provisioning_state
		       FROM cdc_system.source_object_registry
		      WHERE id = ?`, sourceID).Row()
	if err := row.Scan(&s); err != nil {
		return "", fmt.Errorf("read provisioning_state for source %d: %w", sourceID, err)
	}
	return ProvisioningState(s), nil
}

// readMode fetches the current provisioning_mode for one row.
func (o *ProvisioningOrchestrator) readMode(ctx context.Context, sourceID int64) (string, error) {
	var m string
	row := o.db.WithContext(ctx).
		Raw(`SELECT provisioning_mode
		       FROM cdc_system.source_object_registry
		      WHERE id = ?`, sourceID).Row()
	if err := row.Scan(&m); err != nil {
		return "", fmt.Errorf("read provisioning_mode for source %d: %w", sourceID, err)
	}
	return m, nil
}

// nextLogSeq returns the next monotonic seq for a step_log entry on
// one source. Reads jsonb_array_length(provisioning_step_log) and adds
// 1. Note: log cap may shift the array; seq is an action counter, NOT
// an index — duplicates after FIFO trim are acceptable.
func (o *ProvisioningOrchestrator) nextLogSeq(ctx context.Context, sourceID int64) int {
	var n int
	row := o.db.WithContext(ctx).
		Raw(`SELECT COALESCE(jsonb_array_length(provisioning_step_log), 0)
		       FROM cdc_system.source_object_registry
		      WHERE id = ?`, sourceID).Row()
	if err := row.Scan(&n); err != nil {
		return 0
	}
	return n + 1
}

// casUpdateState issues a Compare-And-Swap UPDATE that flips
// provisioning_state from `from` to `to`, appends one log entry (D6 +
// D7 enforced together so callers can't forget either), and optionally
// stamps last_step_error.
//
// Returns ErrConflict when RowsAffected==0 (race lost or row missing).
func (o *ProvisioningOrchestrator) casUpdateState(
	ctx context.Context,
	sourceID int64,
	from, to ProvisioningState,
	entry stepLogEntry,
	lastStepError *string,
) error {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal step log entry: %w", err)
	}

	var (
		res *gorm.DB
		sql string
	)
	if lastStepError != nil {
		sql = `UPDATE cdc_system.source_object_registry
		          SET provisioning_state    = ?,
		              provisioning_step_log = cdc_system.append_step_log_capped(
		                                          provisioning_step_log,
		                                          ?::jsonb,
		                                          ?),
		              last_step_error       = ?,
		              updated_at            = NOW()
		        WHERE id = ?
		          AND provisioning_state = ?`
		res = o.db.WithContext(ctx).Exec(sql,
			string(to), string(entryJSON), ProvisioningStepLogMaxEntries,
			*lastStepError, sourceID, string(from))
	} else {
		sql = `UPDATE cdc_system.source_object_registry
		          SET provisioning_state    = ?,
		              provisioning_step_log = cdc_system.append_step_log_capped(
		                                          provisioning_step_log,
		                                          ?::jsonb,
		                                          ?),
		              last_step_error       = NULL,
		              updated_at            = NOW()
		        WHERE id = ?
		          AND provisioning_state = ?`
		res = o.db.WithContext(ctx).Exec(sql,
			string(to), string(entryJSON), ProvisioningStepLogMaxEntries,
			sourceID, string(from))
	}
	if res.Error != nil {
		return fmt.Errorf("cas update %d %s->%s: %w", sourceID, from, to, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrConflict
	}
	return nil
}

// publishCmd marshals + publishes a NATS command with trace context
// injected (D8).
func (o *ProvisioningOrchestrator) publishCmd(
	ctx context.Context,
	subject string,
	payload map[string]any,
) error {
	injectTraceContext(ctx, payload)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal cmd payload: %w", err)
	}
	if o.nats == nil {
		return fmt.Errorf("nats conn nil — cannot publish %s", subject)
	}
	return o.nats.Publish(subject, body)
}

func newCorrelationID(sourceID int64, step string) string {
	return fmt.Sprintf("prov-%d-%s-%d", sourceID, step, time.Now().UnixNano())
}

// --- Public methods --------------------------------------------------

// Advance moves the source one step forward. Reads current state,
// looks up the StepDescriptor, CAS-flips to NextPending, then publishes
// the command. Idempotent under races: ErrConflict on lost race.
//
// Phase D Option-A (Architect ruling 2026-04-29): some transitions
// require a metadata side-effect before the worker can succeed. The
// orchestrator owns those UPSERTs — kept in sync between worker and
// CMS so D1 auto-fanout (which goes through worker.Advance) and CMS
// REST /advance (which goes through CMS.Advance) both seed identically.
func (o *ProvisioningOrchestrator) Advance(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	desc, ok := Transitions[cur]
	if !ok {
		return fmt.Errorf("%w: state=%s not advanceable", ErrInvalidTransition, cur)
	}

	// Step-specific metadata seed / lookup. Runs BEFORE CAS so a
	// failure leaves provisioning_state untouched.
	var masterTable, sourceTable string
	switch desc.Step {
	case "master_bind":
		masterTable, err = o.seedMasterBindingForAdvance(ctx, sourceID, actor)
		if err != nil {
			return fmt.Errorf("seed master_binding: %w", err)
		}
	case "discover":
		masterTable, err = o.lookupMasterTableForSource(ctx, sourceID)
		if err != nil {
			return fmt.Errorf("lookup master_table for discover: %w", err)
		}
		sourceTable, err = o.lookupSourceTableForSource(ctx, sourceID)
		if err != nil {
			return fmt.Errorf("lookup source_table for discover: %w", err)
		}
	case "schedule_enable":
		masterTable, err = o.lookupMasterTableForSource(ctx, sourceID)
		if err != nil {
			return fmt.Errorf("lookup master_table for schedule_enable: %w", err)
		}
	}

	corr := newCorrelationID(sourceID, desc.Step)
	now := time.Now().UTC()
	sp := trace.SpanFromContext(ctx).SpanContext()
	entry := stepLogEntry{
		Seq:           o.nextLogSeq(ctx, sourceID),
		Step:          desc.Step,
		FromState:     string(cur),
		ToState:       string(desc.NextPending),
		Actor:         actor,
		CorrelationID: corr,
		StartedAt:     now,
		CompletedAt:   now,
		Success:       true,
		Message:       fmt.Sprintf("dispatched %s -> %s", desc.Step, desc.NextPending),
	}
	if sp.IsValid() {
		entry.TraceID = sp.TraceID().String()
		entry.SpanID = sp.SpanID().String()
	}

	if err := o.casUpdateState(ctx, sourceID, cur, desc.NextPending, entry, nil); err != nil {
		return err
	}

	payload := map[string]any{
		"source_id":      sourceID,
		"correlation_id": corr,
		"triggered_by":   "provisioning",
		"actor":          actor,
		"step":           desc.Step,
	}
	switch desc.Step {
	case "master_bind":
		payload["master_table"] = masterTable
		payload["provisioning"] = true
	case "discover":
		payload["target_table"] = masterTable
		payload["source_table"] = sourceTable
		payload["provisioning"] = true
	case "schedule_enable":
		payload["master_table"] = masterTable
	}
	if err := o.publishCmd(ctx, desc.CmdSubject, payload); err != nil {
		// CAS already committed — DB now in *_pending. RecoveryLoop will
		// time it out after TTL if no one consumes the implicit redrive.
		o.logger.Warn("provisioning: publish cmd failed (state already advanced; will TTL out if not consumed)",
			zap.Int64("source_id", sourceID),
			zap.String("subject", desc.CmdSubject),
			zap.Error(err))
		return fmt.Errorf("publish %s: %w", desc.CmdSubject, err)
	}
	o.logger.Info("provisioning: advanced",
		zap.Int64("source_id", sourceID),
		zap.String("from", string(cur)),
		zap.String("to_pending", string(desc.NextPending)),
		zap.String("step", desc.Step),
		zap.String("master_table", masterTable),
		zap.String("correlation_id", corr))
	return nil
}

// seedMasterBindingForAdvance — see CMS port for the rationale.
// Identical implementation kept here so D1 auto-fanout (worker-side
// Advance) and CMS REST /advance produce the same metadata shape.
func (o *ProvisioningOrchestrator) seedMasterBindingForAdvance(
	ctx context.Context, sourceID int64, actor string,
) (string, error) {
	type srcRow struct {
		SourceObjectName     string `gorm:"column:source_object_name"`
		SourceConnectionID   int64  `gorm:"column:source_connection_id"`
		SourceConnectionCode string `gorm:"column:source_connection_code"`
		ShadowBindingID      *int64 `gorm:"column:shadow_binding_id"`
	}
	var src srcRow
	if err := o.db.WithContext(ctx).Raw(
		`SELECT sor.source_object_name,
		        sor.source_connection_id,
		        cr.connection_code AS source_connection_code,
		        sb.id              AS shadow_binding_id
		   FROM cdc_system.source_object_registry sor
		   JOIN cdc_system.connection_registry cr
		     ON cr.id = sor.source_connection_id
		   LEFT JOIN cdc_system.shadow_binding sb
		     ON sb.source_object_id = sor.id
		    AND sb.is_active = true
		  WHERE sor.id = ?`, sourceID).Scan(&src).Error; err != nil {
		return "", fmt.Errorf("source lookup: %w", err)
	}
	if src.SourceObjectName == "" {
		return "", fmt.Errorf("source_id=%d not found in source_object_registry", sourceID)
	}

	masterConnCode := os.Getenv("PROVISIONING_DEFAULT_MASTER_CONNECTION_CODE")
	type masterConnRow struct {
		ID             int64   `gorm:"column:id"`
		ConnectionCode string  `gorm:"column:connection_code"`
		DefaultDB      *string `gorm:"column:default_database"`
	}
	var mc masterConnRow
	q := o.db.WithContext(ctx).
		Table("cdc_system.connection_registry").
		Select("id, connection_code, default_database").
		Where("status = ? AND role_type IN ('master','mixed')", "active")
	if masterConnCode != "" {
		q = q.Where("connection_code = ?", masterConnCode)
	}
	if err := q.Order("id").Limit(1).Scan(&mc).Error; err != nil {
		return "", fmt.Errorf("master connection lookup: %w", err)
	}
	if mc.ID == 0 {
		if masterConnCode != "" {
			return "", fmt.Errorf("master connection_code=%q not found or not active", masterConnCode)
		}
		return "", fmt.Errorf("no active master connection in connection_registry — set PROVISIONING_DEFAULT_MASTER_CONNECTION_CODE or seed a row with role_type='master'")
	}

	masterSchema := os.Getenv("PROVISIONING_DEFAULT_MASTER_SCHEMA")
	if masterSchema == "" {
		masterSchema = "dw_" + src.SourceConnectionCode
	}
	masterTable := naming.NormalizeIdentifier(src.SourceObjectName)
	masterDB := ""
	if mc.DefaultDB != nil {
		masterDB = *mc.DefaultDB
	}
	physicalFQN := masterDB + "." + masterSchema + "." + masterTable
	bindingCode := fmt.Sprintf("auto_src_%d", sourceID)

	if err := o.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.master_binding
		   (binding_code, source_object_id, shadow_binding_id,
		    master_connection_id, master_database, master_schema,
		    master_table, physical_table_fqn,
		    transform_type, transform_spec,
		    schema_status, is_active,
		    created_by, created_at, updated_at)
		 VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, 'copy_1_to_1', '{}'::jsonb,
		         'approved', true, ?, NOW(), NOW())
		 ON CONFLICT (master_connection_id, master_schema, master_table)
		 DO UPDATE SET
		   is_active     = true,
		   schema_status = 'approved',
		   updated_at    = NOW()`,
		bindingCode, sourceID, src.ShadowBindingID,
		mc.ID, masterDB, masterSchema, masterTable, physicalFQN,
		"provisioning:"+actor).Error; err != nil {
		return "", fmt.Errorf("upsert master_binding: %w", err)
	}
	o.logger.Info("provisioning: master_binding seeded",
		zap.Int64("source_id", sourceID),
		zap.Int64("master_connection_id", mc.ID),
		zap.String("master_schema", masterSchema),
		zap.String("master_table", masterTable))
	return masterTable, nil
}

// lookupSourceTableForSource resolves the source_object_name for a
// given source row. Discover step needs it on the outbound payload as
// `source_table` so the discover handler can attach mapping rules to
// the correct legacy table key.
func (o *ProvisioningOrchestrator) lookupSourceTableForSource(
	ctx context.Context, sourceID int64,
) (string, error) {
	var name string
	if err := o.db.WithContext(ctx).Raw(
		`SELECT source_object_name FROM cdc_system.source_object_registry WHERE id = ?`,
		sourceID).Row().Scan(&name); err != nil {
		return "", fmt.Errorf("source_object_registry lookup: %w", err)
	}
	if name == "" {
		return "", fmt.Errorf("source_id=%d not found in source_object_registry", sourceID)
	}
	return name, nil
}

// lookupMasterTableForSource resolves the active master_binding's
// master_table for a source. Schedule_enable advance needs it on the
// outbound payload.
func (o *ProvisioningOrchestrator) lookupMasterTableForSource(
	ctx context.Context, sourceID int64,
) (string, error) {
	var masterTable string
	if err := o.db.WithContext(ctx).Raw(
		`SELECT master_table
		   FROM cdc_system.master_binding
		  WHERE source_object_id = ?
		    AND is_active = true
		  ORDER BY id DESC
		  LIMIT 1`, sourceID).Row().Scan(&masterTable); err != nil {
		return "", fmt.Errorf("master_binding lookup: %w", err)
	}
	if masterTable == "" {
		return "", fmt.Errorf("no active master_binding for source_id=%d — master_bind step must run first", sourceID)
	}
	return masterTable, nil
}

// HandleStepCompleted is the NATS callback for
// `cdc.evt.provisioning.step_completed`. Payload shape:
//
//	{
//	  "source_id":     123,
//	  "step":          "shadow_bind"|"master_bind"|"discover"|"schedule_enable",
//	  "success":       true|false,
//	  "error":         "..."  (when !success),
//	  "correlation_id":"...",
//	  "actor":         "shadow_bind_handler",
//	  "trace_id":      "...",
//	  "span_id":       "..."
//	}
//
// On success: CAS *_pending -> final state. If the source is in `auto`
// mode and the new state is advanceable, fire-and-forget Advance.
// On failure: CAS *_pending -> failed, stash err in last_step_error.
type StepCompletedEvent struct {
	SourceID      int64  `json:"source_id"`
	Step          string `json:"step"`
	Success       bool   `json:"success"`
	Error         string `json:"error,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Actor         string `json:"actor,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
}

func (o *ProvisioningOrchestrator) HandleStepCompleted(ctx context.Context, ev StepCompletedEvent) error {
	if ev.SourceID == 0 || ev.Step == "" {
		return fmt.Errorf("step_completed: missing source_id or step (payload=%+v)", ev)
	}
	cur, err := o.readState(ctx, ev.SourceID)
	if err != nil {
		return err
	}
	if !IsPending(cur) {
		// Likely already finalized by RecoveryLoop or another instance.
		return ErrConflict
	}
	finalState, ok := PendingToFinalize[cur]
	if !ok {
		return fmt.Errorf("step_completed: state %s not in PendingToFinalize", cur)
	}

	now := time.Now().UTC()
	entry := stepLogEntry{
		Seq:           o.nextLogSeq(ctx, ev.SourceID),
		Step:          ev.Step,
		FromState:     string(cur),
		Actor:         firstNonEmpty(ev.Actor, "step_handler"),
		CorrelationID: ev.CorrelationID,
		StartedAt:     now,
		CompletedAt:   now,
		Success:       ev.Success,
		Error:         ev.Error,
		TraceID:       ev.TraceID,
		SpanID:        ev.SpanID,
	}

	if !ev.Success {
		entry.ToState = string(StateFailed)
		entry.Message = fmt.Sprintf("%s failed: %s", ev.Step, ev.Error)
		errMsg := ev.Error
		if errMsg == "" {
			errMsg = "step_completed reported failure with no error string"
		}
		if err := o.casUpdateState(ctx, ev.SourceID, cur, StateFailed, entry, &errMsg); err != nil {
			return err
		}
		o.logger.Warn("provisioning: step failed",
			zap.Int64("source_id", ev.SourceID),
			zap.String("step", ev.Step),
			zap.String("from", string(cur)),
			zap.String("error", ev.Error))
		return nil
	}

	entry.ToState = string(finalState)
	entry.Message = fmt.Sprintf("%s succeeded: %s -> %s", ev.Step, cur, finalState)
	if err := o.casUpdateState(ctx, ev.SourceID, cur, finalState, entry, nil); err != nil {
		return err
	}
	o.logger.Info("provisioning: step completed",
		zap.Int64("source_id", ev.SourceID),
		zap.String("step", ev.Step),
		zap.String("to", string(finalState)))

	// D1 fan-out: when in auto mode AND new state is advanceable, kick.
	mode, mErr := o.readMode(ctx, ev.SourceID)
	if mErr != nil {
		o.logger.Warn("provisioning: read mode after step (skip fan-out)",
			zap.Int64("source_id", ev.SourceID), zap.Error(mErr))
		return nil
	}
	if mode == "auto" && CanAdvance(finalState) {
		if err := o.Advance(ctx, ev.SourceID, "auto-fanout"); err != nil && !errors.Is(err, ErrConflict) {
			o.logger.Warn("provisioning: auto fan-out advance failed",
				zap.Int64("source_id", ev.SourceID), zap.Error(err))
		}
	}
	return nil
}

// SetMode flips provisioning_mode auto<->manual. D1: when going
// manual->auto, kick Advance immediately if currently advanceable.
func (o *ProvisioningOrchestrator) SetMode(ctx context.Context, sourceID int64, target, actor string) error {
	if target != "auto" && target != "manual" {
		return fmt.Errorf("setmode: invalid target %q (want auto|manual)", target)
	}
	opposite := "manual"
	if target == "manual" {
		opposite = "auto"
	}
	res := o.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.source_object_registry
		    SET provisioning_mode = ?, updated_at = NOW()
		  WHERE id = ?
		    AND provisioning_mode = ?`,
		target, sourceID, opposite)
	if res.Error != nil {
		return fmt.Errorf("setmode update: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// Either already in target mode (no-op) or row missing — do not
		// fan-out. ErrConflict only when the row exists but mode mismatched.
		var existing string
		row := o.db.WithContext(ctx).Raw(
			`SELECT provisioning_mode FROM cdc_system.source_object_registry WHERE id = ?`,
			sourceID).Row()
		if err := row.Scan(&existing); err != nil {
			return fmt.Errorf("setmode: source %d not found: %w", sourceID, err)
		}
		if existing == target {
			return nil // no-op
		}
		return ErrConflict
	}
	o.logger.Info("provisioning: mode changed",
		zap.Int64("source_id", sourceID),
		zap.String("from", opposite),
		zap.String("to", target),
		zap.String("actor", actor))

	// D1: only kick Advance on manual -> auto.
	if target != "auto" {
		return nil
	}
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if !CanAdvance(cur) {
		return nil // terminal / pending — nothing to do
	}
	if err := o.Advance(ctx, sourceID, fmt.Sprintf("setmode-auto:%s", actor)); err != nil && !errors.Is(err, ErrConflict) {
		o.logger.Warn("provisioning: setmode auto fan-out advance failed",
			zap.Int64("source_id", sourceID), zap.Error(err))
		return err
	}
	return nil
}

// Pause: running -> paused. D6 CAS.
func (o *ProvisioningOrchestrator) Pause(ctx context.Context, sourceID int64, actor string) error {
	now := time.Now().UTC()
	entry := stepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "pause",
		FromState:   string(StateRunning),
		ToState:     string(StatePaused),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     "manual pause",
	}
	return o.casUpdateState(ctx, sourceID, StateRunning, StatePaused, entry, nil)
}

// Resume: paused -> running. D6 CAS. Phase D (Q5) — also kicks
// Advance afterwards so the orchestrator re-tests the live state and
// fans out the next step if one became available while paused.
// Errors from the kick are non-fatal (running isn't currently
// advanceable, so ErrInvalidTransition is expected and ignored).
func (o *ProvisioningOrchestrator) Resume(ctx context.Context, sourceID int64, actor string) error {
	now := time.Now().UTC()
	entry := stepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "resume",
		FromState:   string(StatePaused),
		ToState:     string(StateRunning),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     "manual resume",
	}
	if err := o.casUpdateState(ctx, sourceID, StatePaused, StateRunning, entry, nil); err != nil {
		return err
	}
	if err := o.Advance(ctx, sourceID, "resume:"+actor); err != nil &&
		!errors.Is(err, ErrInvalidTransition) && !errors.Is(err, ErrConflict) {
		o.logger.Warn("provisioning: resume kick advance failed (non-fatal)",
			zap.Int64("source_id", sourceID), zap.Error(err))
	}
	return nil
}

// Retry: failed -> from_state of last failed entry, then Advance.
// D2: re-fires CURRENT step (not previous). The from_state of the last
// log entry with success=false equals the state we were in when the
// command was dispatched — exactly what Advance() needs to look up
// in Transitions.
func (o *ProvisioningOrchestrator) Retry(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur != StateFailed {
		return fmt.Errorf("%w: retry requires state=failed, got %s", ErrInvalidTransition, cur)
	}

	// Find the most recent failed entry's from_state.
	var fromStr string
	row := o.db.WithContext(ctx).Raw(`
		SELECT elem->>'from_state' AS from_state
		  FROM cdc_system.source_object_registry sor,
		       jsonb_array_elements(sor.provisioning_step_log) AS elem
		 WHERE sor.id = ?
		   AND (elem->>'success')::boolean = false
		 ORDER BY (elem->>'seq')::int DESC
		 LIMIT 1`, sourceID).Row()
	if err := row.Scan(&fromStr); err != nil {
		return fmt.Errorf("retry: cannot find last failed entry for source %d: %w", sourceID, err)
	}
	target := ProvisioningState(fromStr)
	if !CanAdvance(target) {
		return fmt.Errorf("retry: from_state=%s not advanceable", target)
	}

	now := time.Now().UTC()
	entry := stepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "retry",
		FromState:   string(StateFailed),
		ToState:     string(target),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     fmt.Sprintf("retry: clearing failed state, re-firing %s", target),
	}
	if err := o.casUpdateState(ctx, sourceID, StateFailed, target, entry, nil); err != nil {
		return err
	}
	return o.Advance(ctx, sourceID, fmt.Sprintf("retry:%s", actor))
}

// Archive: any non-archived state -> archived. CAS uses NOT-equals so
// idempotent retries from any state succeed exactly once.
func (o *ProvisioningOrchestrator) Archive(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur == StateArchived {
		return nil // idempotent
	}

	now := time.Now().UTC()
	entry := stepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "archive",
		FromState:   string(cur),
		ToState:     string(StateArchived),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     fmt.Sprintf("archive from %s", cur),
	}
	return o.casUpdateState(ctx, sourceID, cur, StateArchived, entry, nil)
}

// RecoveryLoop scans for *_pending rows older than ProvisioningPendingTTL
// and stamps them failed with last_step_error='TIMEOUT_EXCEEDED' (D3).
// Run as a goroutine at boot. Exits cleanly when ctx is cancelled.
func (o *ProvisioningOrchestrator) RecoveryLoop(ctx context.Context) {
	tick := time.NewTicker(ProvisioningRecoveryInterval)
	defer tick.Stop()
	o.logger.Info("provisioning: recovery loop started",
		zap.Duration("ttl", ProvisioningPendingTTL),
		zap.Duration("interval", ProvisioningRecoveryInterval))
	for {
		select {
		case <-ctx.Done():
			o.logger.Info("provisioning: recovery loop stopped")
			return
		case <-tick.C:
			if err := o.recoveryTick(ctx); err != nil {
				o.logger.Warn("provisioning: recovery tick failed", zap.Error(err))
			}
		}
	}
}

// recoveryTick is exported for tests via package-private name; the
// inner SQL is what we want to assert on.
func (o *ProvisioningOrchestrator) recoveryTick(ctx context.Context) error {
	type pendingRow struct {
		ID    int64  `gorm:"column:id"`
		State string `gorm:"column:provisioning_state"`
	}
	var rows []pendingRow

	cutoff := time.Now().Add(-ProvisioningPendingTTL)
	err := o.db.WithContext(ctx).Raw(`
		SELECT id, provisioning_state
		  FROM cdc_system.source_object_registry
		 WHERE provisioning_state IN ('shadow_pending','master_pending','mapping_pending','schedule_pending')
		   AND updated_at < ?`, cutoff).Scan(&rows).Error
	if err != nil {
		return fmt.Errorf("recovery scan: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	for _, r := range rows {
		now := time.Now().UTC()
		entry := stepLogEntry{
			Seq:         o.nextLogSeq(ctx, r.ID),
			Step:        "timeout",
			FromState:   r.State,
			ToState:     string(StateFailed),
			Actor:       "recovery_loop",
			StartedAt:   now,
			CompletedAt: now,
			Success:     false,
			Error:       "TIMEOUT_EXCEEDED",
			Message:     fmt.Sprintf("no completion event within %s — marking failed", ProvisioningPendingTTL),
		}
		errMsg := "TIMEOUT_EXCEEDED"
		// Add cutoff guard to the CAS WHERE so we don't race a
		// completion event arriving in the same tick.
		entryJSON, mErr := json.Marshal(entry)
		if mErr != nil {
			o.logger.Warn("recovery: marshal entry", zap.Int64("id", r.ID), zap.Error(mErr))
			continue
		}
		res := o.db.WithContext(ctx).Exec(`
			UPDATE cdc_system.source_object_registry
			   SET provisioning_state    = 'failed',
			       provisioning_step_log = cdc_system.append_step_log_capped(
			                                   provisioning_step_log,
			                                   ?::jsonb,
			                                   ?),
			       last_step_error       = ?,
			       updated_at            = NOW()
			 WHERE id = ?
			   AND provisioning_state = ?
			   AND updated_at < ?`,
			string(entryJSON), ProvisioningStepLogMaxEntries,
			errMsg, r.ID, r.State, cutoff)
		if res.Error != nil {
			o.logger.Warn("recovery: cas failed",
				zap.Int64("id", r.ID), zap.Error(res.Error))
			continue
		}
		if res.RowsAffected > 0 {
			o.logger.Info("provisioning: recovered (timeout)",
				zap.Int64("source_id", r.ID),
				zap.String("from_state", r.State))
		}
	}
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
