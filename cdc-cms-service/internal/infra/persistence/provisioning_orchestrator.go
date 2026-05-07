// Package service — provisioning_orchestrator.go (CMS port)
//
// CMS-side orchestrator for synchronous Manager-triggered actions
// (Advance, Pause, Resume, Retry, Archive, SetMode). It calls the SAME
// `cdc_system.append_step_log_capped` PG helper and the SAME CAS WHERE
// guards as the worker-side orchestrator — both write to the same DB,
// so concurrency is safe by D6.
//
// What lives here vs worker:
//   - CMS owns the REST trigger surface (this file + the handler).
//   - Worker owns HandleStepCompleted (NATS subscriber for step events)
//     and RecoveryLoop (TTL timeout sweep).
//   - State machine table is duplicated under provisioning_state_machine.go.
//
// Architect rulings (04_decisions_provisioning_mode.md) — D1, D2, D6,
// D7, D8 apply directly here. D3 + step_completed handling stay
// worker-side.
package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Step-log cap, env-overridable. Must match the worker-side default
// (50) so both surfaces produce comparable history lengths.
var ProvisioningStepLogMaxEntries = 50

func init() {
	if v := os.Getenv("PROVISIONING_STEP_LOG_MAX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			ProvisioningStepLogMaxEntries = n
		}
	}
}

// ErrProvisioningConflict — CAS guard rejected the UPDATE because
// another worker (or another CMS instance) changed the row first.
// HTTP layer maps this to 409 Conflict.
var ErrProvisioningConflict = errors.New("provisioning: state changed concurrently")

// ErrProvisioningInvalidTransition — caller asked for an action the
// state machine doesn't support from the current state. HTTP layer
// maps this to 422 Unprocessable Entity.
var ErrProvisioningInvalidTransition = errors.New("provisioning: invalid transition for current state")

// ErrProvisioningSourceNotFound — id doesn't exist in source_object_registry.
// HTTP layer maps to 404.
var ErrProvisioningSourceNotFound = errors.New("provisioning: source not found")

type provisioningStepLogEntry struct {
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
// commands. Stateless — DB is single source of truth.
type ProvisioningOrchestrator struct {
	db     *gorm.DB
	nats   *nats.Conn
	logger *zap.Logger
}

func NewProvisioningOrchestrator(db *gorm.DB, conn *nats.Conn, logger *zap.Logger) *ProvisioningOrchestrator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ProvisioningOrchestrator{db: db, nats: conn, logger: logger}
}

// ---------------- Read API (powers GET endpoint) -----------------

// SourceProvisioningSnapshot is the JSON shape returned by GetState.
// Mirrors the columns the FE needs without exposing the full
// source_object_registry row.
type SourceProvisioningSnapshot struct {
	SourceID            int64           `json:"source_id"`
	ProvisioningMode    string          `json:"provisioning_mode"`
	ProvisioningState   string          `json:"provisioning_state"`
	LastStepError       *string         `json:"last_step_error,omitempty"`
	ProvisioningStepLog json.RawMessage `json:"provisioning_step_log"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

func (o *ProvisioningOrchestrator) GetState(ctx context.Context, sourceID int64) (*SourceProvisioningSnapshot, error) {
	type row struct {
		ID            int64     `gorm:"column:id"`
		Mode          string    `gorm:"column:provisioning_mode"`
		State         string    `gorm:"column:provisioning_state"`
		LastErr       *string   `gorm:"column:last_step_error"`
		StepLog       []byte    `gorm:"column:provisioning_step_log"`
		UpdatedAt     time.Time `gorm:"column:updated_at"`
	}
	var r row
	err := o.db.WithContext(ctx).Raw(
		`SELECT id, provisioning_mode, provisioning_state, last_step_error,
		        provisioning_step_log, updated_at
		   FROM cdc_system.source_object_registry
		  WHERE id = ?`, sourceID).Scan(&r).Error
	if err != nil {
		return nil, fmt.Errorf("get state: %w", err)
	}
	if r.ID == 0 {
		return nil, ErrProvisioningSourceNotFound
	}
	logRaw := json.RawMessage(r.StepLog)
	if len(logRaw) == 0 {
		logRaw = json.RawMessage("[]")
	}
	return &SourceProvisioningSnapshot{
		SourceID:            r.ID,
		ProvisioningMode:    r.Mode,
		ProvisioningState:   r.State,
		LastStepError:       r.LastErr,
		ProvisioningStepLog: logRaw,
		UpdatedAt:           r.UpdatedAt,
	}, nil
}

// ---------------- Internal helpers ----------------------------

func (o *ProvisioningOrchestrator) readState(ctx context.Context, sourceID int64) (ProvisioningState, error) {
	var s string
	row := o.db.WithContext(ctx).Raw(
		`SELECT provisioning_state FROM cdc_system.source_object_registry WHERE id = ?`,
		sourceID).Row()
	if err := row.Scan(&s); err != nil {
		return "", ErrProvisioningSourceNotFound
	}
	return ProvisioningState(s), nil
}

func (o *ProvisioningOrchestrator) readMode(ctx context.Context, sourceID int64) (string, error) {
	var m string
	row := o.db.WithContext(ctx).Raw(
		`SELECT provisioning_mode FROM cdc_system.source_object_registry WHERE id = ?`,
		sourceID).Row()
	if err := row.Scan(&m); err != nil {
		return "", ErrProvisioningSourceNotFound
	}
	return m, nil
}

func (o *ProvisioningOrchestrator) nextLogSeq(ctx context.Context, sourceID int64) int {
	var n int
	row := o.db.WithContext(ctx).Raw(
		`SELECT COALESCE(jsonb_array_length(provisioning_step_log), 0)
		   FROM cdc_system.source_object_registry WHERE id = ?`,
		sourceID).Row()
	if err := row.Scan(&n); err != nil {
		return 0
	}
	return n + 1
}

func injectProvisioningTraceContext(ctx context.Context, payload map[string]any) {
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

// casUpdateState — single SQL: CAS UPDATE state + append capped step
// log + optionally stamp last_step_error. RowsAffected==0 →
// ErrProvisioningConflict. (D6 + D7)
func (o *ProvisioningOrchestrator) casUpdateState(
	ctx context.Context,
	sourceID int64,
	from, to ProvisioningState,
	entry provisioningStepLogEntry,
	lastStepError *string,
) error {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal step log entry: %w", err)
	}
	var res *gorm.DB
	if lastStepError != nil {
		res = o.db.WithContext(ctx).Exec(
			`UPDATE cdc_system.source_object_registry
			    SET provisioning_state    = ?,
			        provisioning_step_log = cdc_system.append_step_log_capped(
			                                    provisioning_step_log, ?::jsonb, ?),
			        last_step_error       = ?,
			        updated_at            = NOW()
			  WHERE id = ?
			    AND provisioning_state = ?`,
			string(to), string(entryJSON), ProvisioningStepLogMaxEntries,
			*lastStepError, sourceID, string(from))
	} else {
		res = o.db.WithContext(ctx).Exec(
			`UPDATE cdc_system.source_object_registry
			    SET provisioning_state    = ?,
			        provisioning_step_log = cdc_system.append_step_log_capped(
			                                    provisioning_step_log, ?::jsonb, ?),
			        last_step_error       = NULL,
			        updated_at            = NOW()
			  WHERE id = ?
			    AND provisioning_state = ?`,
			string(to), string(entryJSON), ProvisioningStepLogMaxEntries,
			sourceID, string(from))
	}
	if res.Error != nil {
		return fmt.Errorf("cas update %d %s->%s: %w", sourceID, from, to, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrProvisioningConflict
	}
	return nil
}

func (o *ProvisioningOrchestrator) publishCmd(
	ctx context.Context, subject string, payload map[string]any,
) error {
	injectProvisioningTraceContext(ctx, payload)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal cmd: %w", err)
	}
	if o.nats == nil {
		return fmt.Errorf("nats nil — cannot publish %s", subject)
	}
	return o.nats.Publish(subject, body)
}

func newProvisioningCorrelationID(sourceID int64, step string) string {
	return fmt.Sprintf("prov-%d-%s-%d", sourceID, step, time.Now().UnixNano())
}

func provisioningEntryWithSpan(ctx context.Context, e provisioningStepLogEntry) provisioningStepLogEntry {
	sp := trace.SpanFromContext(ctx).SpanContext()
	if sp.IsValid() {
		e.TraceID = sp.TraceID().String()
		e.SpanID = sp.SpanID().String()
	}
	return e
}

// ---------------- Public mutation API ------------------------

// Advance moves the source one step forward. Looks up StepDescriptor,
// CAS-flips state→NextPending, publishes the command. ErrConflict on
// race; ErrInvalidTransition on terminal/non-advanceable input.
//
// Phase D Option-A (Architect ruling 2026-04-29): for steps that need
// a metadata side-effect (master_bind requires a row in
// cdc_system.master_binding before the worker's MasterDDLGenerator can
// `Apply`), the orchestrator does the UPSERT here — keeping the worker
// focused on execution and CMS as the metadata authority. The seed
// runs BEFORE the CAS so a failure leaves state untouched.
func (o *ProvisioningOrchestrator) Advance(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	desc, ok := ProvisioningTransitions[cur]
	if !ok {
		return fmt.Errorf("%w: state=%s not advanceable", ErrProvisioningInvalidTransition, cur)
	}

	// Step-specific metadata seed / lookup. Runs BEFORE CAS so a
	// failure here leaves provisioning_state untouched and the user
	// can retry without the row being stuck in an in-flight pending.
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

	corr := newProvisioningCorrelationID(sourceID, desc.Step)
	now := time.Now().UTC()
	entry := provisioningEntryWithSpan(ctx, provisioningStepLogEntry{
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
	})
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
	// Step-specific payload extras matching worker handler contracts.
	switch desc.Step {
	case "master_bind":
		// master_ddl_handler.go masterCreateRequest requires
		// master_table + provisioning=true (defer-emit branch) +
		// source_id (already present above).
		payload["master_table"] = masterTable
		payload["provisioning"] = true
	case "discover":
		// command_handler.go HandleDiscover needs target_table (DW
		// master), source_table (legacy mapping key), provisioning=true
		// to trigger defer-emit step_completed.
		payload["target_table"] = masterTable
		payload["source_table"] = sourceTable
		payload["provisioning"] = true
	case "schedule_enable":
		// provisioning_step_handlers.go scheduleEnableRequest needs
		// master_table to flip the right transmute_schedule row.
		payload["master_table"] = masterTable
	}
	if err := o.publishCmd(ctx, desc.CmdSubject, payload); err != nil {
		o.logger.Warn("provisioning: publish cmd failed (state advanced; will TTL out if not consumed)",
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

// seedMasterBindingForAdvance UPSERTs cdc_system.master_binding for the
// source so the worker's MasterDDLGenerator can succeed. Phase D
// Option-A: CMS owns metadata, worker owns execution.
//
// Convention (V1 auto-pipeline 1:1 mapping):
//   master_connection: first connection_registry row with role_type IN
//                      ('master','mixed') and status='active'.
//                      Override via env PROVISIONING_DEFAULT_MASTER_CONNECTION_CODE.
//   master_schema:     env PROVISIONING_DEFAULT_MASTER_SCHEMA, fallback
//                      to "dw_" + source connection_code.
//   master_table:      source_object_name (1:1).
//   transform_type:    'copy_1_to_1'
//   schema_status:     'approved' (auto-flow opt-in == user vetted)
//   is_active:         true (transmute_schedule join requires active)
//
// Idempotent via UNIQUE(master_connection_id, master_schema, master_table)
// + ON CONFLICT DO UPDATE — re-running the step (or RecoveryLoop retry)
// touches updated_at without duplicating rows.
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
		return "", ErrProvisioningSourceNotFound
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
	masterTable := src.SourceObjectName
	masterDB := ""
	if mc.DefaultDB != nil {
		masterDB = *mc.DefaultDB
	}
	physicalFQN := masterDB + "." + masterSchema + "." + masterTable

	// binding_code stable per source so re-runs hit ON CONFLICT.
	bindingCode := fmt.Sprintf("auto_src_%d", sourceID)

	// ON CONFLICT key = binding_code (stable per source via auto_src_<id>)
	// instead of (master_connection_id, master_schema, master_table). The
	// latter mishandles two scenarios:
	//   1. Same source row re-flipped Auto after master_table rename →
	//      INSERT raises UNIQUE(binding_code) violation that's not caught
	//      by the (conn,schema,table) ON CONFLICT clause.
	//   2. Lookup by source_object_id later returns 0 rows when the
	//      pre-existing row for the same physical table belongs to a
	//      different source.
	// Updating EXCLUDED.* ensures the binding row always reflects the
	// CURRENT mapping for THIS source. Cross-source collision on the
	// physical table is caught by the secondary UNIQUE constraint and
	// surfaces as a clear error rather than silent ownership swap.
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
		 ON CONFLICT (binding_code)
		 DO UPDATE SET
		   source_object_id     = EXCLUDED.source_object_id,
		   shadow_binding_id    = EXCLUDED.shadow_binding_id,
		   master_connection_id = EXCLUDED.master_connection_id,
		   master_database      = EXCLUDED.master_database,
		   master_schema        = EXCLUDED.master_schema,
		   master_table         = EXCLUDED.master_table,
		   physical_table_fqn   = EXCLUDED.physical_table_fqn,
		   is_active            = true,
		   schema_status        = 'approved',
		   updated_at           = NOW()`,
		bindingCode, sourceID, src.ShadowBindingID,
		mc.ID, masterDB, masterSchema, masterTable, physicalFQN,
		"provisioning:"+actor).Error; err != nil {
		return "", fmt.Errorf("upsert master_binding: %w", err)
	}

	o.logger.Info("provisioning: master_binding seeded",
		zap.Int64("source_id", sourceID),
		zap.Int64("master_connection_id", mc.ID),
		zap.String("master_connection_code", mc.ConnectionCode),
		zap.String("master_schema", masterSchema),
		zap.String("master_table", masterTable))
	return masterTable, nil
}

// lookupSourceTableForSource resolves source_object_name. Discover step
// payload requires source_table so the discover handler can attach
// mapping rules to the legacy table key.
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

// lookupMasterTableForSource finds the active master_binding.master_table
// for a source. Used by schedule_enable advance — the master_binding
// must exist (seeded earlier on master_bind) so this is a pure read.
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

// SetMode flips provisioning_mode auto<->manual. D1: manual->auto kicks
// Advance immediately if currently advanceable.
func (o *ProvisioningOrchestrator) SetMode(ctx context.Context, sourceID int64, target, actor string) error {
	if target != "auto" && target != "manual" {
		return fmt.Errorf("%w: invalid target mode %q (want auto|manual)", ErrProvisioningInvalidTransition, target)
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
		// No-op (already in target) or row missing.
		var existing string
		if err := o.db.WithContext(ctx).Raw(
			`SELECT provisioning_mode FROM cdc_system.source_object_registry WHERE id = ?`,
			sourceID).Row().Scan(&existing); err != nil {
			return ErrProvisioningSourceNotFound
		}
		if existing == target {
			return nil // idempotent no-op
		}
		return ErrProvisioningConflict
	}
	o.logger.Info("provisioning: mode changed",
		zap.Int64("source_id", sourceID),
		zap.String("from", opposite),
		zap.String("to", target),
		zap.String("actor", actor))
	if target != "auto" {
		return nil
	}
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if !ProvisioningCanAdvance(cur) {
		return nil
	}
	if err := o.Advance(ctx, sourceID, fmt.Sprintf("setmode-auto:%s", actor)); err != nil &&
		!errors.Is(err, ErrProvisioningConflict) {
		o.logger.Warn("provisioning: setmode auto fan-out advance failed",
			zap.Int64("source_id", sourceID), zap.Error(err))
		return err
	}
	return nil
}

func (o *ProvisioningOrchestrator) Pause(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur != StateRunning {
		return fmt.Errorf("%w: pause requires state=running, got %s", ErrProvisioningInvalidTransition, cur)
	}
	now := time.Now().UTC()
	entry := provisioningEntryWithSpan(ctx, provisioningStepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "pause",
		FromState:   string(StateRunning),
		ToState:     string(StatePaused),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     "manual pause",
	})
	return o.casUpdateState(ctx, sourceID, StateRunning, StatePaused, entry, nil)
}

func (o *ProvisioningOrchestrator) Resume(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur != StatePaused {
		return fmt.Errorf("%w: resume requires state=paused, got %s", ErrProvisioningInvalidTransition, cur)
	}
	now := time.Now().UTC()
	entry := provisioningEntryWithSpan(ctx, provisioningStepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "resume",
		FromState:   string(StatePaused),
		ToState:     string(StateRunning),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     "manual resume",
	})
	if err := o.casUpdateState(ctx, sourceID, StatePaused, StateRunning, entry, nil); err != nil {
		return err
	}
	// Phase D Q5 — re-test reality: after resuming, kick Advance so a
	// source whose underlying step has progressed during the pause does
	// not stay stuck on the previously-pending state. ErrInvalidTransition
	// (already running) and ErrConflict (race vs RecoveryLoop) are
	// expected outcomes — log and swallow.
	if err := o.Advance(ctx, sourceID, "resume:"+actor); err != nil &&
		!errors.Is(err, ErrProvisioningInvalidTransition) && !errors.Is(err, ErrProvisioningConflict) {
		o.logger.Warn("provisioning: resume kick advance failed (non-fatal)",
			zap.Int64("source_id", sourceID), zap.Error(err))
	}
	return nil
}

// Retry: failed -> from_state of last failed step_log entry, then Advance. D2.
func (o *ProvisioningOrchestrator) Retry(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur != StateFailed {
		return fmt.Errorf("%w: retry requires state=failed, got %s", ErrProvisioningInvalidTransition, cur)
	}
	var fromStr string
	row := o.db.WithContext(ctx).Raw(`
		SELECT elem->>'from_state'
		  FROM cdc_system.source_object_registry sor,
		       jsonb_array_elements(sor.provisioning_step_log) elem
		 WHERE sor.id = ?
		   AND (elem->>'success')::boolean = false
		 ORDER BY (elem->>'seq')::int DESC
		 LIMIT 1`, sourceID).Row()
	if err := row.Scan(&fromStr); err != nil {
		return fmt.Errorf("retry: cannot find last failed entry: %w", err)
	}
	target := ProvisioningState(fromStr)
	if !ProvisioningCanAdvance(target) {
		return fmt.Errorf("%w: retry from_state=%s not advanceable", ErrProvisioningInvalidTransition, target)
	}
	now := time.Now().UTC()
	entry := provisioningEntryWithSpan(ctx, provisioningStepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "retry",
		FromState:   string(StateFailed),
		ToState:     string(target),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     fmt.Sprintf("retry: clearing failed state, re-firing %s", target),
	})
	if err := o.casUpdateState(ctx, sourceID, StateFailed, target, entry, nil); err != nil {
		return err
	}
	return o.Advance(ctx, sourceID, fmt.Sprintf("retry:%s", actor))
}

// Archive: any non-archived state -> archived. Idempotent.
func (o *ProvisioningOrchestrator) Archive(ctx context.Context, sourceID int64, actor string) error {
	cur, err := o.readState(ctx, sourceID)
	if err != nil {
		return err
	}
	if cur == StateArchived {
		return nil
	}
	now := time.Now().UTC()
	entry := provisioningEntryWithSpan(ctx, provisioningStepLogEntry{
		Seq:         o.nextLogSeq(ctx, sourceID),
		Step:        "archive",
		FromState:   string(cur),
		ToState:     string(StateArchived),
		Actor:       actor,
		StartedAt:   now,
		CompletedAt: now,
		Success:     true,
		Message:     fmt.Sprintf("archive from %s", cur),
	})
	return o.casUpdateState(ctx, sourceID, cur, StateArchived, entry, nil)
}
