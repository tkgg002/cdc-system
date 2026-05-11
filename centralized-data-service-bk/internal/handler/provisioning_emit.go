// Package handler — provisioning_emit.go
//
// Phase D — shared helper for the four step handlers (shadow_bind,
// master_bind, discover, schedule_enable) to emit
// `cdc.evt.provisioning.step_completed` consistently.
//
// Architect rulings applied:
//   D8 — trace propagation. Inbound payload may carry `trace_id` /
//        `span_id`; helper echoes them outbound so the CMS orchestrator
//        finalize step lands in the same span.
//   Q4 — every error path MUST publish success=false with sanitized
//        error string before returning.
//
// Design: callers use `defer ph.EmitStepCompleted(...)` with a captured
// named-return `err` so both happy and panic paths emit reliably.
package handler

import (
	"encoding/json"
	"time"

	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// stepCompletedPayload — wire shape consumed by
// service.StepCompletedEvent (orchestrator side).
type stepCompletedPayload struct {
	SourceID      int64  `json:"source_id"`
	Step          string `json:"step"`
	Success       bool   `json:"success"`
	Error         string `json:"error,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Actor         string `json:"actor,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
	CompletedAt   string `json:"completed_at"`
}

// emitStepCompleted publishes `cdc.evt.provisioning.step_completed`.
// Pure publish — never touches DB. Best-effort: if NATS is down the
// caller logs WARN; the orchestrator's RecoveryLoop will flip
// `*_pending -> failed` after the configured TTL so the source can
// neither stay pending forever nor silently advance.
//
// All inputs are scalar so a panicked caller's deferred call still
// executes safely. `err` may be nil → success=true.
func emitStepCompleted(
	conn *nats.Conn,
	logger *zap.Logger,
	sourceID int64,
	step string,
	stepErr error,
	correlationID, actor, traceID, spanID string,
) {
	if sourceID == 0 || step == "" {
		// Ad-hoc command not coming from provisioning flow; no-op.
		return
	}
	if conn == nil {
		logger.Warn("provisioning emit: nats conn nil — skip",
			zap.Int64("source_id", sourceID), zap.String("step", step))
		return
	}
	payload := stepCompletedPayload{
		SourceID:      sourceID,
		Step:          step,
		Success:       stepErr == nil,
		CorrelationID: correlationID,
		Actor:         actor,
		TraceID:       traceID,
		SpanID:        spanID,
		CompletedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if stepErr != nil {
		payload.Error = service.SanitizeFreeformText(stepErr.Error(), 2000)
	}
	body, mErr := json.Marshal(payload)
	if mErr != nil {
		logger.Warn("provisioning emit: marshal failed",
			zap.Int64("source_id", sourceID),
			zap.String("step", step),
			zap.Error(mErr))
		return
	}
	if pErr := conn.Publish(SubjectProvisioningStepCompleted, body); pErr != nil {
		logger.Warn("provisioning emit: publish failed",
			zap.Int64("source_id", sourceID),
			zap.String("step", step),
			zap.Error(pErr))
		return
	}
	logger.Info("provisioning emit: step_completed published",
		zap.Int64("source_id", sourceID),
		zap.String("step", step),
		zap.Bool("success", stepErr == nil),
		zap.String("correlation_id", correlationID))
}
