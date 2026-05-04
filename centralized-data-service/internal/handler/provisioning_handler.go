// Package handler — provisioning_handler.go
//
// Thin NATS subscriber that forwards `cdc.evt.provisioning.step_completed`
// payloads into ProvisioningOrchestrator.HandleStepCompleted. Decoupled
// so the orchestrator stays transport-agnostic and can be reused by
// the CMS REST handlers (Phase C) without re-wiring NATS bits.
package handler

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"centralized-data-service/internal/service"
)

// SubjectProvisioningStepCompleted — NATS topic for inbound step_completed
// events from shadow_bind / master_bind / discover / schedule_enable
// handlers.
const SubjectProvisioningStepCompleted = "cdc.evt.provisioning.step_completed"

// ProvisioningHandler bundles the orchestrator + logger so the NATS
// subscribe callback has stable references.
type ProvisioningHandler struct {
	orch   *service.ProvisioningOrchestrator
	logger *zap.Logger
}

func NewProvisioningHandler(orch *service.ProvisioningOrchestrator, logger *zap.Logger) *ProvisioningHandler {
	return &ProvisioningHandler{orch: orch, logger: logger}
}

// HandleStepCompleted is the NATS subscribe callback. Errors are
// logged at WARN — NATS doesn't redeliver and ErrConflict is a normal
// outcome when RecoveryLoop or another instance already finalized the
// row.
func (h *ProvisioningHandler) HandleStepCompleted(msg *nats.Msg) {
	var ev service.StepCompletedEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		h.logger.Warn("provisioning handler: bad payload",
			zap.String("subject", msg.Subject),
			zap.Error(err))
		return
	}
	if err := h.orch.HandleStepCompleted(context.Background(), ev); err != nil {
		if errors.Is(err, service.ErrConflict) {
			h.logger.Info("provisioning handler: conflict (already finalized)",
				zap.Int64("source_id", ev.SourceID),
				zap.String("step", ev.Step))
			return
		}
		h.logger.Warn("provisioning handler: process step failed",
			zap.Int64("source_id", ev.SourceID),
			zap.String("step", ev.Step),
			zap.Error(err))
	}
}
