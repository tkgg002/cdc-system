package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// MasterDDLHandler listens on cdc.cmd.master-create and materialises the
// requested master table via MasterDDLGenerator.Apply. Reply (or
// cdc.result.master-create publish) carries the result struct.
type MasterDDLHandler struct {
	gen      *service.MasterDDLGenerator
	natsConn *nats.Conn
	logger   *zap.Logger
}

func NewMasterDDLHandler(gen *service.MasterDDLGenerator, conn *nats.Conn, logger *zap.Logger) *MasterDDLHandler {
	return &MasterDDLHandler{gen: gen, natsConn: conn, logger: logger}
}

type masterCreateRequest struct {
	MasterTable   string `json:"master_table"`
	TriggeredBy   string `json:"triggered_by,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	// Phase D (Q2 alias) — when this command arrives via
	// cdc.cmd.master.bind from the provisioning orchestrator,
	// Provisioning=true tells the handler to publish a
	// `cdc.evt.provisioning.step_completed` event after Apply
	// succeeds (or fails). SourceID identifies the row in
	// source_object_registry to finalize.
	Provisioning bool   `json:"provisioning,omitempty"`
	SourceID     int64  `json:"source_id,omitempty"`
	TraceID      string `json:"trace_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
}

type masterCreateResponse struct {
	*service.MasterDDLResult
	CorrelationID string `json:"correlation_id,omitempty"`
}

// HandleMasterCreate consumes cdc.cmd.master-create AND (Q2 alias)
// cdc.cmd.master.bind. The two subjects share a handler; provisioning
// flow sets Provisioning=true on the payload to opt into emitting
// step_completed at the end (defer + named-style stepErr capture).
func (h *MasterDDLHandler) HandleMasterCreate(msg *nats.Msg) {
	var req masterCreateRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		h.replyErr(msg, req.CorrelationID, "invalid_payload: "+err.Error())
		return
	}

	// Phase D (Q4): defer emit so both happy/fail paths publish.
	// Only fires when this is the provisioning alias path.
	var stepErr error
	defer func() {
		if !req.Provisioning {
			return
		}
		emitStepCompleted(h.natsConn, h.logger,
			req.SourceID, "master_bind", stepErr,
			req.CorrelationID, "master_bind_handler",
			req.TraceID, req.SpanID)
	}()

	if req.MasterTable == "" {
		stepErr = fmt.Errorf("master_table required")
		h.replyErr(msg, req.CorrelationID, "master_table required")
		return
	}
	if req.Provisioning && req.SourceID == 0 {
		stepErr = fmt.Errorf("provisioning master_bind: source_id required")
		h.replyErr(msg, req.CorrelationID, stepErr.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := h.gen.Apply(ctx, req.MasterTable)
	if err != nil {
		stepErr = err
		h.logger.Warn("master DDL apply failed",
			zap.String("master", req.MasterTable),
			zap.String("correlation_id", req.CorrelationID),
			zap.Error(err))
		if res == nil {
			res = &service.MasterDDLResult{MasterName: req.MasterTable, Err: err.Error()}
		}
		h.reply(msg, masterCreateResponse{MasterDDLResult: res, CorrelationID: req.CorrelationID})
		return
	}

	h.logger.Info("master DDL applied",
		zap.String("master", res.MasterName),
		zap.Int("rule_count", res.RuleCount),
		zap.Int("index_count", len(res.IndexSQL)),
		zap.Bool("rls_applied", res.RLSApplied),
		zap.Bool("provisioning", req.Provisioning))

	h.reply(msg, masterCreateResponse{MasterDDLResult: res, CorrelationID: req.CorrelationID})
}

func (h *MasterDDLHandler) reply(msg *nats.Msg, resp masterCreateResponse) {
	data, _ := json.Marshal(resp)
	if msg.Reply != "" {
		_ = msg.Respond(data)
		return
	}
	if h.natsConn != nil {
		_ = h.natsConn.Publish("cdc.result.master-create", data)
	}
}

func (h *MasterDDLHandler) replyErr(msg *nats.Msg, correlationID, errMsg string) {
	h.logger.Warn("master-create error", zap.String("correlation_id", correlationID), zap.String("error", errMsg))
	h.reply(msg, masterCreateResponse{
		MasterDDLResult: &service.MasterDDLResult{Err: errMsg},
		CorrelationID:   correlationID,
	})
}
