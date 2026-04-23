// Package handler — transmute_handler.go
//
// NATS subscriber for the Shadow → Master transmutation pipeline
// (plan v2 §R6). Subject contract:
//
//	cdc.cmd.transmute          — materialise 1 master table, optional source_ids filter
//	cdc.cmd.transmute-preview  — evaluate rules against a sample, no write
//	cdc.result.transmute       — async result (TransmuteResult JSON)
package handler

import (
	"context"
	"encoding/json"
	"time"

	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransmuteHandler wires a TransmuterModule to NATS. Kept distinct from
// CommandHandler so the two packages (Shadow→Master vs. legacy bridge)
// can evolve independently.
type TransmuteHandler struct {
	svc      *service.TransmuterModule
	db       *gorm.DB
	natsConn *nats.Conn
	logger   *zap.Logger
}

func NewTransmuteHandler(svc *service.TransmuterModule, db *gorm.DB, conn *nats.Conn, logger *zap.Logger) *TransmuteHandler {
	return &TransmuteHandler{svc: svc, db: db, natsConn: conn, logger: logger}
}

// HandleTransmuteShadow consumes cdc.cmd.transmute-shadow — a lightweight
// trigger fired by the SinkWorker post-ingest hook. Payload carries the
// shadow table + the single source_id just written. This handler fans
// out to every master currently registered against the shadow (active +
// schema_status='approved') by re-publishing cdc.cmd.transmute once per
// master.
func (h *TransmuteHandler) HandleTransmuteShadow(msg *nats.Msg) {
	var req struct {
		ShadowTable   string   `json:"shadow_table"`
		SourceIDs     []string `json:"source_ids"`
		CorrelationID string   `json:"correlation_id,omitempty"`
	}
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		h.logger.Warn("transmute-shadow: bad payload", zap.Error(err))
		return
	}
	if req.ShadowTable == "" || h.db == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var masters []string
	err := h.db.WithContext(ctx).Raw(
		`SELECT master_name FROM cdc_internal.master_table_registry
		  WHERE source_shadow = ? AND is_active = true AND schema_status = 'approved'`,
		req.ShadowTable,
	).Scan(&masters).Error
	if err != nil {
		h.logger.Warn("transmute-shadow: master lookup failed",
			zap.String("shadow", req.ShadowTable), zap.Error(err))
		return
	}
	if len(masters) == 0 {
		return
	}
	for _, m := range masters {
		out, _ := json.Marshal(map[string]any{
			"master_table":   m,
			"source_ids":     req.SourceIDs,
			"triggered_by":   "sinkworker-hook",
			"correlation_id": req.CorrelationID,
		})
		if h.natsConn != nil {
			_ = h.natsConn.Publish("cdc.cmd.transmute", out)
		}
	}
}

// TransmuteRequest is the payload for cdc.cmd.transmute.
type TransmuteRequest struct {
	MasterTable   string   `json:"master_table"`
	SourceIDs     []string `json:"source_ids,omitempty"` // optional — incremental
	TriggeredBy   string   `json:"triggered_by,omitempty"`
	CorrelationID string   `json:"correlation_id,omitempty"`
}

// TransmuteResponse mirrors TransmuterModule.TransmuteResult + correlation.
type TransmuteResponse struct {
	service.TransmuteResult
	CorrelationID string `json:"correlation_id,omitempty"`
	Err           string `json:"error,omitempty"`
}

// HandleTransmute consumes cdc.cmd.transmute. Runs the full Shadow → Master
// materialise cycle, writes a result to cdc.result.transmute.
func (h *TransmuteHandler) HandleTransmute(msg *nats.Msg) {
	var req TransmuteRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		h.replyErr(msg, req.CorrelationID, "invalid_payload: "+err.Error())
		return
	}
	if req.MasterTable == "" {
		h.replyErr(msg, req.CorrelationID, "master_table required")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	res, err := h.svc.Run(ctx, req.MasterTable, req.SourceIDs)
	resp := TransmuteResponse{TransmuteResult: res, CorrelationID: req.CorrelationID}
	if err != nil {
		resp.Err = err.Error()
		h.logger.Warn("transmute failed",
			zap.String("master", req.MasterTable),
			zap.String("correlation_id", req.CorrelationID),
			zap.Error(err))
	} else {
		h.logger.Info("transmute complete",
			zap.String("master", req.MasterTable),
			zap.Int64("scanned", res.Scanned),
			zap.Int64("inserted", res.Inserted),
			zap.Int64("updated", res.Updated),
			zap.Int64("skipped", res.Skipped),
			zap.Int64("type_errors", res.TypeErrors),
			zap.Int64("rule_misses", res.RuleMisses),
			zap.String("active_gate", res.ActiveGate),
			zap.Int64("duration_ms", res.DurationMs))
	}

	h.reply(msg, resp)
}

func (h *TransmuteHandler) reply(msg *nats.Msg, resp TransmuteResponse) {
	data, _ := json.Marshal(resp)
	if msg.Reply != "" {
		_ = msg.Respond(data)
		return
	}
	if h.natsConn != nil {
		_ = h.natsConn.Publish("cdc.result.transmute", data)
	}
}

func (h *TransmuteHandler) replyErr(msg *nats.Msg, correlationID, errMsg string) {
	h.logger.Warn("transmute error", zap.String("correlation_id", correlationID), zap.String("error", errMsg))
	h.reply(msg, TransmuteResponse{
		TransmuteResult: service.TransmuteResult{},
		CorrelationID:   correlationID,
		Err:             errMsg,
	})
}
