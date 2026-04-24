package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ReconHandler handles reconciliation NATS commands
type ReconHandler struct {
	reconCore   *service.ReconCore
	healer      *service.ReconHealer // plan WORKER task #9 — Phase 2/3 heal
	db          *gorm.DB
	mongoClient *mongo.Client
	schema      *service.SchemaAdapter
	masking     *service.MaskingService
	backfill    *service.BackfillSourceTsService
	tsDetector  *service.TimestampDetector // Migration 017 — manual re-detect
	natsPub     NatsPublisher
	logger      *zap.Logger
}

// NatsPublisher is the minimal surface of nats.Conn the handler needs
// to publish result events. Kept as an interface so unit tests can
// substitute a no-op / in-memory mock.
type NatsPublisher interface {
	Publish(subject string, data []byte) error
}

func NewReconHandler(reconCore *service.ReconCore, db *gorm.DB, mongoClient *mongo.Client, schema *service.SchemaAdapter, logger *zap.Logger) *ReconHandler {
	return &ReconHandler{reconCore: reconCore, db: db, mongoClient: mongoClient, schema: schema, logger: logger}
}

// WithBackfill wires the backfill service. Separated from the
// constructor so existing call sites that don't use backfill keep
// working.
func (h *ReconHandler) WithBackfill(b *service.BackfillSourceTsService, pub NatsPublisher) *ReconHandler {
	h.backfill = b
	h.natsPub = pub
	return h
}

// WithHealer wires the v3 ReconHealer so HandleReconHeal routes through
// HealWindow (Phase 2/3 signal + batched direct heal) instead of the
// legacy ReconCore.Heal path.
func (h *ReconHandler) WithHealer(healer *service.ReconHealer) *ReconHandler {
	h.healer = healer
	return h
}

func (h *ReconHandler) WithMaskingService(masking *service.MaskingService) *ReconHandler {
	h.masking = masking
	return h
}

// WithTimestampDetector wires the Migration 017 auto-detect service so
// HandleDetectTimestampField can sample Mongo and update registry rows.
// Separated from the constructor so legacy call sites keep compiling.
func (h *ReconHandler) WithTimestampDetector(td *service.TimestampDetector) *ReconHandler {
	h.tsDetector = td
	return h
}

// HandleReconCheck — subscribe "cdc.cmd.recon-check"
func (h *ReconHandler) HandleReconCheck(msg *nats.Msg) {
	var payload struct {
		Tier  string `json:"tier"`
		Table string `json:"table"`
	}
	json.Unmarshal(msg.Data, &payload)

	h.logger.Info("recon check received", zap.String("tier", payload.Tier), zap.String("table", payload.Table))

	ctx := context.Background()

	if payload.Table == "*" || payload.Table == "" {
		// Check all
		reports := h.reconCore.CheckAll(ctx)
		result, _ := json.Marshal(map[string]interface{}{"status": "success", "tables_checked": len(reports)})
		if msg.Reply != "" {
			msg.Respond(result)
		}
		h.logActivity("recon-check-all", "*", "success", int64(len(reports)), nil)
		return
	}

	// Single table
	var entry model.TableRegistry
	if err := h.db.Where("target_table = ?", payload.Table).First(&entry).Error; err != nil {
		h.logActivity("recon-check", payload.Table, "error", 0, fmt.Errorf("registry not found: %s", payload.Table))
		return
	}

	var report *model.ReconciliationReport
	switch payload.Tier {
	case "2":
		report = h.reconCore.RunTier2(ctx, entry)
	case "3":
		report = h.reconCore.RunTier3(ctx, entry)
	default:
		report = h.reconCore.RunTier1(ctx, entry)
	}

	h.logActivity("recon-check", payload.Table, report.Status, report.Diff, nil)

	if msg.Reply != "" {
		result, _ := json.Marshal(report)
		msg.Respond(result)
	}
}

// HandleReconHeal — subscribe "cdc.cmd.recon-heal".
// Plan WORKER task #9 — routes the heal through ReconHealer.HealWindow
// (Phase 2/3: Debezium signal + batched $in + OCC guard + audit batching
// + per-table sensitive-field masking).
//
// Post v3 root-cause fix (2026-04-17): the legacy `ReconCore.Heal`
// fallback has been REMOVED. The legacy path logged one activity_log
// row per record (spamming audit on large Tier 2 reports) and also
// sidestepped the OCC / masking guards. `worker_server.go` always
// wires a non-nil healer whenever recon is enabled — if we ever see
// a nil healer here it is a wiring bug, surface it instead of
// silently degrading.
func (h *ReconHandler) HandleReconHeal(msg *nats.Msg) {
	var payload struct {
		Table string `json:"table"`
	}
	json.Unmarshal(msg.Data, &payload)

	h.logger.Info("recon heal received", zap.String("table", payload.Table))
	ctx := context.Background()

	if h.healer == nil {
		err := fmt.Errorf("v3 healer not wired — worker_server init is broken; refusing to fall back to legacy heal")
		h.logger.Error("recon heal rejected", zap.String("table", payload.Table), zap.Error(err))
		h.logActivity("recon-heal", payload.Table, "error", 0, err)
		if msg.Reply != "" {
			result, _ := json.Marshal(map[string]interface{}{"error": err.Error()})
			msg.Respond(result)
		}
		return
	}

	var entry model.TableRegistry
	if err := h.db.Where("target_table = ?", payload.Table).First(&entry).Error; err != nil {
		h.logActivity("recon-heal", payload.Table, "error", 0, fmt.Errorf("registry not found"))
		return
	}

	// Get latest Tier 2 report with missing IDs
	var report model.ReconciliationReport
	if err := h.db.Where("target_table = ? AND tier = 2 AND missing_count > 0", payload.Table).
		Order("checked_at DESC").First(&report).Error; err != nil {
		// No Tier 2 report → run Tier 2 first
		h.logger.Info("no tier 2 report, running tier 2 first", zap.String("table", payload.Table))
		newReport := h.reconCore.RunTier2(ctx, entry)
		if newReport.MissingCount == 0 {
			h.logActivity("recon-heal", payload.Table, "success", 0, nil)
			return
		}
		report = *newReport
	}

	var missingIDs []string
	json.Unmarshal(report.MissingIDs, &missingIDs)

	// Window bounds for Phase A signal: derive from the report's
	// check time backwards by the default recon lookback. Debezium
	// uses these as the updated_at filter for incremental snapshots.
	tHi := report.CheckedAt
	if tHi.IsZero() {
		tHi = time.Now()
	}
	tLo := tHi.Add(-7 * 24 * time.Hour)

	res, healErr := h.healer.HealWindow(ctx, entry, tLo, tHi, missingIDs)
	var healedCount int
	if res != nil {
		healedCount = res.Upserted
		h.logger.Info("recon heal via v3 healer",
			zap.String("table", payload.Table),
			zap.Int("upserted", res.Upserted),
			zap.Int("skipped", res.Skipped),
			zap.Int("errored", res.Errored),
			zap.Bool("used_signal", res.UsedSignal),
			zap.String("signal_id", res.SignalID),
		)
	}

	if healErr != nil {
		h.logActivity("recon-heal", payload.Table, "error", 0, healErr)
	} else {
		h.logActivity("recon-heal", payload.Table, "success", int64(healedCount), nil)
	}

	if msg.Reply != "" {
		result, _ := json.Marshal(map[string]interface{}{"healed": healedCount, "total_missing": len(missingIDs)})
		msg.Respond(result)
	}
}

// HandleRetryFailed — subscribe "cdc.cmd.retry-failed"
func (h *ReconHandler) HandleRetryFailed(msg *nats.Msg) {
	var payload struct {
		FailedLogID uint64 `json:"failed_log_id"`
		TargetTable string `json:"target_table"`
		RecordID    string `json:"record_id"`
		RawJSON     string `json:"raw_json"`
	}
	json.Unmarshal(msg.Data, &payload)

	h.logger.Info("retry failed received", zap.Uint64("id", payload.FailedLogID), zap.String("table", payload.TargetTable))

	sanitizedRawJSON := h.sanitizeRetryRawJSON(payload.TargetTable, payload.RawJSON)

	// Parse raw JSON → map
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(sanitizedRawJSON), &data); err != nil {
		h.updateFailedLog(payload.FailedLogID, "failed", err.Error())
		return
	}

	// Get registry for PK field
	var entry model.TableRegistry
	if err := h.db.Where("target_table = ?", payload.TargetTable).First(&entry).Error; err != nil {
		h.updateFailedLog(payload.FailedLogID, "failed", "registry not found")
		return
	}

	// Upsert via SchemaAdapter
	schema := h.schema.GetSchema(payload.TargetTable)
	if schema == nil {
		h.schema.PrepareForCDCInsert(payload.TargetTable, entry.PrimaryKeyField)
		schema = h.schema.GetSchema(payload.TargetTable)
	}
	if schema == nil {
		h.updateFailedLog(payload.FailedLogID, "failed", "schema not found")
		return
	}

	// Retry path: unknown Debezium ts_ms → pass 0 so the OCC guard on
	// _source_ts is skipped (hash guard still applies).
	query, values := h.schema.BuildUpsertSQL(schema, payload.TargetTable, entry.PrimaryKeyField, payload.RecordID, data, sanitizedRawJSON, "retry", "", 0)
	if err := h.db.Exec(query, values...).Error; err != nil {
		h.updateFailedLog(payload.FailedLogID, "failed", err.Error())
		return
	}

	h.updateFailedLog(payload.FailedLogID, "resolved", "")
	h.logActivity("retry-failed", payload.TargetTable, "success", 1, nil)
}

// HandleDebeziumSignal — subscribe "cdc.cmd.debezium-signal" + "cdc.cmd.debezium-snapshot"
func (h *ReconHandler) HandleDebeziumSignal(msg *nats.Msg) {
	var payload struct {
		Type       string `json:"type"`
		Database   string `json:"database"`
		Collection string `json:"collection"`
		Table      string `json:"table"`
	}
	json.Unmarshal(msg.Data, &payload)

	h.logger.Info("debezium signal received", zap.String("type", payload.Type), zap.String("table", payload.Table))

	// Determine database + collection
	db := payload.Database
	collection := payload.Collection
	if payload.Table != "" && db == "" {
		// Lookup from registry
		var entry model.TableRegistry
		if err := h.db.Where("target_table = ?", payload.Table).First(&entry).Error; err == nil {
			db = entry.SourceDB
			collection = entry.SourceTable
		}
	}

	if db == "" || h.mongoClient == nil {
		h.logActivity("debezium-signal", payload.Table, "error", 0, fmt.Errorf("database or mongodb not configured"))
		return
	}

	// Insert signal into MongoDB debezium_signal collection
	signalDoc := bson.M{
		"type": "execute-snapshot",
		"data": bson.M{
			"data-collections": []string{db + "." + collection},
			"type":             "incremental",
		},
	}

	coll := h.mongoClient.Database(db).Collection("debezium_signal")
	_, err := coll.InsertOne(context.Background(), signalDoc)
	if err != nil {
		h.logActivity("debezium-signal", payload.Table, "error", 0, err)
		return
	}

	h.logActivity("debezium-signal", payload.Table, "success", 1, nil)
}

// HandleBackfillSourceTs — subscribe "cdc.cmd.recon-backfill-source-ts".
//
// Payload: {"table": "<target_table>" (optional — empty = all tables),
//
//	"run_id": "<uuid>",
//	"batch_size": <int, optional>}
//
// Runs synchronously inside the NATS handler goroutine (NATS.Go spins
// a goroutine per message). Updates recon_runs tier=4 as it progresses
// and emits a summary to `cdc.result.recon-backfill-source-ts`.
func (h *ReconHandler) HandleBackfillSourceTs(msg *nats.Msg) {
	var payload struct {
		Table     string `json:"table"`
		RunID     string `json:"run_id"`
		BatchSize int    `json:"batch_size"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Warn("backfill: invalid payload", zap.Error(err))
		return
	}

	if h.backfill == nil {
		h.logger.Warn("backfill: service not configured")
		h.logActivity("recon-backfill-source-ts", payload.Table, "error", 0, fmt.Errorf("backfill service not configured"))
		return
	}

	ctx := context.Background()
	var tables []string
	if payload.Table != "" {
		tables = []string{payload.Table}
	}

	h.logger.Info("backfill: run starting",
		zap.String("run_id", payload.RunID),
		zap.Strings("tables", tables),
		zap.Int("batch_size", payload.BatchSize),
	)

	results, err := h.backfill.BackfillAll(ctx, payload.RunID, tables)
	outcome := "success"
	if err != nil {
		outcome = "error"
		h.logger.Error("backfill: run failed", zap.Error(err))
	}

	// Activity log (summary row).
	h.backfill.WriteActivity(ctx, payload.RunID, results, outcome)

	// Result publish — CMS can subscribe for push updates; polling via
	// GET /api/recon/backfill-source-ts/status remains the primary UX.
	if h.natsPub != nil {
		result, _ := json.Marshal(map[string]interface{}{
			"run_id":  payload.RunID,
			"outcome": outcome,
			"results": results,
		})
		_ = h.natsPub.Publish("cdc.result.recon-backfill-source-ts", result)
	}

	if msg.Reply != "" {
		result, _ := json.Marshal(map[string]interface{}{
			"run_id":  payload.RunID,
			"outcome": outcome,
			"results": results,
		})
		msg.Respond(result)
	}
}

// HandleDetectTimestampField — subscribe "cdc.cmd.detect-timestamp-field".
//
// Payload:
//
//	{ "registry_id": <uint>, "target_table": "<optional>" }
//
// Behaviour:
//   - Loads the registry row (by registry_id OR target_table fallback).
//   - Runs TimestampDetector.DetectForCollection with the candidate chain
//     from the registry (or the default chain if empty).
//   - If registry.timestamp_field_source == 'admin_override', the detector
//     still runs (so operators can see the result) but the registry row
//     is NOT mutated — UI publishes for review only.
//   - Otherwise: updates timestamp_field / _detected_at / _confidence.
//   - Publishes result to msg.Reply when set so the caller gets the
//     detection payload synchronously.
//
// Failure handling: any detection error is logged + sent to the reply
// subject as `{error: "..."}` so callers can distinguish infra failure
// from legitimate low-confidence detection.
func (h *ReconHandler) HandleDetectTimestampField(msg *nats.Msg) {
	var payload struct {
		RegistryID  uint   `json:"registry_id"`
		TargetTable string `json:"target_table"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Warn("detect-timestamp-field: bad payload", zap.Error(err))
		return
	}

	if h.tsDetector == nil {
		h.logger.Warn("detect-timestamp-field: detector not wired")
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]interface{}{"error": "detector not configured"})
			msg.Respond(resp)
		}
		return
	}

	ctx := context.Background()
	var entry model.TableRegistry
	q := h.db.WithContext(ctx)
	if payload.RegistryID > 0 {
		q = q.Where("id = ?", payload.RegistryID)
	} else if payload.TargetTable != "" {
		q = q.Where("target_table = ?", payload.TargetTable)
	} else {
		h.logger.Warn("detect-timestamp-field: missing registry_id and target_table")
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]interface{}{"error": "registry_id or target_table required"})
			msg.Respond(resp)
		}
		return
	}
	if err := q.First(&entry).Error; err != nil {
		h.logger.Warn("detect-timestamp-field: registry lookup failed",
			zap.Uint("registry_id", payload.RegistryID),
			zap.String("target_table", payload.TargetTable),
			zap.Error(err))
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]interface{}{"error": "registry not found"})
			msg.Respond(resp)
		}
		return
	}

	candidates := entry.GetCandidates()
	result, err := h.tsDetector.DetectForCollection(ctx, entry.SourceDB, entry.SourceTable, candidates, 100)
	if err != nil {
		h.logger.Warn("detect-timestamp-field: detection failed",
			zap.String("target_table", entry.TargetTable),
			zap.Error(err))
		h.logActivity("detect-timestamp-field", entry.TargetTable, "error", 0, err)
		if msg.Reply != "" {
			resp, _ := json.Marshal(map[string]interface{}{"error": err.Error()})
			msg.Respond(resp)
		}
		return
	}

	source := "auto"
	if entry.TimestampFieldSource != nil {
		source = *entry.TimestampFieldSource
	}
	if source == "admin_override" {
		h.logger.Info("detect-timestamp-field: admin_override — registry not mutated",
			zap.String("target_table", entry.TargetTable),
			zap.String("detected_field", result.Field),
			zap.String("confidence", result.Confidence),
		)
	} else {
		now := time.Now().UTC()
		updates := map[string]interface{}{
			"timestamp_field":             result.Field,
			"timestamp_field_detected_at": now,
			"timestamp_field_confidence":  result.Confidence,
			"timestamp_field_source":      "auto",
		}
		if err := h.db.WithContext(ctx).Model(&model.TableRegistry{}).
			Where("id = ?", entry.ID).Updates(updates).Error; err != nil {
			h.logger.Warn("detect-timestamp-field: registry update failed",
				zap.Uint("id", entry.ID), zap.Error(err))
		}
	}

	h.logActivity("detect-timestamp-field", entry.TargetTable, "success", 0, nil)

	if msg.Reply != "" {
		resp, _ := json.Marshal(map[string]interface{}{
			"registry_id":  entry.ID,
			"target_table": entry.TargetTable,
			"result":       result,
			"source":       source,
		})
		msg.Respond(resp)
	}
}

func (h *ReconHandler) updateFailedLog(id uint64, status, errMsg string) {
	updates := map[string]interface{}{"status": status}
	if status == "resolved" {
		now := time.Now()
		updates["resolved_at"] = now
	}
	if errMsg != "" {
		updates["error_message"] = errMsg
	}
	updates["last_retry_at"] = time.Now()
	h.db.Model(&model.FailedSyncLog{}).Where("id = ?", id).Updates(updates)
}

func (h *ReconHandler) logActivity(operation, table, status string, rows int64, err error) {
	now := time.Now()
	var errPtr *string
	if err != nil {
		e := err.Error()
		errPtr = &e
	}
	h.db.Create(&model.ActivityLog{
		Operation:    operation,
		TargetTable:  table,
		Status:       status,
		RowsAffected: rows,
		ErrorMessage: errPtr,
		TriggeredBy:  "nats-command",
		StartedAt:    now,
		CompletedAt:  &now,
	})
}

func (h *ReconHandler) sanitizeRetryRawJSON(table, raw string) string {
	if h.masking != nil {
		return string(h.masking.MaskJSONPayload(table, []byte(raw)))
	}
	if json.Valid([]byte(raw)) {
		return raw
	}
	wrapped, _ := json.Marshal(map[string]string{"raw": raw})
	return string(wrapped)
}
