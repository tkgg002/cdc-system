package sinkworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"centralized-data-service/pkgs/idgen"
)

// SinkWorker implements plan v7.2 §2 — the new pipeline that lands
// Debezium events into cdc_internal shadow tables with 10 system fields
// enforced, schema-on-read, and full fencing-trigger protection.
//
// Concurrency note: a single SinkWorker is *not* thread-safe for reuse
// across Kafka reader goroutines because ordering and OCC rely on a
// serial write stream. Spawn one per kafka.Reader if you need parallelism
// per topic.
type SinkWorker struct {
	db            *gorm.DB
	schemaManager *SchemaManager
	avro          *avroDecoder
	machineID     int
	fencingToken  int64
	source        string
	logger        *zap.Logger
	// natsConn, when non-nil, enables real-time Shadow → Master fan-out.
	// After a successful shadow upsert the worker publishes a lightweight
	// trigger to cdc.cmd.transmute — the Transmuter picks it up and
	// materialises master rows keyed by _gpay_source_id (plan v2 §R6.2).
	natsConn *nats.Conn
}

// Config bundles the wiring dependencies so callers don't have to pass
// seven positional arguments.
type Config struct {
	DB                *gorm.DB
	SchemaManager     *SchemaManager
	SchemaRegistryURL string
	MachineID         int
	FencingToken      int64
	// Source is the string stamped into _source for every record. Defaults
	// to "debezium-v125" when empty so production traffic is always tagged
	// with the pipeline version that wrote it.
	Source string
	Logger *zap.Logger
	// NATSConn enables post-ingest Transmute fan-out when non-nil.
	NATSConn *nats.Conn
}

func New(cfg Config) *SinkWorker {
	src := cfg.Source
	if src == "" {
		src = "debezium-v125"
	}
	return &SinkWorker{
		db:            cfg.DB,
		schemaManager: cfg.SchemaManager,
		avro:          newAvroDecoder(cfg.SchemaRegistryURL),
		machineID:     cfg.MachineID,
		fencingToken:  cfg.FencingToken,
		source:        src,
		logger:        cfg.Logger,
		natsConn:      cfg.NATSConn,
	}
}

// HandleMessage processes ONE Debezium Kafka message end-to-end.
//
// Pipeline (maps onto plan §6.1 six-step systematic mapping, with every
// §7 gap fixed):
//
//  1. Decode envelope (Avro or JSON). Missing value => tombstone => noop.
//  2. Extract op, `after`, `source.ts_ms`, `_gpay_source_id`. (§7.1, §7.2)
//  3. Build 10-field system record. `_raw_data` = canonical JSON of the
//     full envelope (Option B, user-approved). (§7.3)
//  4. Merge business fields from `after`, skipping any key that collides
//     with system namespace. (§7.4)
//  5. Ensure the shadow table matches the record's columns (T1.3).
//  6. Within a single transaction: SET LOCAL fencing vars, then UPSERT
//     with OCC guard. (§7.5 + §7.6)
func (w *SinkWorker) HandleMessage(ctx context.Context, msg kafka.Message) error {
	envelope, err := w.avro.decodeMessage(msg.Value)
	if err != nil {
		return fmt.Errorf("decode envelope offset=%d: %w", msg.Offset, err)
	}
	if envelope == nil {
		// Debezium tombstone — nothing to write. Caller commits offset.
		return nil
	}

	op := fmt.Sprintf("%v", unwrapAvroUnion(envelope["op"]))
	if op == "" {
		op = "c"
	}

	after, err := decodeAfter(envelope["after"])
	if err != nil {
		return fmt.Errorf("decode after: %w", err)
	}
	if after == nil {
		after = map[string]any{}
	}

	sourceID := extractSourceID(after, msg.Key)
	if sourceID == "" {
		return fmt.Errorf("cannot extract _gpay_source_id: topic=%s offset=%d", msg.Topic, msg.Offset)
	}

	// §7.1 — _source_ts anchors OCC; fall back to 0 (guard allows update).
	sourceTsMs := extractSourceTsMs(envelope)

	deleted := op == "d"

	sfID, err := idgen.NextID()
	if err != nil {
		return fmt.Errorf("sonyflake next id: %w", err)
	}

	// §7.3 — hash spans the full envelope so the audit record proves both
	// the payload and the Debezium metadata at ingest time. Canonicalise
	// so semantically-equal envelopes always produce the same hash.
	//
	// Pre-process: unwrap Avro unions + parse `after` (which Debezium MongoDB
	// sends as a JSON string) so _raw_data reads like a clean document to
	// downstream SQL, not like nested Avro framing.
	cleanEnv := cleanEnvelopeForStorage(envelope, after)
	envJSON, err := canonicalJSON(cleanEnv)
	if err != nil {
		return fmt.Errorf("canonical envelope: %w", err)
	}
	hash := sha256Hex(envJSON)

	now := time.Now()
	record := map[string]any{
		"_gpay_id":        int64(sfID), // cast so GORM binds as BIGINT, not NUMERIC
		"_gpay_source_id": sourceID,
		"_raw_data":       string(envJSON), // JSONB accepts text literal
		"_source":         w.source,
		"_synced_at":      now,
		"_source_ts":      sourceTsMs,
		"_version":        int64(1), // row-version managed by trigger/OCC, not Debezium
		"_hash":           hash,
		"_gpay_deleted":   deleted,
		"_created_at":     now,
		"_updated_at":     now,
	}

	// §7.4 — merge business, skip ALL system-reserved keys plus Mongo `_id`
	// (captured separately as _gpay_source_id). gjson-free path: we already
	// have after as map[string]any from decodeAfter.
	for k, v := range after {
		if shouldSkipBusinessKey(k) {
			continue
		}
		record[k] = v
	}

	table := extractTableFromTopic(msg.Topic)
	if table == "" {
		return fmt.Errorf("cannot derive table from topic %q", msg.Topic)
	}

	if err := w.schemaManager.EnsureShadowTable(ctx, table, record); err != nil {
		return fmt.Errorf("schema ensure %s: %w", table, err)
	}

	snap := isSnapshotEvent(envelope)
	if err := w.upsertWithFencing(ctx, table, record, snap); err != nil {
		return fmt.Errorf("upsert %s: %w", table, err)
	}
	if snap {
		w.logger.Debug("snapshot event upserted",
			zap.String("table", table),
			zap.String("source_id", sourceID),
		)
	}
	// Post-ingest Transmute fan-out (plan v2 §R6.2). Best-effort publish —
	// shadow write is already durable, so a NATS blip only delays master
	// materialisation until the next scheduler tick or manual trigger.
	w.publishTransmuteTrigger(table, sourceID)
	return nil
}

// publishTransmuteTrigger fires one NATS message per successful shadow
// upsert. The Transmuter consumes cdc.cmd.transmute and incrementally
// materialises the master rows keyed by this source_id. Fire-and-forget:
// any publish error is logged but never bubbled up.
func (w *SinkWorker) publishTransmuteTrigger(shadowTable, sourceID string) {
	if w.natsConn == nil {
		return
	}
	// Master table discovery is delayed to the Transmuter side so this
	// hook stays cheap. We publish one message per shadow table; the
	// Transmuter enumerates all active masters for that shadow.
	payload, err := json.Marshal(map[string]any{
		"shadow_table":   shadowTable,
		"source_ids":     []string{sourceID},
		"triggered_by":   "sinkworker",
		"correlation_id": fmt.Sprintf("sink-%s-%d", shadowTable, time.Now().UnixNano()),
	})
	if err != nil {
		w.logger.Warn("transmute trigger marshal failed", zap.Error(err))
		return
	}
	if err := w.natsConn.Publish("cdc.cmd.transmute-shadow", payload); err != nil {
		w.logger.Warn("transmute trigger publish failed",
			zap.String("table", shadowTable), zap.Error(err))
	}
}

// upsertWithFencing wraps SET LOCAL fencing vars + UPSERT in a single
// transaction. SET LOCAL must precede any INSERT because `tg_fencing_guard`
// runs BEFORE INSERT OR UPDATE and will raise if the session vars are
// missing (§7.6). When isSnapshot is true, the insert uses ON CONFLICT
// DO NOTHING so snapshot replay cannot clobber newer streaming updates.
func (w *SinkWorker) upsertWithFencing(ctx context.Context, table string, record map[string]any, isSnapshot bool) error {
	return w.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			`SELECT set_config('app.fencing_machine_id', ?, true), set_config('app.fencing_token', ?, true)`,
			fmt.Sprintf("%d", w.machineID),
			fmt.Sprintf("%d", w.fencingToken),
		).Error; err != nil {
			return fmt.Errorf("set fencing vars: %w", err)
		}

		var sqlText string
		var values []any
		if isSnapshot {
			sqlText, values = buildUpsertSQLSnapshot(table, record)
		} else {
			sqlText, values = buildUpsertSQL(table, record)
		}
		if err := tx.Exec(sqlText, values...).Error; err != nil {
			return fmt.Errorf("exec upsert: %w", err)
		}
		return nil
	})
}

// shouldSkipBusinessKey guards against business fields colliding with the
// 10 system columns, with three explicit extras:
//   - Any `_gpay_*` (reserved namespace — plan §7.4)
//   - `_id` Mongo ObjectID — already captured as _gpay_source_id
//   - `__v`, `__v2` Mongo internal version markers — noisy, skip
func shouldSkipBusinessKey(k string) bool {
	if strings.HasPrefix(k, "_gpay_") {
		return true
	}
	switch k {
	case "_id", "_raw_data", "_source", "_synced_at", "_source_ts",
		"_version", "_hash", "_created_at", "_updated_at":
		return true
	}
	if strings.HasPrefix(k, "__v") {
		return true
	}
	return false
}

// extractTableFromTopic maps a Debezium topic name to a shadow table name.
// Convention: cdc.<db-server>.<database>.<collection>. Any hyphen in the
// collection becomes underscore (Postgres identifiers). Uppercase lowered
// for consistency.
func extractTableFromTopic(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 3 {
		return ""
	}
	coll := parts[len(parts)-1]
	coll = strings.ReplaceAll(coll, "-", "_")
	return strings.ToLower(coll)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// cleanEnvelopeForStorage rebuilds the Debezium envelope with Avro union
// wrappers stripped and `after` replaced by the already-parsed document.
// The returned map is safe to hand to canonicalJSON and is semantically
// equivalent to (but far more readable than) the raw goavro output.
// Option B per plan §7.3 — full envelope preserved.
func cleanEnvelopeForStorage(envelope map[string]any, after map[string]any) map[string]any {
	out := make(map[string]any, len(envelope))
	for k, v := range envelope {
		switch k {
		case "after":
			// Prefer the already-parsed map from decodeAfter.
			out["after"] = after
		case "before":
			if parsed, err := decodeAfter(v); err == nil {
				out["before"] = parsed
			} else {
				out["before"] = nil
			}
		default:
			out[k] = unwrapAvroUnion(v)
		}
	}
	return out
}
