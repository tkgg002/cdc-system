package handler

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/metrics"
	"centralized-data-service/pkgs/observability"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// otelTraceSpanFromContext returns the active span in ctx, or nil when
// tracing is disabled / no span is present. Wrapped so the hot-path
// reader doesn't have to import oteltrace directly.
func otelTraceSpanFromContext(ctx context.Context) oteltrace.Span {
	sp := oteltrace.SpanFromContext(ctx)
	if !sp.SpanContext().IsValid() {
		return nil
	}
	return sp
}

// KafkaConsumerConfig holds Kafka consumer configuration.
// TopicPrefix is a list — multi-engine unified pipeline subscribes to
// the union of all prefixes (e.g. cdc.gpay + cdc.goopay + cdc.mariadb).
type KafkaConsumerConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	GroupID           string   `mapstructure:"groupId"`
	TopicPrefix       []string `mapstructure:"topicPrefix"`
	SchemaRegistryURL string   `mapstructure:"schemaRegistryUrl"`
}

// avro schema cache
var schemaCache = make(map[int32]*goavro.Codec)

// batchStats tracks per-topic batch metrics for Activity Log
type batchStats struct {
	topic     string
	processed int
	success   int
	failed    int
	startTime time.Time
}

// KafkaConsumer consumes CDC events from Kafka (Debezium → Kafka → Worker)
type KafkaConsumer struct {
	config       KafkaConsumerConfig
	eventHandler *EventHandler
	registrySvc  interface{ GetDebeziumTables() []string }
	validator    *service.SchemaValidator
	masking      *service.MaskingService
	logger       *zap.Logger
	readers      []*kafka.Reader
	db           *gorm.DB
	batches      map[string]*batchStats // per-topic batch accumulator
	// P0.1 additions — reader manager fields
	refreshMu     sync.Mutex
	currentTopics []string
	// discoverFunc allows test injection of topic discovery. If non-nil,
	// discoverTopics delegates to it instead of dialing Kafka directly.
	discoverFunc func(ctx context.Context) ([]string, error)
}

func NewKafkaConsumer(cfg KafkaConsumerConfig, handler *EventHandler, registrySvc interface{ GetDebeziumTables() []string }, db *gorm.DB, logger *zap.Logger) *KafkaConsumer {
	return &KafkaConsumer{
		config:       cfg,
		eventHandler: handler,
		registrySvc:  registrySvc,
		db:           db,
		logger:       logger,
		batches:      make(map[string]*batchStats),
	}
}

// SetSchemaValidator wires the Phase A JSON validator. Called during
// worker wiring. Nil validator = validation disabled (legacy mode).
func (kc *KafkaConsumer) SetSchemaValidator(v *service.SchemaValidator) {
	kc.validator = v
}

func (kc *KafkaConsumer) SetMaskingService(masking *service.MaskingService) {
	kc.masking = masking
}

// buildReader creates a new kafka.Reader for the given topic set using the
// consumer's config. Does NOT connect immediately — kafka-go dials lazily.
func (kc *KafkaConsumer) buildReader(topics []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:          kc.config.Brokers,
		GroupID:          kc.config.GroupID,
		GroupTopics:      topics,
		MinBytes:         10e3, // 10KB
		MaxBytes:         10e6, // 10MB
		CommitInterval:   time.Second,
		SessionTimeout:   30 * time.Second,
		RebalanceTimeout: 30 * time.Second,
		StartOffset:      kafka.FirstOffset,
		Logger:           nil, // suppress kafka-go internal logs
	})
}

// RefreshTopics re-discovers Kafka topics and recreates the reader if the
// topic set has changed. Idempotent. Safe for concurrent callers
// (NATS handler + background ticker + test).
func (kc *KafkaConsumer) RefreshTopics(ctx context.Context) error {
	// P0.1 close-loop: ensure registry cache is fresh before discovering.
	// Without this, topics newly registered via admin-api won't pass the
	// debeziumTables filter in discoverTopics and will be silently dropped.
	if reloader, ok := kc.registrySvc.(interface{ ReloadAll(context.Context) error }); ok {
		if err := reloader.ReloadAll(ctx); err != nil {
			kc.logger.Warn("registry reload failed during topic refresh", zap.Error(err))
			// non-fatal — continue with stale cache rather than fail refresh entirely
		}
	}

	newTopics, err := kc.discoverTopics(ctx)
	if err != nil {
		return fmt.Errorf("discover topics: %w", err)
	}

	kc.refreshMu.Lock()
	defer kc.refreshMu.Unlock()

	if topicSetEqual(kc.currentTopics, newTopics) {
		kc.logger.Debug("topic set unchanged, skipping refresh",
			zap.Int("count", len(newTopics)))
		return nil
	}

	kc.logger.Info("topic set changed, recreating reader",
		zap.Strings("old", kc.currentTopics),
		zap.Strings("new", newTopics))

	kc.flushAllBatches()

	// Close old readers with a 5s timeout per reader so we don't block the
	// consume loop indefinitely. kafka-go Close() blocks until the fetch loop
	// inside it returns; wrapping in a goroutine + select keeps us non-blocking.
	for _, r := range kc.readers {
		r := r // capture
		done := make(chan struct{})
		go func() {
			defer close(done)
			if cerr := r.Close(); cerr != nil {
				kc.logger.Warn("close old reader", zap.Error(cerr))
			}
		}()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		select {
		case <-done:
		case <-closeCtx.Done():
			kc.logger.Warn("old reader close timed out after 5s, continuing")
		}
		cancel()
	}
	kc.readers = nil

	newReader := kc.buildReader(newTopics)
	kc.readers = append(kc.readers, newReader)
	kc.currentTopics = append([]string(nil), newTopics...)

	return nil
}

// topicSetEqual reports whether a and b contain the same topic names
// regardless of order.
func topicSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	for _, t := range b {
		if _, ok := set[t]; !ok {
			return false
		}
	}
	return true
}

// Start discovers Kafka topics matching prefix and starts consuming
func (kc *KafkaConsumer) Start(ctx context.Context) {
	kc.logger.Info("kafka consumer starting",
		zap.Strings("brokers", kc.config.Brokers),
		zap.String("group", kc.config.GroupID),
		zap.Strings("prefixes", kc.config.TopicPrefix),
	)

	// Wait for Kafka to be ready
	time.Sleep(10 * time.Second)

	// Discover topics matching prefix AND registry sync_engine=debezium
	topics, err := kc.discoverTopics(ctx)
	if err != nil {
		kc.logger.Error("failed to discover kafka topics, retrying in 30s", zap.Error(err))
		time.Sleep(30 * time.Second)
		topics, err = kc.discoverTopics(ctx)
		if err != nil {
			kc.logger.Fatal("kafka topic discovery failed", zap.Error(err))
			return
		}
	}

	if len(topics) == 0 {
		kc.logger.Warn("no kafka topics found matching prefix, will retry periodically",
			zap.Strings("prefixes", kc.config.TopicPrefix))
		retryTicker := time.NewTicker(60 * time.Second)
		defer retryTicker.Stop()
		for len(topics) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-retryTicker.C:
				topics, err = kc.discoverTopics(ctx)
				if err != nil {
					kc.logger.Warn("kafka topic discovery retry failed", zap.Error(err))
				}
			}
		}
	}

	// === P0.1 — initial reader build via shared helper ===
	kc.refreshMu.Lock()
	reader := kc.buildReader(topics)
	kc.readers = append(kc.readers, reader)
	kc.currentTopics = append([]string(nil), topics...)
	kc.refreshMu.Unlock()

	kc.logger.Info("kafka consumer started",
		zap.Strings("topics", topics),
		zap.String("group", kc.config.GroupID),
	)

	// Batch flush ticker — flush every 5 seconds
	flushTicker := time.NewTicker(5 * time.Second)
	defer flushTicker.Stop()

	// P0.1 safety net — auto-refresh topic set every 60s.
	refreshTicker := time.NewTicker(60 * time.Second)
	defer refreshTicker.Stop()

	// Consume loop
	for {
		select {
		case <-ctx.Done():
			kc.flushAllBatches() // flush remaining
			kc.Stop()
			return
		case <-flushTicker.C:
			kc.flushAllBatches()
		case <-refreshTicker.C:
			if err := kc.RefreshTopics(ctx); err != nil {
				kc.logger.Warn("auto refresh topics failed", zap.Error(err))
			}
		default:
			kc.refreshMu.Lock()
			if len(kc.readers) == 0 {
				kc.refreshMu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			currentReader := kc.readers[0]
			kc.refreshMu.Unlock()

			msg, err := currentReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// If reader was recreated mid-fetch, error is expected.
				if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "reader closed") {
					kc.logger.Debug("reader closed during refresh, retrying", zap.Error(err))
					time.Sleep(200 * time.Millisecond)
					continue
				}
				kc.logger.Error("kafka fetch error", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			start := time.Now()

			// T14 / plan WORKER task #10 — OTel Kafka trace context.
			// Extract W3C traceparent + tracestate from Kafka message
			// headers so the span created here becomes a CHILD of the
			// upstream producer span when one exists (future: Debezium
			// Kafka Connect interceptor). When no parent header is
			// present we still start a root span so SigNoz has a
			// message-scoped trace.
			carrier := propagation.MapCarrier{}
			for _, h := range msg.Headers {
				carrier[h.Key] = string(h.Value)
			}
			parentCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

			spanCtx, span := observability.StartSpan(parentCtx, "kafka.consume",
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", msg.Topic),
				attribute.String("messaging.operation", "receive"),
				attribute.Int("messaging.kafka.partition", msg.Partition),
				attribute.Int64("messaging.kafka.offset", msg.Offset),
				attribute.Int64("messaging.kafka.message.timestamp_ms", msg.Time.UnixMilli()),
			)

			// T9: E2E latency — Kafka message timestamp (set by Debezium) → now
			if !msg.Time.IsZero() {
				e2eLatency := time.Since(msg.Time)
				metrics.E2ELatency.Observe(e2eLatency.Seconds())
				span.SetAttributes(attribute.Float64("e2e_latency_seconds", e2eLatency.Seconds()))
			}

			// T6: Track batch stats per topic
			batch := kc.getOrCreateBatch(msg.Topic)

			procErr := kc.processMessage(spanCtx, msg)
			if procErr != nil {
				kc.logger.Error("kafka message processing failed",
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Error(procErr),
				)
				span.SetAttributes(attribute.String("error", procErr.Error()))
				metrics.EventsProcessed.WithLabelValues("error", "", msg.Topic, "error").Inc()
				batch.failed++
			} else {
				duration := time.Since(start)
				metrics.EventsProcessed.WithLabelValues("kafka", "", msg.Topic, "success").Inc()
				metrics.ProcessingDuration.WithLabelValues("kafka", "", msg.Topic).Observe(duration.Seconds())
				span.SetAttributes(attribute.Float64("duration_seconds", duration.Seconds()))
				batch.success++
			}
			batch.processed++
			span.End()

			// Flush batch if reached 100 messages
			if batch.processed >= 100 {
				kc.flushBatch(msg.Topic)
			}

			// v3 §9: DLQ write-before-ACK. If processing failed, we MUST
			// persist the failure into failed_sync_logs BEFORE committing
			// the Kafka offset. A DLQ insert error keeps the offset so
			// the broker redelivers on the next fetch (at-least-once
			// guarantee preserved). Successful process → commit directly.
			if procErr != nil {
				if dlqErr := kc.writeDLQ(ctx, msg, procErr); dlqErr != nil {
					service.DLQWriteFail.Inc()
					kc.logger.Error("kafka DLQ write failed — skipping offset commit for redelivery",
						zap.String("topic", msg.Topic),
						zap.Int("partition", msg.Partition),
						zap.Int64("offset", msg.Offset),
						zap.Error(dlqErr),
					)
					// Do NOT CommitMessages — Kafka redelivers. Continue loop.
					continue
				}
			}

			if err := currentReader.CommitMessages(ctx, msg); err != nil {
				kc.logger.Error("kafka commit failed", zap.Error(err))
			}
		}
	}
}

// processMessage converts Kafka message to CDC event and delegates to EventHandler
func (kc *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	// Debezium Kafka message value contains the CDC event
	// For Avro: need to strip schema registry header (magic byte + schema ID = 5 bytes)
	// For JSON: parse directly

	value := msg.Value
	if len(value) == 0 {
		return nil // tombstone
	}

	// Parse message: Avro (with Schema Registry) or JSON
	var event map[string]interface{}
	if len(value) > 5 && value[0] == 0 {
		// Avro format: 1 byte magic (0x00) + 4 bytes schema ID + Avro binary
		schemaID := int32(binary.BigEndian.Uint32(value[1:5]))
		avroData := value[5:]

		codec, err := kc.getAvroCodec(schemaID)
		if err != nil {
			return fmt.Errorf("get avro schema %d: %w", schemaID, err)
		}

		native, _, err := codec.NativeFromBinary(avroData)
		if err != nil {
			return fmt.Errorf("avro decode (schema %d): %w", schemaID, err)
		}

		var ok bool
		event, ok = native.(map[string]interface{})
		if !ok {
			return fmt.Errorf("avro decoded to %T, expected map", native)
		}
	} else {
		// JSON format fallback
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("parse kafka message as JSON: %w", err)
		}
	}

	// Unwrap Avro union types: goavro decodes unions as map[string]interface{}{"type": value}
	op := unwrapAvroUnion(event["op"])
	afterRaw := unwrapAvroUnion(event["after"])
	beforeRaw := unwrapAvroUnion(event["before"])
	// NOTE: do NOT unwrapAvroUnion on `source` — Debezium's Source record
	// is a non-nullable Avro record (not a union), so unwrapping would
	// strip a random field. Pass the raw map through.
	sourceRaw := event["source"]

	// Parse after data — Debezium MongoDB sends JSON string in "after" field
	var afterData map[string]interface{}
	switch v := afterRaw.(type) {
	case map[string]interface{}:
		afterData = v
	case string:
		json.Unmarshal([]byte(v), &afterData)
	}

	// Parse before data — same pattern as after (needed for DELETE events with REPLICA IDENTITY FULL)
	var beforeData map[string]interface{}
	switch v := beforeRaw.(type) {
	case map[string]interface{}:
		beforeData = v
	case string:
		json.Unmarshal([]byte(v), &beforeData)
	}

	// B9 — Unwrap Avro union envelopes for every field value.
	// Avro nullable union ["null","string"] decodes to {"string":"x"} for non-null values.
	// pgx cannot encode map[string]interface{} into timestamp/int columns — unwrap first.
	if afterData != nil {
		afterData = unwrapAvroUnionMap(afterData)
	}
	if beforeData != nil {
		beforeData = unwrapAvroUnionMap(beforeData)
	}

	// Extract Debezium source.ts_ms (milliseconds since epoch). Missing
	// or malformed → 0 (downstream skips OCC guard).
	sourceTsMs := extractSourceTsMs(sourceRaw)

	// Plan WORKER task #10 — attach source.ts_ms to the active span so
	// SigNoz shows CDC lag (consumer-ts - source-ts) at the span level.
	if span := otelTraceSpanFromContext(ctx); span != nil {
		span.SetAttributes(
			attribute.Int64("source.ts_ms", sourceTsMs),
		)
	}

	opStr := fmt.Sprintf("%v", op)
	if afterData == nil && opStr != "d" {
		kc.logger.Debug("kafka message has no 'after' data, skipping",
			zap.String("topic", msg.Topic),
			zap.String("op", opStr),
		)
		return nil
	}

	// v3 Phase A schema validation — reject messages whose payload
	// drifted vs cdc_table_registry / information_schema. The error
	// is caught by the top-level consumer loop which then writes the
	// message to failed_sync_logs (write-before-ACK).
	if kc.validator != nil && afterData != nil {
		// Extract table name from topic: cdc.goopay.{db}.{collection}
		parts := strings.Split(msg.Topic, ".")
		if len(parts) >= 4 {
			tbl := parts[3]
			if err := kc.validator.ValidatePayloadWithCase(tbl, afterData); err != nil {
				return fmt.Errorf("schema_validator: %w", err)
			}
		}
	}

	// Build CDCEvent-compatible JSON for EventHandler
	// A1 fix: pass beforeData (parsed from Avro, may be nil for INSERT/non-FULL-identity ops)
	var beforeField interface{}
	if len(beforeData) > 0 {
		beforeField = beforeData
	}
	cdcEvent := map[string]interface{}{
		"source": fmt.Sprintf("/kafka/%s", msg.Topic),
		"data": map[string]interface{}{
			"op":           opStr,
			"before":       beforeField,
			"after":        afterData,
			"source_ts_ms": sourceTsMs,
		},
	}

	cdcJSON, _ := json.Marshal(cdcEvent)
	subject := msg.Topic

	kc.logger.Info("kafka CDC event",
		zap.String("topic", msg.Topic),
		zap.String("op", opStr),
		zap.Int("partition", msg.Partition),
		zap.Int64("offset", msg.Offset),
		zap.Int("after_fields", len(afterData)),
		zap.Int64("source_ts_ms", sourceTsMs),
	)

	return kc.eventHandler.HandleRaw(ctx, subject, cdcJSON)
}

// extractSourceTsMs pulls Debezium's `payload.source.ts_ms` (milliseconds
// since epoch) out of the decoded message source block. Supports both
// JSON converter (map[string]interface{}) and Avro (record) forms. The
// Debezium Source record is not a union, so the value is either a raw
// int64/long or — when the caller happens to pass a union-wrapped value
// — a single-key map.
// Returns 0 when not present / not numeric.
func extractSourceTsMs(source interface{}) int64 {
	m, ok := source.(map[string]interface{})
	if !ok {
		return 0
	}
	raw := m["ts_ms"]
	// Defensive: if some upstream converter wraps the scalar in a
	// single-key Avro union map, unwrap it.
	if mm, ok := raw.(map[string]interface{}); ok && len(mm) == 1 {
		for _, v := range mm {
			raw = v
			break
		}
	}
	switch v := raw.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case string:
		// Some converters serialise int64 as string when > 2^53
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

// discoverTopics finds Kafka topics matching ANY configured prefix
// (multi-engine unified pipeline). Returns the union, deduped.
// Logs per-prefix match count for observability.
// If kc.discoverFunc is set (test injection), it delegates there instead.
func (kc *KafkaConsumer) discoverTopics(ctx context.Context) ([]string, error) {
	if kc.discoverFunc != nil {
		return kc.discoverFunc(ctx)
	}
	conn, err := kafka.DialContext(ctx, "tcp", kc.config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}

	// Permissive registry filter — empty set means no filter, matching
	// legacy behaviour for fresh stacks without V1 active rows.
	debeziumTables := make(map[string]bool)
	if kc.registrySvc != nil {
		for _, t := range kc.registrySvc.GetDebeziumTables() {
			debeziumTables[t] = true
		}
	}

	topicNames := make([]string, 0, len(partitions))
	for _, p := range partitions {
		topicNames = append(topicNames, p.Topic)
	}

	topics, perPrefix, prefixes := filterMatchingTopics(topicNames, kc.config.TopicPrefix, debeziumTables)
	if len(prefixes) == 0 {
		kc.logger.Warn("kafka topic discovery: no prefixes configured")
		return nil, nil
	}

	fields := []zap.Field{
		zap.Strings("prefixes", prefixes),
		zap.Strings("topics", topics),
		zap.Int("debezium_tables", len(debeziumTables)),
	}
	for _, pre := range prefixes {
		fields = append(fields, zap.Int("count_"+pre, perPrefix[pre]))
	}
	kc.logger.Info("discovered kafka topics (multi-prefix union)", fields...)
	return topics, nil
}

// filterMatchingTopics is the pure core of discoverTopics — extracted
// so unit tests can verify multi-prefix union without a live broker.
//
// Returns: deduped union of matching topics, per-prefix match count,
// and the cleaned prefix list (blanks dropped).
func filterMatchingTopics(topicNames, configuredPrefixes []string, debeziumTables map[string]bool) ([]string, map[string]int, []string) {
	prefixes := make([]string, 0, len(configuredPrefixes))
	for _, p := range configuredPrefixes {
		p = strings.TrimSpace(p)
		if p != "" {
			prefixes = append(prefixes, p)
		}
	}
	if len(prefixes) == 0 {
		return nil, nil, nil
	}

	perPrefix := make(map[string]int, len(prefixes))
	topicSet := make(map[string]bool)
	out := make([]string, 0)

	for _, topic := range topicNames {
		if strings.HasPrefix(topic, "_") {
			continue
		}
		var matched string
		for _, pre := range prefixes {
			if strings.HasPrefix(topic, pre) {
				matched = pre
				break
			}
		}
		if matched == "" {
			continue
		}
		// Extract object name from `cdc.<prefix-tail>.<db>.<object>`.
		parts := strings.Split(topic, ".")
		var tableName string
		if len(parts) >= 4 {
			tableName = parts[len(parts)-1]
		}
		if len(debeziumTables) > 0 && !debeziumTables[tableName] {
			continue
		}
		if !topicSet[topic] {
			topicSet[topic] = true
			perPrefix[matched]++
			out = append(out, topic)
		}
	}
	return out, perPrefix, prefixes
}

// getAvroCodec fetches Avro schema from Schema Registry by ID and caches it
func (kc *KafkaConsumer) getAvroCodec(schemaID int32) (*goavro.Codec, error) {
	if codec, ok := schemaCache[schemaID]; ok {
		return codec, nil
	}

	url := fmt.Sprintf("%s/schemas/ids/%d", kc.config.SchemaRegistryURL, schemaID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch schema: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("schema registry %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse schema response: %w", err)
	}

	// Sanitize Avro schema names: replace `-` with `_` (Avro name spec: [A-Za-z0-9_])
	sanitizedSchema := sanitizeAvroSchemaNames(result.Schema)

	codec, err := goavro.NewCodec(sanitizedSchema)
	if err != nil {
		return nil, fmt.Errorf("create avro codec: %w", err)
	}

	schemaCache[schemaID] = codec
	kc.logger.Info("avro schema cached", zap.Int32("schema_id", schemaID))
	return codec, nil
}

// unwrapAvroUnion extracts value from goavro union type.
// goavro decodes union ["null","string"] as: nil (for null) or map[string]interface{}{"string": "value"}
// Non-map or multi-key maps pass through unchanged (they are real record types, not unions).
func unwrapAvroUnion(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]interface{}); ok && len(m) == 1 {
		// Single-key map → Avro union envelope: {"string": "actual_value"}
		for _, val := range m {
			return val
		}
	}
	return v
}

// unwrapAvroUnionMap applies unwrapAvroUnion to every top-level value in a map.
// Used to sanitize Debezium Avro "after" payloads so pgx receives native Go
// types (string, int64, etc.) rather than single-key map[string]interface{} envelopes.
func unwrapAvroUnionMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = unwrapAvroUnion(v)
	}
	return out
}

// sanitizeAvroSchemaNames replaces invalid chars in Avro name/namespace fields
// Avro spec: names must match [A-Za-z_][A-Za-z0-9_]*
func sanitizeAvroSchemaNames(schema string) string {
	// Parse, fix names, re-serialize
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		// Fallback: simple string replace for common pattern (dashes in names)
		return strings.ReplaceAll(schema, "-", "_")
	}
	fixNames(parsed)
	fixed, _ := json.Marshal(parsed)
	return string(fixed)
}

func fixNames(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, v2 := range val {
			if k == "name" || k == "namespace" {
				if s, ok := v2.(string); ok {
					val[k] = strings.ReplaceAll(s, "-", "_")
				}
			}
			fixNames(v2)
		}
	case []interface{}:
		for _, item := range val {
			fixNames(item)
		}
	}
}

// getOrCreateBatch returns the batch stats for a topic, creating if needed
func (kc *KafkaConsumer) getOrCreateBatch(topic string) *batchStats {
	if b, ok := kc.batches[topic]; ok {
		return b
	}
	b := &batchStats{topic: topic, startTime: time.Now()}
	kc.batches[topic] = b
	return b
}

// flushBatch writes batch Activity Log for a single topic and resets
func (kc *KafkaConsumer) flushBatch(topic string) {
	b, ok := kc.batches[topic]
	if !ok || b.processed == 0 {
		return
	}

	durationMs := int(time.Since(b.startTime).Milliseconds())
	details, _ := json.Marshal(map[string]interface{}{
		"topic":     b.topic,
		"processed": b.processed,
		"success":   b.success,
		"failed":    b.failed,
	})

	now := time.Now()
	kc.db.Create(&model.ActivityLog{
		Operation:    "kafka-consume-batch",
		TargetTable:  b.topic,
		Status:       "success",
		RowsAffected: int64(b.processed),
		DurationMs:   &durationMs,
		Details:      details,
		TriggeredBy:  "kafka-consumer",
		StartedAt:    b.startTime,
		CompletedAt:  &now,
	})

	delete(kc.batches, topic)
}

// flushAllBatches flushes batch stats for all topics
func (kc *KafkaConsumer) flushAllBatches() {
	for topic := range kc.batches {
		kc.flushBatch(topic)
	}
}

// writeDLQ persists a failed Kafka message into `failed_sync_logs`.
//
// The insert is the ONLY ground truth linking a Kafka offset to a
// retryable row. The caller (consume loop) guarantees we do NOT
// CommitMessages until this INSERT returns nil — so any error here
// means Kafka redelivers the same message on the next fetch.
//
// Status is derived from the error class:
//   - schema_validator: schema_drift / missing_required → status='pending'
//     (worker retry will NOT fix it; an ops action on cdc_table_registry
//     or migration is needed). We still queue the row so the CMS DLQ
//     drill-down has evidence.
//   - other errors → status='failed'. The DLQ worker retries with
//     exponential backoff.
func (kc *KafkaConsumer) writeDLQ(ctx context.Context, msg kafka.Message, procErr error) error {
	// Parse topic to extract target table name.
	var targetTable string
	parts := strings.Split(msg.Topic, ".")
	if len(parts) >= 4 {
		targetTable = parts[3]
	}

	// Try to pull record_id from the message body for DLQ diagnostics.
	recordID, operation, rawJSON := extractDLQMetadata(msg)
	rawJSON = kc.sanitizeDLQRawJSON(targetTable, rawJSON)

	errorType := "processing"
	status := "failed"
	errText := procErr.Error()
	sanitizedErrText := service.SanitizeFreeformText(errText, 2000)
	if strings.Contains(errText, "schema_drift") {
		errorType = "schema_drift"
		status = "pending"
	} else if strings.Contains(errText, "missing_required_field") {
		errorType = "missing_required"
		status = "pending"
	}

	partition := msg.Partition
	offset := msg.Offset
	row := model.FailedSyncLog{
		TargetTable:    targetTable,
		SourceTable:    targetTable,
		RecordID:       recordID,
		Operation:      operation,
		RawJSON:        rawJSON,
		ErrorMessage:   sanitizedErrText,
		ErrorType:      errorType,
		KafkaTopic:     msg.Topic,
		KafkaPartition: &partition,
		KafkaOffset:    &offset,
		RetryCount:     0,
		MaxRetries:     5,
		Status:         status,
	}
	// GORM Create on a partitioned parent is transparent — Postgres
	// routes into the right monthly partition via created_at default.
	return kc.db.WithContext(ctx).Create(&row).Error
}

// extractDLQMetadata pulls (recordID, operation, rawJSON) out of a
// Debezium Kafka message for DLQ diagnostics. Best-effort — missing
// fields become empty strings / empty JSON.
func extractDLQMetadata(msg kafka.Message) (string, string, []byte) {
	// Key often carries the JSON-encoded _id. Use it verbatim when set.
	var recordID string
	if len(msg.Key) > 0 {
		// Debezium encodes key as: {"id":"<oid-hex>"} or a bare string.
		var keyMap map[string]interface{}
		if err := json.Unmarshal(msg.Key, &keyMap); err == nil {
			if v, ok := keyMap["id"]; ok {
				recordID = fmt.Sprintf("%v", v)
			}
		}
		if recordID == "" {
			recordID = strings.Trim(string(msg.Key), `"`)
		}
	}

	var operation string
	if len(msg.Value) > 0 {
		var v map[string]interface{}
		if err := json.Unmarshal(msg.Value, &v); err == nil {
			if op, ok := v["op"].(string); ok {
				operation = op
			}
			// If we didn't get ID from key, try payload.after._id
			if recordID == "" {
				if after, ok := v["after"].(map[string]interface{}); ok {
					if id, ok := after["_id"]; ok {
						recordID = fmt.Sprintf("%v", id)
					}
				}
				if recordID == "" {
					if after, ok := v["after"].(string); ok {
						var afterMap map[string]interface{}
						if err := json.Unmarshal([]byte(after), &afterMap); err == nil {
							if id, ok := afterMap["_id"]; ok {
								recordID = fmt.Sprintf("%v", id)
							}
						}
					}
				}
			}
		}
	}

	// Extract the Kafka value into a JSONB-friendly payload. 
	// Track B5: If value contains binary/non-UTF8 (Avro/Protobuf), 
	// PostgreSQL will reject plain string insert. Base64 encode instead.
	raw := msg.Value
	if !json.Valid(raw) || !utf8.Valid(raw) {
		encoded := base64.StdEncoding.EncodeToString(raw)
		wrapped, _ := json.Marshal(map[string]string{
			"raw_base64": encoded,
			"encoding":   "base64",
			"note":       "original payload contained binary or invalid utf8",
		})
		raw = wrapped
	}
	return recordID, operation, raw
}

func (kc *KafkaConsumer) sanitizeDLQRawJSON(table string, raw []byte) json.RawMessage {
	if kc.masking != nil {
		return kc.masking.MaskJSONPayload(table, raw)
	}
	if json.Valid(raw) {
		return json.RawMessage(raw)
	}
	wrapped, _ := json.Marshal(map[string]string{"raw": string(raw)})
	return json.RawMessage(wrapped)
}

// Stop closes all Kafka readers
func (kc *KafkaConsumer) Stop() {
	for _, r := range kc.readers {
		r.Close()
	}
	kc.logger.Info("kafka consumer stopped")
}
