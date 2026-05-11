package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/metrics"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type BatchBuffer struct {
	records       []*model.UpsertRecord
	mu            sync.Mutex
	maxSize       int
	timeout       time.Duration
	db            *gorm.DB
	schemaAdapter *service.SchemaAdapter
	connMgr       *service.ConnectionManager
	adapters      sync.Map // role:key -> *service.SchemaAdapter
	masking       *service.MaskingService
	logger        *zap.Logger
	flushCh       chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	lastFlush     time.Time
}

type BatchStatus struct {
	BufferSize int       `json:"buffer_size"`
	MaxSize    int       `json:"max_size"`
	LastFlush  time.Time `json:"last_flush"`
}

func NewBatchBuffer(maxSize int, timeout time.Duration, db *gorm.DB, schemaAdapter *service.SchemaAdapter, logger *zap.Logger) *BatchBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	bb := &BatchBuffer{
		records:       make([]*model.UpsertRecord, 0, maxSize),
		maxSize:       maxSize,
		timeout:       timeout,
		db:            db,
		schemaAdapter: schemaAdapter,
		logger:        logger,
		flushCh:       make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
	}
	go bb.timerLoop()
	return bb
}

func (bb *BatchBuffer) SetMaskingService(masking *service.MaskingService) {
	bb.masking = masking
}

func (bb *BatchBuffer) SetConnectionManager(connMgr *service.ConnectionManager) {
	bb.connMgr = connMgr
}

func (bb *BatchBuffer) Add(record *model.UpsertRecord) {
	bb.mu.Lock()
	bb.records = append(bb.records, record)
	shouldFlush := len(bb.records) >= bb.maxSize
	bb.mu.Unlock()

	if shouldFlush {
		select {
		case bb.flushCh <- struct{}{}:
		default:
		}
	}
}

func (bb *BatchBuffer) timerLoop() {
	ticker := time.NewTicker(bb.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-bb.ctx.Done():
			bb.flush()
			return
		case <-ticker.C:
			bb.flush()
		case <-bb.flushCh:
			bb.flush()
		}
	}
}

func (bb *BatchBuffer) flush() {
	bb.mu.Lock()
	if len(bb.records) == 0 {
		bb.mu.Unlock()
		return
	}
	batch := bb.records
	bb.records = make([]*model.UpsertRecord, 0, bb.maxSize)
	bb.lastFlush = time.Now()
	bb.mu.Unlock()

	// Group by connection + schema + table
	byTable := make(map[string][]*model.UpsertRecord)
	for _, r := range batch {
		byTable[bb.groupKey(r)] = append(byTable[bb.groupKey(r)], r)
	}

	for groupKey, records := range byTable {
		if err := bb.batchUpsert(records); err != nil {
			bb.logger.Error("batch upsert failed",
				zap.String("group", groupKey),
				zap.Int("count", len(records)),
				zap.Error(err),
			)
		} else {
			bb.logger.Info("batch upsert ok",
				zap.String("group", groupKey),
				zap.Int("count", len(records)),
			)
		}
	}
}

func (bb *BatchBuffer) batchUpsert(records []*model.UpsertRecord) error {
	if len(records) == 0 {
		return nil
	}
	first := records[0]
	tableName := first.TableName
	schemaName := bb.recordSchema(first)
	db := bb.resolveDB(first)
	schemaAdapter := bb.resolveSchemaAdapter(first, db)

	// Prepare table once via SchemaAdapter (CDC columns, NOT NULL, UNIQUE — all dynamic)
	pk := first.PrimaryKeyField
	if err := schemaAdapter.PrepareForCDCInsertInSchema(schemaName, tableName, pk); err != nil {
		bb.logger.Error("prepare table failed",
			zap.String("schema", schemaName),
			zap.String("table", tableName),
			zap.Error(err),
		)
		return err
	}

	schema := schemaAdapter.GetSchemaInSchema(schemaName, tableName)
	if schema == nil {
		return fmt.Errorf("schema not found for %s.%s", schemaName, tableName)
	}

	// Path B Hardened remap: shadow tables emitted by ShadowAutomator carry
	// both `id BIGINT` (sonyflake-generated, internal stable) and
	// `source_id VARCHAR(200) UNIQUE` (external anchor for source PK).
	// event_handler converts Mongo `_id` → `id`; when shadow exposes the
	// `source_id` anchor, route the source PK there instead so the BIGINT
	// `id` slot stays free for the BEFORE INSERT sonyflake trigger.
	effectivePK := first.PrimaryKeyField
	if effectivePK == "id" {
		if _, hasSourceID := schema.Columns["source_id"]; hasSourceID {
			effectivePK = "source_id"
		}
	}

	for _, r := range records {
		query, values := schemaAdapter.BuildUpsertSQLInSchema(
			schema, bb.recordSchema(r), r.TableName, effectivePK,
			r.PrimaryKeyValue, r.MappedData,
			r.RawData, r.Source, r.Hash, r.SourceTsMs,
		)
		if err := db.Exec(query, values...).Error; err != nil {
			bb.logger.Error("upsert failed",
				zap.String("schema", bb.recordSchema(r)),
				zap.String("table", tableName),
				zap.String("pk", r.PrimaryKeyValue),
				zap.Error(err),
			)
			// Persist only sanitized payloads into failed_sync_logs.
			bb.db.Create(bb.buildFailedSyncLog(tableName, r, err))
			metrics.SyncFailed.WithLabelValues(tableName, "upsert", r.Source).Inc()
		} else {
			metrics.SyncSuccess.WithLabelValues(tableName, "upsert", r.Source).Inc()
		}
	}
	return nil
}

func (bb *BatchBuffer) groupKey(record *model.UpsertRecord) string {
	return fmt.Sprintf("%s|%s|%s|%s",
		strings.TrimSpace(record.ConnectionRole),
		strings.TrimSpace(record.ConnectionKey),
		bb.recordSchema(record),
		strings.TrimSpace(record.TableName),
	)
}

func (bb *BatchBuffer) recordSchema(record *model.UpsertRecord) string {
	if record == nil || strings.TrimSpace(record.SchemaName) == "" {
		return "public"
	}
	return strings.TrimSpace(record.SchemaName)
}

func (bb *BatchBuffer) resolveDB(record *model.UpsertRecord) *gorm.DB {
	if bb.connMgr == nil || record == nil {
		return bb.db
	}
	role := strings.TrimSpace(record.ConnectionRole)
	key := strings.TrimSpace(record.ConnectionKey)
	if role == "shadow" && key != "" {
		if db, err := bb.connMgr.GetShadowDB(context.Background(), key); err == nil {
			return db
		}
	}
	if role == "master" && key != "" {
		if db, err := bb.connMgr.GetMasterDB(context.Background(), key); err == nil {
			return db
		}
	}
	return bb.db
}

func (bb *BatchBuffer) resolveSchemaAdapter(record *model.UpsertRecord, db *gorm.DB) *service.SchemaAdapter {
	if db == nil {
		return bb.schemaAdapter
	}
	cacheKey := "legacy"
	if record != nil {
		cacheKey = strings.TrimSpace(record.ConnectionRole) + ":" + strings.TrimSpace(record.ConnectionKey)
		if cacheKey == ":" || cacheKey == "" {
			cacheKey = "legacy"
		}
	}
	if cached, ok := bb.adapters.Load(cacheKey); ok {
		return cached.(*service.SchemaAdapter)
	}
	adapter := service.NewSchemaAdapter(db, bb.logger)
	bb.adapters.Store(cacheKey, adapter)
	return adapter
}

func (bb *BatchBuffer) buildFailedSyncLog(tableName string, record *model.UpsertRecord, err error) *model.FailedSyncLog {
	rawJSON := bb.sanitizeRawData(tableName, record.RawData)
	return &model.FailedSyncLog{
		TargetTable:  tableName,
		RecordID:     record.PrimaryKeyValue,
		Operation:    "upsert",
		RawJSON:      rawJSON,
		ErrorMessage: err.Error(),
		ErrorType:    classifyError(err),
		Status:       "failed",
	}
}

func (bb *BatchBuffer) sanitizeRawData(tableName, raw string) json.RawMessage {
	if bb.masking != nil {
		return bb.masking.MaskJSONPayload(tableName, []byte(raw))
	}
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw)
	}
	wrapped, _ := json.Marshal(map[string]string{"raw": raw})
	return json.RawMessage(wrapped)
}

func classifyError(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "SQLSTATE 42703"): // column does not exist
		return "schema_mismatch"
	case strings.Contains(msg, "SQLSTATE 22P02"): // invalid input syntax
		return "type_error"
	case strings.Contains(msg, "SQLSTATE 23502"): // not null violation
		return "not_null"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	default:
		return "unknown"
	}
}

func (bb *BatchBuffer) GetStatus() BatchStatus {
	bb.mu.Lock()
	defer bb.mu.Unlock()
	return BatchStatus{
		BufferSize: len(bb.records),
		MaxSize:    bb.maxSize,
		LastFlush:  bb.lastFlush,
	}
}

func (bb *BatchBuffer) Stop() {
	bb.cancel()
}
