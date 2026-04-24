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

	// Group by table
	byTable := make(map[string][]*model.UpsertRecord)
	for _, r := range batch {
		byTable[r.TableName] = append(byTable[r.TableName], r)
	}

	for tableName, records := range byTable {
		if err := bb.batchUpsert(tableName, records); err != nil {
			bb.logger.Error("batch upsert failed",
				zap.String("table", tableName),
				zap.Int("count", len(records)),
				zap.Error(err),
			)
		} else {
			bb.logger.Info("batch upsert ok",
				zap.String("table", tableName),
				zap.Int("count", len(records)),
			)
		}
	}
}

func (bb *BatchBuffer) batchUpsert(tableName string, records []*model.UpsertRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Prepare table once via SchemaAdapter (CDC columns, NOT NULL, UNIQUE — all dynamic)
	pk := records[0].PrimaryKeyField
	if err := bb.schemaAdapter.PrepareForCDCInsert(tableName, pk); err != nil {
		bb.logger.Error("prepare table failed", zap.String("table", tableName), zap.Error(err))
		return err
	}

	schema := bb.schemaAdapter.GetSchema(tableName)
	if schema == nil {
		return fmt.Errorf("schema not found for %s", tableName)
	}

	for _, r := range records {
		query, values := bb.schemaAdapter.BuildUpsertSQL(
			schema, tableName, r.PrimaryKeyField,
			r.PrimaryKeyValue, r.MappedData,
			r.RawData, r.Source, r.Hash, r.SourceTsMs,
		)
		if err := bb.db.Exec(query, values...).Error; err != nil {
			bb.logger.Error("upsert failed",
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
