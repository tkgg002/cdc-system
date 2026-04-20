package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/metrics"
	"centralized-data-service/pkgs/utils"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type EventHandler struct {
	db              *gorm.DB
	registrySvc     *service.RegistryService
	dynamicMapper   *service.DynamicMapper
	schemaInspector *service.SchemaInspector
	batchBuffer     *BatchBuffer
	logger          *zap.Logger
}

func NewEventHandler(
	db *gorm.DB,
	registrySvc *service.RegistryService,
	mapper *service.DynamicMapper,
	inspector *service.SchemaInspector,
	buffer *BatchBuffer,
	logger *zap.Logger,
) *EventHandler {
	return &EventHandler{
		db:              db,
		registrySvc:     registrySvc,
		dynamicMapper:   mapper,
		schemaInspector: inspector,
		batchBuffer:     buffer,
		logger:          logger,
	}
}

// HandleRaw processes a CDC event from any transport (NATS, Kafka, etc.)
func (h *EventHandler) HandleRaw(ctx context.Context, subject string, data []byte) error {
	start := time.Now()

	var event model.CDCEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("parse CDC event: %w", err)
	}

	sourceStr := fmt.Sprintf("%v", event.Source)
	sourceDB, sourceTable := extractSourceAndTable(subject, sourceStr)
	return h.processEvent(ctx, start, &event, subject, sourceDB, sourceTable)
}

func (h *EventHandler) Handle(ctx context.Context, msg *nats.Msg) error {
	return h.HandleRaw(ctx, msg.Subject, msg.Data)
}

func (h *EventHandler) processEvent(ctx context.Context, start time.Time, event *model.CDCEvent, subject, sourceDB, sourceTable string) error {

	tableConfig := h.registrySvc.GetTableConfigBySource(sourceTable)
	if tableConfig == nil {
		h.logger.Debug("table not in registry, skipping", zap.String("source_table", sourceTable))
		return nil
	}
	targetTable := tableConfig.TargetTable

	if event.Data.Op == "d" {
		return h.handleDelete(ctx, event, targetTable, tableConfig)
	}

	data := event.Data.After
	if data == nil {
		return fmt.Errorf("no 'after' data in event for table %s", sourceTable)
	}

	// Schema Inspection (non-blocking) — use targetTable for DB schema lookup
	if _, err := h.schemaInspector.InspectEvent(ctx, targetTable, sourceDB, data); err != nil {
		h.logger.Error("schema inspection failed", zap.Error(err))
	}

	// Extract primary key
	pkField := tableConfig.PrimaryKeyField
	pkValue := extractPrimaryKey(data, pkField, tableConfig.SourceType)

	// Dynamic Mapper — map raw data to typed columns + raw JSON
	mapped, err := h.dynamicMapper.MapData(ctx, targetTable, data)
	if err != nil {
		return fmt.Errorf("dynamic mapper: %w", err)
	}

	hash := utils.CalculateHash(data)

	// Use PK field as-is from registry (registry stores correct column name for target table)
	pgPKField := pkField

	record := &model.UpsertRecord{
		TableName:       targetTable,
		PrimaryKeyField: pgPKField,
		PrimaryKeyValue: pkValue,
		MappedData:      mapped.Columns,
		RawData:         string(mapped.RawJSON),
		Source:          "debezium",
		Hash:            hash,
		SourceTsMs:      event.Data.SourceTsMs,
	}
	h.batchBuffer.Add(record)

	// Metrics
	duration := time.Since(start)
	metrics.EventsProcessed.WithLabelValues(event.Data.Op, sourceDB, sourceTable, "success").Inc()
	metrics.ProcessingDuration.WithLabelValues(event.Data.Op, sourceDB, sourceTable).Observe(duration.Seconds())

	h.logger.Debug("event processed",
		zap.String("source_table", sourceTable),
		zap.String("target_table", targetTable),
		zap.String("pk", pkValue),
		zap.Duration("duration", duration),
	)

	return nil
}

func (h *EventHandler) handleDelete(ctx context.Context, event *model.CDCEvent, tableName string, config *model.TableRegistry) error {
	before := event.Data.Before
	if before == nil {
		return fmt.Errorf("no 'before' data in delete event")
	}

	pkField := config.PrimaryKeyField
	pkValue := extractPrimaryKey(before, pkField, config.SourceType)
	pgPKField := pkField
	if pkField == "_id" {
		pgPKField = "id"
	}

	sql := fmt.Sprintf(`UPDATE "%s" SET _deleted = TRUE, _updated_at = NOW() WHERE %s = ?`, tableName, pgPKField)
	return h.db.WithContext(ctx).Exec(sql, pkValue).Error
}

func extractSourceAndTable(subject, source string) (string, string) {
	// subject format: cdc.goopay.{source_db}.{table_name}
	parts := strings.Split(subject, ".")
	if len(parts) >= 4 {
		return parts[2], parts[3]
	}
	// Fallback: parse from event source
	sourceParts := strings.Split(source, "/")
	if len(sourceParts) >= 2 {
		return sourceParts[len(sourceParts)-2], sourceParts[len(sourceParts)-1]
	}
	return "unknown", "unknown"
}

func extractPrimaryKey(data map[string]interface{}, pkField, sourceType string) string {
	// MongoDB: _id can be ObjectId object {"$oid": "..."}
	if sourceType == "mongodb" && pkField == "_id" {
		if idMap, ok := data["_id"].(map[string]interface{}); ok {
			if oid, ok := idMap["$oid"].(string); ok {
				return oid
			}
		}
	}

	if val, ok := data[pkField]; ok {
		switch v := val.(type) {
		case string:
			return v
		case float64:
			return fmt.Sprintf("%.0f", v)
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}
