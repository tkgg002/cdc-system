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
	connMgr         *service.ConnectionManager
	registrySvc     service.MetadataRegistry
	dynamicMapper   *service.DynamicMapper
	schemaInspector *service.SchemaInspector
	batchBuffer     *BatchBuffer
	logger          *zap.Logger
}

func NewEventHandler(
	db *gorm.DB,
	connMgr *service.ConnectionManager,
	registrySvc service.MetadataRegistry,
	mapper *service.DynamicMapper,
	inspector *service.SchemaInspector,
	buffer *BatchBuffer,
	logger *zap.Logger,
) *EventHandler {
	return &EventHandler{
		db:              db,
		connMgr:         connMgr,
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

	// B3 fan-out: resolve master + all logical-clone routes for this source event.
	routes := h.registrySvc.ResolveSourceRoutes(sourceDB, sourceTable)
	if len(routes) == 0 {
		h.logger.Debug("table not in registry, skipping", zap.String("source_table", sourceTable))
		return nil
	}

	if event.Data.Op == "d" {
		return h.handleDelete(ctx, event, routes)
	}

	data := event.Data.After
	if data == nil {
		return fmt.Errorf("no 'after' data in event for table %s", sourceTable)
	}

	// Schema Inspection (non-blocking) — use primary (master) route's target table.
	primaryRoute := routes[0]
	primaryTargetTable := primaryRoute.TableConfig.TargetTable
	if _, err := h.schemaInspector.InspectEvent(ctx, primaryTargetTable, sourceDB, data); err != nil {
		h.logger.Error("schema inspection failed", zap.Error(err))
	}

	hash := utils.CalculateHash(data)

	// Fan-out: enqueue one UpsertRecord per route (master + clones).
	for _, route := range routes {
		tableConfig := route.TableConfig
		targetTable := tableConfig.TargetTable

		// Extract primary key
		pkField := tableConfig.PrimaryKeyField
		pkValue := extractPrimaryKey(data, pkField, tableConfig.SourceType)

		// Dynamic Mapper — map raw data to typed columns + raw JSON
		mapped, err := h.dynamicMapper.MapData(ctx, targetTable, data)
		if err != nil {
			return fmt.Errorf("dynamic mapper (target=%s): %w", targetTable, err)
		}

		// Use PK field as-is from registry (registry stores correct column name for target table)
		pgPKField := pkField
		if pkField == "_id" {
			pgPKField = "id"
		}

		record := &model.UpsertRecord{
			TableName:        targetTable,
			SchemaName:       shadowSchemaName(route),
			ConnectionRole:   "shadow",
			ConnectionKey:    route.ShadowConnectionKey,
			PhysicalTableFQN: shadowPhysicalTable(route),
			PrimaryKeyField:  pgPKField,
			PrimaryKeyValue:  pkValue,
			MappedData:       mapped.Columns,
			RawData:          string(mapped.RawJSON),
			Source:           "debezium",
			Hash:             hash,
			SourceTsMs:       event.Data.SourceTsMs,
		}
		h.batchBuffer.Add(record)

		h.logger.Debug("event processed",
			zap.String("source_table", sourceTable),
			zap.String("source_db", sourceDB),
			zap.String("target_table", targetTable),
			zap.String("pk", pkValue),
		)
	}

	// Metrics (once per source event, not per route)
	duration := time.Since(start)
	metrics.EventsProcessed.WithLabelValues(event.Data.Op, sourceDB, sourceTable, "success").Inc()
	metrics.ProcessingDuration.WithLabelValues(event.Data.Op, sourceDB, sourceTable).Observe(duration.Seconds())

	return nil
}

func (h *EventHandler) handleDelete(ctx context.Context, event *model.CDCEvent, routes []*service.ResolvedSourceRoute) error {
	if len(routes) == 0 {
		return fmt.Errorf("missing routes for delete event")
	}
	before := event.Data.Before
	// A2 fix (P1.1/G3) — KHÔNG hard-fail nếu before==nil.
	// Per-route check pkValue dưới đây sẽ warn+skip route cụ thể nếu không trích được PK.

	// B3 fan-out: propagate DELETE to all shadow tables (master + clones).
	for _, route := range routes {
		if route == nil || route.TableConfig == nil {
			continue
		}
		config := route.TableConfig

		pkField := config.PrimaryKeyField
		var pkValue string
		if before != nil {
			pkValue = extractPrimaryKey(before, pkField, config.SourceType)
		}
		if pkValue == "" {
			h.logger.Warn("delete event missing PK, skipping tombstone for route",
				zap.String("target_table", config.TargetTable),
				zap.String("pk_field", pkField))
			continue
		}
		pgPKField := pkField
		if pkField == "_id" {
			pgPKField = "id"
		}

		db := h.db
		if h.connMgr != nil && strings.TrimSpace(route.ShadowConnectionKey) != "" {
			if shadowDB, err := h.connMgr.GetShadowDB(ctx, route.ShadowConnectionKey); err == nil {
				db = shadowDB
			}
		}

		// P1.1 (G3) — tombstone-first UPSERT. Handles delete events for rows
		// that may not yet exist in shadow (replay / first-touch delete).
		// _gpay_source_id stamped here mirrors B11 INSERT/UPDATE branch.
		sql := fmt.Sprintf(
			`INSERT INTO %s (%s, _gpay_source_id, _deleted, _created_at, _updated_at, _source)
     VALUES (?, ?::text, TRUE, NOW(), NOW(), 'debezium')
     ON CONFLICT (%s) DO UPDATE SET
        _deleted    = TRUE,
        _updated_at = NOW()`,
			qualifiedShadowTable(route),
			quoteEventIdent(pgPKField),
			quoteEventIdent(pgPKField),
		)
		if err := db.WithContext(ctx).Exec(sql, pkValue, pkValue).Error; err != nil {
			return fmt.Errorf("delete fan-out (target=%s): %w", config.TargetTable, err)
		}
	}
	return nil
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

func shadowSchemaName(route *service.ResolvedSourceRoute) string {
	if route == nil || route.ShadowBinding == nil {
		return "public"
	}
	if v := strings.TrimSpace(route.ShadowBinding.ShadowSchema); v != "" {
		return v
	}
	return "public"
}

func shadowPhysicalTable(route *service.ResolvedSourceRoute) string {
	if route == nil || route.ShadowBinding == nil {
		return ""
	}
	if v := strings.TrimSpace(route.ShadowBinding.PhysicalTableFQN); v != "" {
		return v
	}
	return qualifiedShadowTable(route)
}

func qualifiedShadowTable(route *service.ResolvedSourceRoute) string {
	tableName := ""
	if route != nil && route.TableConfig != nil {
		tableName = strings.TrimSpace(route.TableConfig.TargetTable)
	}
	if tableName == "" && route != nil && route.ShadowBinding != nil {
		tableName = strings.TrimSpace(route.ShadowBinding.ShadowTable)
	}
	return quoteEventIdent(shadowSchemaName(route)) + "." + quoteEventIdent(tableName)
}

func quoteEventIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}
