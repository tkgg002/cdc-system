package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/internal/repository"
	"centralized-data-service/pkgs/metrics"
	"centralized-data-service/pkgs/natsconn"
	"centralized-data-service/pkgs/rediscache"
	"centralized-data-service/pkgs/utils"

	"go.uber.org/zap"
)

type SchemaInspector struct {
	pendingRepo *repository.PendingFieldRepo
	redisCache  *rediscache.RedisCache
	natsClient  *natsconn.NatsClient
	logger      *zap.Logger
}

type SchemaDrift struct {
	Detected  bool
	TableName string
	SourceDB  string
	NewFields []DetectedField
}

type DetectedField struct {
	FieldName     string      `json:"field_name"`
	SampleValue   interface{} `json:"sample_value"`
	SuggestedType string      `json:"suggested_type"`
}

func NewSchemaInspector(
	pendingRepo *repository.PendingFieldRepo,
	cache *rediscache.RedisCache,
	nats *natsconn.NatsClient,
	logger *zap.Logger,
) *SchemaInspector {
	return &SchemaInspector{
		pendingRepo: pendingRepo,
		redisCache:  cache,
		natsClient:  nats,
		logger:      logger,
	}
}

func (si *SchemaInspector) InspectEvent(ctx context.Context, tableName, sourceDB string, eventData map[string]interface{}) (*SchemaDrift, error) {
	eventFields := extractFieldNames(eventData)

	tableSchema, err := si.getTableSchema(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("get table schema: %w", err)
	}

	newFields := findNewFields(eventFields, tableSchema)
	if len(newFields) == 0 {
		return &SchemaDrift{Detected: false}, nil
	}

	si.logger.Info("schema drift detected",
		zap.String("source_db", sourceDB),
		zap.String("table", tableName),
		zap.Int("new_fields", len(newFields)),
	)

	var detectedFields []DetectedField
	for _, fieldName := range newFields {
		value := eventData[fieldName]
		suggestedType := utils.InferDataType(value)

		detectedFields = append(detectedFields, DetectedField{
			FieldName:     fieldName,
			SampleValue:   value,
			SuggestedType: suggestedType,
		})

		sampleJSON, _ := json.Marshal(value)
		if err := si.pendingRepo.UpsertPendingField(ctx, tableName, sourceDB, fieldName, string(sampleJSON), suggestedType); err != nil {
			si.logger.Error("failed to save pending field", zap.Error(err), zap.String("field", fieldName))
		}
	}

	si.publishDriftAlert(sourceDB, tableName, detectedFields)
	metrics.SchemaDriftDetected.WithLabelValues(sourceDB, tableName).Inc()

	return &SchemaDrift{
		Detected:  true,
		TableName: tableName,
		SourceDB:  sourceDB,
		NewFields: detectedFields,
	}, nil
}

func (si *SchemaInspector) getTableSchema(ctx context.Context, tableName string) (map[string]bool, error) {
	cacheKey := fmt.Sprintf("schema:%s", tableName)

	cached, err := si.redisCache.Get(ctx, cacheKey)
	if err == nil && cached != "" {
		var schema map[string]bool
		if err := json.Unmarshal([]byte(cached), &schema); err == nil {
			return schema, nil
		}
	}

	schema, err := si.pendingRepo.GetTableColumns(ctx, tableName)
	if err != nil {
		return nil, err
	}

	schemaJSON, _ := json.Marshal(schema)
	si.redisCache.Set(ctx, cacheKey, string(schemaJSON), 5*time.Minute)

	return schema, nil
}

func (si *SchemaInspector) publishDriftAlert(sourceDB, tableName string, fields []DetectedField) {
	alert := map[string]interface{}{
		"source_db":   sourceDB,
		"table":       tableName,
		"new_fields":  fields,
		"detected_at": time.Now().Format(time.RFC3339),
	}
	alertJSON, _ := json.Marshal(alert)

	if err := si.natsClient.Conn.Publish("schema.drift.detected", alertJSON); err != nil {
		si.logger.Error("failed to publish drift alert", zap.Error(err))
	}
}

func extractFieldNames(data map[string]interface{}) []string {
	fields := make([]string, 0, len(data))
	for key := range data {
		fields = append(fields, key)
	}
	return fields
}

func findNewFields(eventFields []string, tableSchema map[string]bool) []string {
	// Fields to skip (internal/CDC metadata)
	skip := map[string]bool{
		"_id": true, "_raw_data": true, "_source": true, "_synced_at": true,
		"_version": true, "_hash": true, "_deleted": true, "_created_at": true, "_updated_at": true,
	}

	var newFields []string
	for _, field := range eventFields {
		if skip[field] {
			continue
		}
		if !tableSchema[field] {
			newFields = append(newFields, field)
		}
	}
	return newFields
}
