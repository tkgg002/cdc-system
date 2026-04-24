package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"centralized-data-service/internal/model"

	"go.uber.org/zap"
)

// MappedData represents the result of mapping a CDC event through rules.
type MappedData struct {
	Columns      map[string]interface{} // Direct column mappings (field → typed value)
	EnrichedData map[string]interface{} // Fields needing enrichment processing (Phase 2+)
	RawJSON      []byte                 // Masked raw JSON persisted into _raw_data
}

// DynamicMapper handles config-driven field mapping from CDC events to PostgreSQL columns.
// Uses RegistryService for cached mapping rules (no duplicate cache).
type DynamicMapper struct {
	registry      *RegistryService
	schemaAdapter *SchemaAdapter
	masking       *MaskingService
	logger        *zap.Logger
}

func NewDynamicMapper(registry *RegistryService, logger *zap.Logger, adapters ...*SchemaAdapter) *DynamicMapper {
	dm := &DynamicMapper{
		registry: registry,
		logger:   logger,
	}
	if len(adapters) > 0 {
		dm.schemaAdapter = adapters[0]
	}
	return dm
}

func (dm *DynamicMapper) SetSchemaAdapter(schemaAdapter *SchemaAdapter) {
	dm.schemaAdapter = schemaAdapter
}

func (dm *DynamicMapper) SetMaskingService(masking *MaskingService) {
	dm.masking = masking
}

// LoadRules delegates to RegistryService.ReloadAll (single source of truth for cache)
func (dm *DynamicMapper) LoadRules(ctx context.Context) error {
	return dm.registry.ReloadAll(ctx)
}

// GetRulesForTable returns cached mapping rules for a target table
func (dm *DynamicMapper) GetRulesForTable(targetTable string) []model.MappingRule {
	return dm.registry.GetMappingRules(targetTable)
}

// MapData applies mapping rules to transform raw CDC event data into structured columns.
// Returns mapped typed columns plus masked raw JSON for _raw_data persistence.
func (dm *DynamicMapper) MapData(ctx context.Context, targetTable string, rawData map[string]interface{}) (*MappedData, error) {
	rules := dm.registry.GetMappingRules(targetTable)
	if len(rules) == 0 {
		// No rules — store as raw only
		rawJSON, _ := json.Marshal(dm.maskRawData(targetTable, rawData))
		return &MappedData{
			Columns:      make(map[string]interface{}),
			EnrichedData: make(map[string]interface{}),
			RawJSON:      rawJSON,
		}, nil
	}

	columns := make(map[string]interface{})
	enriched := make(map[string]interface{})

	for _, rule := range rules {
		if !rule.IsActive {
			continue
		}

		val, exists := rawData[rule.SourceField]
		if !exists {
			// Check nested field (e.g., "info.fee")
			val = getNestedField(rawData, rule.SourceField)
			if val == nil {
				continue
			}
		}

		// Unwrap MongoDB special types: {"$oid":"..."} → string, {"$date":epoch} → timestamp
		val = unwrapMongoTypes(val)

		if rule.IsEnriched {
			enriched[rule.TargetColumn] = val
			continue
		}

		// Convert type
		converted, err := convertType(val, rule.DataType)
		if err != nil {
			dm.logger.Debug("type conversion failed, using raw value",
				zap.String("field", rule.SourceField),
				zap.String("target_type", rule.DataType),
				zap.Error(err),
			)
			columns[rule.TargetColumn] = val
			continue
		}
		columns[rule.TargetColumn] = converted
	}

	rawJSON, _ := json.Marshal(dm.maskRawData(targetTable, rawData))

	return &MappedData{
		Columns:      columns,
		EnrichedData: enriched,
		RawJSON:      rawJSON,
	}, nil
}

func (dm *DynamicMapper) maskRawData(targetTable string, rawData map[string]interface{}) map[string]interface{} {
	if dm.masking == nil {
		return cloneAnyMap(rawData)
	}
	return dm.masking.MaskTableData(targetTable, rawData)
}

// BuildUpsertQuery delegates SQL generation to SchemaAdapter so DynamicMapper
// never drifts from the shared OCC upsert semantics.
func (dm *DynamicMapper) BuildUpsertQuery(targetTable, pkField string, mappedData *MappedData) (string, []interface{}) {
	if dm.schemaAdapter == nil {
		if dm.logger != nil {
			dm.logger.Warn("BuildUpsertQuery called without schema adapter",
				zap.String("table", targetTable),
			)
		}
		return "", nil
	}

	schema := dm.schemaAdapter.GetSchema(targetTable)
	if schema == nil {
		if dm.logger != nil {
			dm.logger.Warn("BuildUpsertQuery schema cache miss",
				zap.String("table", targetTable),
			)
		}
		return "", nil
	}

	return dm.schemaAdapter.BuildUpsertSQL(
		schema,
		targetTable,
		pkField,
		nil,
		mappedData.Columns,
		string(mappedData.RawJSON),
		"dynamic-mapper",
		"",
		0,
	)
}

// convertType converts raw value to PostgreSQL-compatible typed value
func convertType(val interface{}, dataType string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	dt := strings.ToUpper(dataType)

	switch {
	case strings.Contains(dt, "INT") || strings.Contains(dt, "BIGINT") || strings.Contains(dt, "SMALLINT"):
		return toInt64(val)
	case strings.Contains(dt, "NUMERIC") || strings.Contains(dt, "DECIMAL") || strings.Contains(dt, "FLOAT") || strings.Contains(dt, "DOUBLE"):
		return toFloat64(val)
	case strings.Contains(dt, "BOOL"):
		return toBool(val)
	case strings.Contains(dt, "TIMESTAMP") || strings.Contains(dt, "DATE"):
		return toTimestamp(val)
	case strings.Contains(dt, "JSONB") || strings.Contains(dt, "JSON"):
		return toJSON(val)
	default: // TEXT, VARCHAR, etc.
		return fmt.Sprintf("%v", val), nil
	}
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case json.Number:
		return v.Int64()
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", val)
	}
}

func toFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func toBool(val interface{}) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	case float64:
		return v != 0, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}

func toTimestamp(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case string:
		// Try common formats
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, v); err == nil {
				return t, nil
			}
		}
		// MongoDB ISODate format with $date
		return v, nil // Let Postgres handle parsing
	case float64:
		// Unix timestamp (seconds or milliseconds)
		if v > 1e12 {
			return time.UnixMilli(int64(v)), nil
		}
		return time.Unix(int64(v), 0), nil
	case map[string]interface{}:
		// MongoDB {"$date": "..."} format
		if dateStr, ok := v["$date"].(string); ok {
			t, err := time.Parse(time.RFC3339, dateStr)
			if err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("cannot parse date from map")
	default:
		return val, nil
	}
}

func toJSON(val interface{}) ([]byte, error) {
	return json.Marshal(val)
}

// unwrapMongoTypes converts MongoDB extended JSON types to Go native types
// {"$oid": "abc123"} → "abc123"
// {"$date": 1770102689256} → time.Time
// {"$date": "2026-01-01T00:00:00Z"} → time.Time
func unwrapMongoTypes(val interface{}) interface{} {
	m, ok := val.(map[string]interface{})
	if !ok {
		return val
	}
	if oid, ok := m["$oid"]; ok {
		return fmt.Sprintf("%v", oid)
	}
	if dateVal, ok := m["$date"]; ok {
		switch d := dateVal.(type) {
		case float64:
			if d > 1e12 {
				return time.UnixMilli(int64(d))
			}
			return time.Unix(int64(d), 0)
		case string:
			if t, err := time.Parse(time.RFC3339, d); err == nil {
				return t
			}
			return d
		case json.Number:
			if ms, err := d.Int64(); err == nil {
				return time.UnixMilli(ms)
			}
		}
		return dateVal
	}
	return val
}

// getNestedField extracts nested field value using dot notation (e.g., "info.fee")
func getNestedField(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}
	return current
}
