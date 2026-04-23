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
	RawJSON      []byte                 // Full raw JSON for _raw_data column
}

// DynamicMapper handles config-driven field mapping from CDC events to PostgreSQL columns.
// Uses RegistryService for cached mapping rules (no duplicate cache).
type DynamicMapper struct {
	registry *RegistryService
	logger   *zap.Logger
}

func NewDynamicMapper(registry *RegistryService, logger *zap.Logger) *DynamicMapper {
	return &DynamicMapper{
		registry: registry,
		logger:   logger,
	}
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
// Returns: mapped typed columns + raw JSON for _raw_data
func (dm *DynamicMapper) MapData(ctx context.Context, targetTable string, rawData map[string]interface{}) (*MappedData, error) {
	rules := dm.registry.GetMappingRules(targetTable)
	if len(rules) == 0 {
		// No rules — store as raw only
		rawJSON, _ := json.Marshal(rawData)
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

	rawJSON, _ := json.Marshal(rawData)

	return &MappedData{
		Columns:      columns,
		EnrichedData: enriched,
		RawJSON:      rawJSON,
	}, nil
}

// BuildUpsertQuery dynamically builds INSERT...ON CONFLICT query based on mapped data.
func (dm *DynamicMapper) BuildUpsertQuery(targetTable, pkField string, mappedData *MappedData) (string, []interface{}) {
	// Columns: pk + mapped + _raw_data + CDC metadata
	colNames := []string{pkField, "_raw_data", "_source", "_synced_at", "_hash", "_version", "_created_at", "_updated_at"}
	placeholders := []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"}
	args := []interface{}{
		nil, // $1 = pk value (set by caller)
		mappedData.RawJSON,
		"debezium",
		time.Now(),
		"", // hash (set by caller)
		1,
		time.Now(),
		time.Now(),
	}

	// Add mapped columns
	idx := 9
	updateClauses := []string{
		"_raw_data = EXCLUDED._raw_data",
		"_synced_at = EXCLUDED._synced_at",
		"_hash = EXCLUDED._hash",
		fmt.Sprintf("_version = \"%s\"._version + 1", targetTable),
		"_updated_at = NOW()",
	}

	for col, val := range mappedData.Columns {
		colNames = append(colNames, fmt.Sprintf(`"%s"`, col))
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, val)
		updateClauses = append(updateClauses, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
		idx++
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT ("%s") DO UPDATE SET %s WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
		targetTable,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
		pkField,
		strings.Join(updateClauses, ", "),
		targetTable,
	)

	return query, args
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
