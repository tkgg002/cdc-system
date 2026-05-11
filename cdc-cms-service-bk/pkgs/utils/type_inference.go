package utils

import "time"

// InferDataType infers a PostgreSQL type from a Go interface{} value
func InferDataType(value interface{}) string {
	if value == nil {
		return "TEXT"
	}

	switch v := value.(type) {
	case bool:
		return "BOOLEAN"

	case float64:
		if v == float64(int64(v)) {
			if v >= -2147483648 && v <= 2147483647 {
				return "INTEGER"
			}
			return "BIGINT"
		}
		return "DECIMAL(18,6)"

	case string:
		if _, err := time.Parse(time.RFC3339, v); err == nil {
			return "TIMESTAMP"
		}
		if _, err := time.Parse("2006-01-02T15:04:05Z", v); err == nil {
			return "TIMESTAMP"
		}
		if _, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return "TIMESTAMP"
		}
		if len(v) <= 100 {
			return "VARCHAR(100)"
		} else if len(v) <= 255 {
			return "VARCHAR(255)"
		}
		return "TEXT"

	case map[string]interface{}:
		return "JSONB"

	case []interface{}:
		return "JSONB"

	default:
		return "TEXT"
	}
}
