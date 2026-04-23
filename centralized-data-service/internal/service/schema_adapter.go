package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ColumnInfo holds metadata for a single column
type ColumnInfo struct {
	Name       string
	DataType   string
	IsNullable bool
}

// TableSchema holds cached schema info for a target table
type TableSchema struct {
	Columns   map[string]ColumnInfo
	PKColumn  string
	HasUnique bool
	Prepared  bool // CDC columns added, NOT NULL dropped, UNIQUE ensured
}

// SchemaAdapter reads target table schemas dynamically and prepares for CDC inserts
type SchemaAdapter struct {
	db     *gorm.DB
	cache  sync.Map // table name → *TableSchema
	logger *zap.Logger
}

func NewSchemaAdapter(db *gorm.DB, logger *zap.Logger) *SchemaAdapter {
	return &SchemaAdapter{db: db, logger: logger}
}

// GetSchema returns cached schema for a table, loading from DB if needed
func (sa *SchemaAdapter) GetSchema(tableName string) *TableSchema {
	if cached, ok := sa.cache.Load(tableName); ok {
		return cached.(*TableSchema)
	}

	schema := sa.loadSchema(tableName)
	if schema != nil {
		sa.cache.Store(tableName, schema)
	}
	return schema
}

// InvalidateCache removes cached schema (call on schema.config.reload)
func (sa *SchemaAdapter) InvalidateCache(tableName string) {
	sa.cache.Delete(tableName)
}

func (sa *SchemaAdapter) loadSchema(tableName string) *TableSchema {
	var rows []struct {
		ColumnName string
		DataType   string
		IsNullable string
	}
	sa.db.Raw(`SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = 'public'
		ORDER BY ordinal_position`, tableName).Scan(&rows)

	if len(rows) == 0 {
		return nil
	}

	schema := &TableSchema{
		Columns: make(map[string]ColumnInfo, len(rows)),
	}
	for _, r := range rows {
		schema.Columns[r.ColumnName] = ColumnInfo{
			Name:       r.ColumnName,
			DataType:   r.DataType,
			IsNullable: r.IsNullable == "YES",
		}
	}
	return schema
}

// PrepareForCDCInsert makes target table ready for CDC upserts:
// 1. Add CDC columns if missing
// 3. Add UNIQUE constraint on PK if missing
func (sa *SchemaAdapter) PrepareForCDCInsert(tableName, pkColumn string) error {
	schema := sa.GetSchema(tableName)
	if schema == nil {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if schema.Prepared {
		return nil
	}

	// 1. Add CDC columns if missing
	cdcCols := map[string]string{
		"_raw_data":   "JSONB",
		"_source":     "VARCHAR(20) DEFAULT 'airbyte'",
		"_synced_at":  "TIMESTAMP DEFAULT NOW()",
		"_version":    "BIGINT DEFAULT 1",
		"_hash":       "VARCHAR(64)",
		"_deleted":    "BOOLEAN DEFAULT FALSE",
		"_created_at": "TIMESTAMP DEFAULT NOW()",
		"_updated_at": "TIMESTAMP DEFAULT NOW()",
	}
	for col, def := range cdcCols {
		if _, exists := schema.Columns[col]; !exists {
			sa.db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" %s`, tableName, col, def))
		}
	}

	for colName, info := range schema.Columns {
		if strings.HasPrefix(colName, "_airbyte_") && !info.IsNullable {
			sa.db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL`, tableName, colName))
		}
	}

	// 3. Add UNIQUE on PK if missing
	var hasUnique bool
	sa.db.Raw(`SELECT EXISTS(
		SELECT 1 FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
		WHERE c.conrelid = ?::regclass AND c.contype IN ('u','p') AND a.attname = ?
	)`, fmt.Sprintf(`"%s"`, tableName), pkColumn).Scan(&hasUnique)

	if !hasUnique {
		constraintName := fmt.Sprintf("%s_%s_cdc_unique", tableName, pkColumn)
		if err := sa.db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ADD CONSTRAINT "%s" UNIQUE ("%s")`,
			tableName, constraintName, pkColumn)).Error; err != nil {
			sa.logger.Warn("add UNIQUE constraint failed (may already exist)", zap.Error(err))
		}
	}

	schema.PKColumn = pkColumn
	schema.HasUnique = true
	schema.Prepared = true

	// Reload schema after modifications
	newSchema := sa.loadSchema(tableName)
	if newSchema != nil {
		newSchema.PKColumn = pkColumn
		newSchema.HasUnique = true
		newSchema.Prepared = true
		sa.cache.Store(tableName, newSchema)
	}

	sa.logger.Info("table prepared for CDC insert", zap.String("table", tableName), zap.String("pk", pkColumn))
	return nil
}

// IsJSONB returns true if column is jsonb/json type
func (sa *SchemaAdapter) IsJSONB(schema *TableSchema, colName string) bool {
	if info, ok := schema.Columns[colName]; ok {
		return info.DataType == "jsonb" || info.DataType == "json"
	}
	return false
}

// CoerceValue converts a Go value to proper Postgres type based on column schema
func (sa *SchemaAdapter) CoerceValue(schema *TableSchema, colName string, val interface{}) interface{} {
	if val == nil {
		return nil
	}

	colType := ""
	if col, ok := schema.Columns[colName]; ok {
		colType = col.DataType
	}

	// For varchar/text columns, convert non-string values to string
	if colType == "character varying" || colType == "text" || colType == "varchar" {
		switch val.(type) {
		case string:
			// already string, pass through
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	if !sa.IsJSONB(schema, colName) {
		return val
	}

	// JSONB column — ensure value is valid JSON
	switch v := val.(type) {
	case string:
		// Try base64 decode (Avro bytes → base64 for JSON fields)
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil && json.Valid(decoded) {
			return string(decoded)
		}
		if json.Valid([]byte(v)) {
			return v
		}
		// Wrap as JSON string
		jsonVal, _ := json.Marshal(v)
		return string(jsonVal)
	case map[string]interface{}, []interface{}:
		jsonVal, _ := json.Marshal(v)
		return string(jsonVal)
	default:
		jsonVal, _ := json.Marshal(v)
		return string(jsonVal)
	}
}

// BuildUpsertSQL constructs INSERT ON CONFLICT with proper quoting + type coercion.
//
// sourceTsMs: Debezium payload.source.ts_ms (milliseconds since epoch).
// Pass 0 if unknown (legacy bridge, retry path) — the OCC guard on
// _source_ts is then skipped so the row is upserted unconditionally.
func (sa *SchemaAdapter) BuildUpsertSQL(schema *TableSchema, tableName string, pkField string,
	pkValue interface{}, mappedData map[string]interface{}, rawData, source, hash string,
	sourceTsMs int64) (string, []interface{}) {

	hasSourceTs := false
	if _, ok := schema.Columns["_source_ts"]; ok {
		hasSourceTs = true
	}

	allCols := []string{fmt.Sprintf(`"%s"`, pkField)}
	allPlaceholders := []string{"?"}
	finalValues := []interface{}{pkValue}

	for col, val := range mappedData {
		if col == pkField {
			continue
		}
		if _, exists := schema.Columns[col]; !exists {
			continue
		}
		allCols = append(allCols, fmt.Sprintf(`"%s"`, col))
		allPlaceholders = append(allPlaceholders, "?")
		finalValues = append(finalValues, sa.CoerceValue(schema, col, val))
	}

	// CDC metadata columns (only emit if the column exists on the target)
	if _, ok := schema.Columns["_raw_data"]; ok {
		allCols = append(allCols, `"_raw_data"`)
		allPlaceholders = append(allPlaceholders, "?")
		finalValues = append(finalValues, rawData)
	}
	if _, ok := schema.Columns["_source"]; ok {
		allCols = append(allCols, `"_source"`)
		allPlaceholders = append(allPlaceholders, "?")
		finalValues = append(finalValues, source)
	}
	if _, ok := schema.Columns["_synced_at"]; ok {
		allCols = append(allCols, `"_synced_at"`)
		allPlaceholders = append(allPlaceholders, "NOW()")
	}
	if _, ok := schema.Columns["_version"]; ok {
		allCols = append(allCols, `"_version"`)
		allPlaceholders = append(allPlaceholders, "1")
	}
	if _, ok := schema.Columns["_hash"]; ok {
		allCols = append(allCols, `"_hash"`)
		allPlaceholders = append(allPlaceholders, "?")
		finalValues = append(finalValues, hash)
	}
	if hasSourceTs {
		allCols = append(allCols, `"_source_ts"`)
		if sourceTsMs > 0 {
			allPlaceholders = append(allPlaceholders, "?")
			finalValues = append(finalValues, sourceTsMs)
		} else {
			// Unknown ts (bridge / retry). Let DB keep NULL.
			allPlaceholders = append(allPlaceholders, "NULL")
		}
	}

	// ON CONFLICT UPDATE clause
	updateSets := []string{}
	for col := range mappedData {
		if col == pkField {
			continue
		}
		if _, exists := schema.Columns[col]; !exists {
			continue
		}
		updateSets = append(updateSets, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, col, col))
	}
	if _, ok := schema.Columns["_raw_data"]; ok {
		updateSets = append(updateSets, `"_raw_data" = EXCLUDED."_raw_data"`)
	}
	if _, ok := schema.Columns["_synced_at"]; ok {
		updateSets = append(updateSets, `"_synced_at" = NOW()`)
	}
	if _, ok := schema.Columns["_version"]; ok {
		updateSets = append(updateSets, fmt.Sprintf(`"_version" = "%s"."_version" + 1`, tableName))
	}
	if _, ok := schema.Columns["_hash"]; ok {
		updateSets = append(updateSets, `"_hash" = EXCLUDED."_hash"`)
	}
	if _, ok := schema.Columns["_updated_at"]; ok {
		updateSets = append(updateSets, `"_updated_at" = NOW()`)
	}
	if hasSourceTs {
		updateSets = append(updateSets, `"_source_ts" = EXCLUDED."_source_ts"`)
	}

	// OCC guard: plan v3 §6 — UPDATE only when the stored _source_ts
	// is strictly older than the incoming one, OR the row has never
	// add a hash guard in this branch — under ts-based OCC, a row
	// arriving with a newer ts always wins even if the business hash
	// happens to match, so `_source_ts` gets refreshed.
	//
	// When the source ts is unknown (0) — bridge / retry path — we
	// fall back to hash-based dedup to preserve legacy semantics.
	var whereClause string
	if hasSourceTs && sourceTsMs > 0 {
		whereClause = fmt.Sprintf(
			`WHERE "%s"."_source_ts" IS NULL OR "%s"."_source_ts" < EXCLUDED."_source_ts"`,
			tableName, tableName,
		)
	} else {
		whereClause = fmt.Sprintf(`WHERE "%s"."_hash" IS DISTINCT FROM EXCLUDED."_hash"`, tableName)
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s) ON CONFLICT ("%s") DO UPDATE SET %s %s`,
		tableName,
		strings.Join(allCols, ", "),
		strings.Join(allPlaceholders, ", "),
		pkField,
		strings.Join(updateSets, ", "),
		whereClause,
	)

	return query, finalValues
}
