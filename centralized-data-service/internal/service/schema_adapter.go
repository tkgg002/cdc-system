package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ColumnInfo holds metadata for a single column
type ColumnInfo struct {
	Name       string
	DataType   string
	IsNullable bool
}

// BusinessColumn — Phase Auto Provisioning Feature A: a column inferred
// from the source object (PG/MariaDB introspection or Mongo sample doc)
// that the shadow table should mirror so downstream Discover finds
// non-empty mapping rules. DataType is a Postgres type literal already
// safe for embedding in DDL (mapper layer is responsible for sanitizing
// engine-specific types into PG equivalents).
type BusinessColumn struct {
	Name     string
	DataType string
	Nullable bool
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
	cache  sync.Map // schema.table → *TableSchema
	logger *zap.Logger
}

func NewSchemaAdapter(db *gorm.DB, logger *zap.Logger) *SchemaAdapter {
	return &SchemaAdapter{db: db, logger: logger}
}

// GetSchema returns cached schema for a table, loading from DB if needed
func (sa *SchemaAdapter) GetSchema(tableName string) *TableSchema {
	return sa.GetSchemaInSchema("public", tableName)
}

func (sa *SchemaAdapter) GetSchemaInSchema(schemaName, tableName string) *TableSchema {
	cacheKey := schemaCacheKey(schemaName, tableName)
	if cached, ok := sa.cache.Load(cacheKey); ok {
		return cached.(*TableSchema)
	}
	if cached, ok := sa.cache.Load(tableName); ok {
		return cached.(*TableSchema)
	}

	schema := sa.loadSchemaInSchema(schemaName, tableName)
	if schema != nil {
		sa.cache.Store(cacheKey, schema)
		sa.cache.Store(tableName, schema)
	}
	return schema
}

// InvalidateCache removes cached schema (call on schema.config.reload)
func (sa *SchemaAdapter) InvalidateCache(tableName string) {
	sa.InvalidateCacheInSchema("public", tableName)
}

func (sa *SchemaAdapter) InvalidateCacheInSchema(schemaName, tableName string) {
	sa.cache.Delete(schemaCacheKey(schemaName, tableName))
	sa.cache.Delete(tableName)
}

func (sa *SchemaAdapter) loadSchemaInSchema(schemaName, tableName string) *TableSchema {
	var rows []struct {
		ColumnName string
		DataType   string
		IsNullable string
	}
	sa.db.Raw(`SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = ?
		ORDER BY ordinal_position`, tableName, schemaName).Scan(&rows)

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
	return sa.PrepareForCDCInsertInSchema("public", tableName, pkColumn)
}

func (sa *SchemaAdapter) PrepareForCDCInsertInSchema(schemaName, tableName, pkColumn string) error {
	return sa.PrepareForCDCInsertWithBusinessCols(schemaName, tableName, pkColumn, nil)
}

// PrepareForCDCInsertWithBusinessCols — Phase Auto Provisioning
// Feature A. Same contract as PrepareForCDCInsertInSchema but lets the
// caller supply a business-column manifest inferred from the source
// object. When the shadow table is auto-created we inline those columns
// in the DDL; when it exists already we ALTER ADD COLUMN IF NOT EXISTS
// for each (idempotent, no destructive change). Pass nil to fall back
// to the legacy PK-only behaviour.
func (sa *SchemaAdapter) PrepareForCDCInsertWithBusinessCols(
	schemaName, tableName, pkColumn string, businessCols []BusinessColumn,
) error {
	schema := sa.GetSchemaInSchema(schemaName, tableName)
	if schema == nil {
		// Track D Hardening (P2 / Bug #6) — architect ruling:
		// auto-create the shadow table instead of failing. Idempotent
		// (CREATE TABLE IF NOT EXISTS) so a manual bootstrap that ran
		// earlier is preserved untouched.
		if err := sa.createShadowTableV1WithCols(schemaName, tableName, pkColumn, businessCols); err != nil {
			return fmt.Errorf("create shadow table %s.%s: %w", schemaName, tableName, err)
		}
		schema = sa.loadSchemaInSchema(schemaName, tableName)
		if schema == nil {
			return fmt.Errorf("shadow table %s.%s still missing after CREATE", schemaName, tableName)
		}
		sa.cache.Store(schemaCacheKey(schemaName, tableName), schema)
		sa.cache.Store(tableName, schema)
		sa.logger.Info("shadow table auto-created",
			zap.String("schema", schemaName),
			zap.String("table", tableName),
			zap.String("pk", pkColumn),
			zap.Int("business_cols", len(businessCols)))
	} else if len(businessCols) > 0 {
		// Existing table — additive ALTER for any business cols not yet
		// present. Type drift on an existing column is intentionally
		// NOT corrected here (fail-safe: avoid silent destructive ALTER
		// TYPE on production data).
		for _, bc := range businessCols {
			if bc.Name == "" || bc.Name == pkColumn {
				continue
			}
			if _, exists := schema.Columns[bc.Name]; exists {
				continue
			}
			nullClause := ""
			if !bc.Nullable {
				nullClause = " NULL" // store NULL — discover-side rules tighten
			}
			ident := pgx.Identifier{bc.Name}.Sanitize()
			ddl := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s%s`,
				quoteQualifiedTable(schemaName, tableName), ident, bc.DataType, nullClause)
			if err := sa.db.Exec(ddl).Error; err != nil {
				sa.logger.Warn("add business column failed",
					zap.String("schema", schemaName),
					zap.String("table", tableName),
					zap.String("col", bc.Name),
					zap.Error(err))
			}
		}
		// Reload so the cdcCols loop below sees the newly added cols.
		if reloaded := sa.loadSchemaInSchema(schemaName, tableName); reloaded != nil {
			schema = reloaded
			sa.cache.Store(schemaCacheKey(schemaName, tableName), schema)
			sa.cache.Store(tableName, schema)
		}
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
			sa.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS "%s" %s`, quoteQualifiedTable(schemaName, tableName), col, def))
		}
	}

	for colName, info := range schema.Columns {
		if strings.HasPrefix(colName, "_airbyte_") && !info.IsNullable {
			sa.db.Exec(fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL`, quoteQualifiedTable(schemaName, tableName), colName))
		}
	}

	// 3. Add UNIQUE on PK if missing
	var hasUnique bool
	sa.db.Raw(`SELECT EXISTS(
		SELECT 1 FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
		WHERE c.conrelid = ?::regclass AND c.contype IN ('u','p') AND a.attname = ?
	)`, quoteQualifiedTable(schemaName, tableName), pkColumn).Scan(&hasUnique)

	if !hasUnique {
		constraintName := fmt.Sprintf("%s_%s_cdc_unique", tableName, pkColumn)
		if err := sa.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT "%s" UNIQUE ("%s")`,
			quoteQualifiedTable(schemaName, tableName), constraintName, pkColumn)).Error; err != nil {
			sa.logger.Warn("add UNIQUE constraint failed (may already exist)", zap.Error(err))
		}
	}

	schema.PKColumn = pkColumn
	schema.HasUnique = true
	schema.Prepared = true

	// Reload schema after modifications
	newSchema := sa.loadSchemaInSchema(schemaName, tableName)
	if newSchema != nil {
		newSchema.PKColumn = pkColumn
		newSchema.HasUnique = true
		newSchema.Prepared = true
		sa.cache.Store(schemaCacheKey(schemaName, tableName), newSchema)
		sa.cache.Store(tableName, newSchema)
	}

	sa.logger.Info("table prepared for CDC insert", zap.String("schema", schemaName), zap.String("table", tableName), zap.String("pk", pkColumn))
	return nil
}

// createShadowTableV1 emits CREATE SCHEMA + CREATE TABLE IF NOT EXISTS
// for a V1 shadow target. Conservative TEXT pk: PrepareForCDCInsert is
// the legacy fallback path, so we don't infer pk type from MappedData
// to avoid schema drift.
//
// NOTE (architect P2 ruling): V1 keeps TEXT PK to swallow any source
// shape (Mongo ObjectID, UUID, BIGINT, ...) without runtime type
// inference. This trades insert-side correctness for SELECT/JOIN cost
// at scale. V2 callers MUST go through SchemaManager.createShadowTable
// which owns the typed-CREATE pipeline.
//
// Identifier quoting via pgx.Identifier{}.Sanitize() — refuses to
// emit anything that would parse as injection (NUL byte) and handles
// embedded quote characters per Postgres lexical rules.
func (sa *SchemaAdapter) createShadowTableV1(schemaName, tableName, pkColumn string) error {
	return sa.createShadowTableV1WithCols(schemaName, tableName, pkColumn, nil)
}

// createShadowTableV1WithCols — variant that inlines business-column
// definitions from the source manifest. Conservative typing: caller has
// already mapped engine-specific types to PG equivalents. PK stays
// TEXT (V1 fallback contract).
func (sa *SchemaAdapter) createShadowTableV1WithCols(
	schemaName, tableName, pkColumn string, businessCols []BusinessColumn,
) error {
	schemaName = strings.TrimSpace(schemaName)
	tableName = strings.TrimSpace(tableName)
	pkColumn = strings.TrimSpace(pkColumn)
	if schemaName == "" || tableName == "" || pkColumn == "" {
		return fmt.Errorf("createShadowTableV1: schema/table/pk required (got %q/%q/%q)", schemaName, tableName, pkColumn)
	}
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	qualified := pgx.Identifier{schemaName, tableName}.Sanitize()
	pkIdent := pgx.Identifier{pkColumn}.Sanitize()

	if err := sa.db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schemaIdent)).Error; err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	// Build inline business column list (skip pk to avoid duplicate).
	var bizDDL strings.Builder
	seen := map[string]bool{strings.ToLower(pkColumn): true}
	for _, bc := range businessCols {
		name := strings.TrimSpace(bc.Name)
		if name == "" || seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		colIdent := pgx.Identifier{name}.Sanitize()
		dt := strings.TrimSpace(bc.DataType)
		if dt == "" {
			dt = "TEXT"
		}
		nullClause := ""
		if !bc.Nullable {
			// Allow NULL anyway: source row may legitimately omit a
			// column (especially Mongo schemaless). Discover-side rules
			// can later tighten via mapping_rule_v2 NOT NULL flags.
			nullClause = " NULL"
		}
		fmt.Fprintf(&bizDDL, "\n\t\t%s %s%s,", colIdent, dt, nullClause)
	}

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		"_gpay_id" BIGINT,
		%s TEXT,%s
		"_raw_data" JSONB,
		"_source" VARCHAR(20) DEFAULT 'airbyte',
		"_synced_at" TIMESTAMP DEFAULT NOW(),
		"_version" BIGINT DEFAULT 1,
		"_hash" VARCHAR(64),
		"_deleted" BOOLEAN DEFAULT FALSE,
		"_created_at" TIMESTAMP DEFAULT NOW(),
		"_updated_at" TIMESTAMP DEFAULT NOW()
	)`, qualified, pkIdent, bizDDL.String())
	if err := sa.db.Exec(ddl).Error; err != nil {
		return fmt.Errorf("create table: %w", err)
	}
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

	// JSONB column — ensure value is valid JSON and normalize Mongo
	// extended JSON / base64-encoded Avro payloads into stable JSONB.
	switch v := val.(type) {
	case string:
		if decoded := decodeBase64JSON(v); len(decoded) > 0 {
			return string(decoded)
		}
		if json.Valid([]byte(v)) {
			var parsed interface{}
			if err := json.Unmarshal([]byte(v), &parsed); err == nil {
				normalized := normalizeMongoExtendedJSON(parsed)
				if marshaled, err := json.Marshal(normalized); err == nil {
					return string(marshaled)
				}
			}
			return v
		}
		// Wrap as JSON string
		jsonVal, _ := json.Marshal(v)
		return string(jsonVal)
	case map[string]interface{}, []interface{}:
		jsonVal, _ := json.Marshal(normalizeMongoExtendedJSON(v))
		return string(jsonVal)
	default:
		jsonVal, _ := json.Marshal(normalizeMongoExtendedJSON(v))
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
	return sa.BuildUpsertSQLInSchema(schema, "public", tableName, pkField, pkValue, mappedData, rawData, source, hash, sourceTsMs)
}

func (sa *SchemaAdapter) BuildUpsertSQLInSchema(schema *TableSchema, schemaName, tableName string, pkField string,
	pkValue interface{}, mappedData map[string]interface{}, rawData, source, hash string,
	sourceTsMs int64) (string, []interface{}) {
	hasSourceTs := false
	if _, ok := schema.Columns["_source_ts"]; ok {
		hasSourceTs = true
	}
	qualifiedTable := quoteQualifiedTable(schemaName, tableName)

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
	// V2 anchor key — populate _gpay_source_id from source PK so master
	// ON CONFLICT (_gpay_source_id) gets a distinct value per source row.
	if _, ok := schema.Columns["_gpay_source_id"]; ok {
		allCols = append(allCols, `"_gpay_source_id"`)
		allPlaceholders = append(allPlaceholders, "?")
		finalValues = append(finalValues, fmt.Sprintf("%v", pkValue))
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
		updateSets = append(updateSets, fmt.Sprintf(`"_version" = %s."_version" + 1`, qualifiedTable))
	}
	if _, ok := schema.Columns["_hash"]; ok {
		updateSets = append(updateSets, `"_hash" = EXCLUDED."_hash"`)
	}
	if _, ok := schema.Columns["_gpay_source_id"]; ok {
		updateSets = append(updateSets, `"_gpay_source_id" = EXCLUDED."_gpay_source_id"`)
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
			`WHERE %s."_source_ts" IS NULL OR %s."_source_ts" <= EXCLUDED."_source_ts"`,
			qualifiedTable, qualifiedTable,
		)
	} else {
		whereClause = fmt.Sprintf(`WHERE %s."_hash" IS DISTINCT FROM EXCLUDED."_hash"`, qualifiedTable)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT ("%s") DO UPDATE SET %s %s`,
		qualifiedTable,
		strings.Join(allCols, ", "),
		strings.Join(allPlaceholders, ", "),
		pkField,
		strings.Join(updateSets, ", "),
		whereClause,
	)

	return query, finalValues
}

func schemaCacheKey(schemaName, tableName string) string {
	return strings.TrimSpace(schemaName) + "." + strings.TrimSpace(tableName)
}

// quoteQualifiedTable emits "schema"."table" using pgx's identifier
// sanitiser instead of hand-rolled escape — covers embedded quotes,
// rejects NUL bytes, matches Postgres lexer rules exactly.
func quoteQualifiedTable(schemaName, tableName string) string {
	return pgx.Identifier{strings.TrimSpace(schemaName), strings.TrimSpace(tableName)}.Sanitize()
}

func decodeBase64JSON(v string) []byte {
	for _, decoder := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		decoded, err := decoder.DecodeString(v)
		if err == nil && json.Valid(decoded) {
			var parsed interface{}
			if err := json.Unmarshal(decoded, &parsed); err == nil {
				normalized := normalizeMongoExtendedJSON(parsed)
				if marshaled, err := json.Marshal(normalized); err == nil {
					return marshaled
				}
			}
			return decoded
		}
	}
	return nil
}

func normalizeMongoExtendedJSON(val interface{}) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		if oid, ok := asOIDValue(v); ok {
			return oid
		}
		if dateVal, ok := asDateValue(v); ok {
			return dateVal
		}
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = normalizeMongoExtendedJSON(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = normalizeMongoExtendedJSON(item)
		}
		return out
	default:
		return val
	}
}

func asOIDValue(v map[string]interface{}) (string, bool) {
	if len(v) != 1 {
		return "", false
	}
	raw, ok := v["$oid"]
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%v", raw), true
}

func asDateValue(v map[string]interface{}) (string, bool) {
	if len(v) != 1 {
		return "", false
	}
	raw, ok := v["$date"]
	if !ok {
		return "", false
	}
	switch d := raw.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, d); err == nil {
			return t.UTC().Format(time.RFC3339Nano), true
		}
		if t, err := time.Parse(time.RFC3339, d); err == nil {
			return t.UTC().Format(time.RFC3339Nano), true
		}
		return d, true
	case float64:
		return time.UnixMilli(int64(d)).UTC().Format(time.RFC3339Nano), true
	case int64:
		return time.UnixMilli(d).UTC().Format(time.RFC3339Nano), true
	case int32:
		return time.UnixMilli(int64(d)).UTC().Format(time.RFC3339Nano), true
	case json.Number:
		if ms, err := d.Int64(); err == nil {
			return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano), true
		}
	case map[string]interface{}:
		if num, ok := d["$numberLong"]; ok {
			switch n := num.(type) {
			case string:
				if parsed, err := json.Number(n).Int64(); err == nil {
					return time.UnixMilli(parsed).UTC().Format(time.RFC3339Nano), true
				}
			case json.Number:
				if parsed, err := n.Int64(); err == nil {
					return time.UnixMilli(parsed).UTC().Format(time.RFC3339Nano), true
				}
			}
		}
	}
	return "", false
}
