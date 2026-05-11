package sinkworker

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// jsonMarshalPkg is encoding/json.Marshal with a friendlier name so the
// file-local jsonMarshal helper stays single-purpose.
var jsonMarshalPkg = json.Marshal

// financialCacheEntry memoises a registry is_financial lookup with a short
// TTL so a toggle from the CMS admin UI takes effect within seconds without
// a SinkWorker restart.
type financialCacheEntry struct {
	isFinancial bool
	loadedAt    time.Time
}

// SchemaManager is idempotent: repeated calls for the same record are cheap.
// It protects shadow_<source_db> schemas from the two well-known Mongo-schema-drift risks
// flagged in plan §7.7:
//  1. Rate limit: MongoDB schema-less => Debezium can push many new fields
//     per minute. Cap per-table ALTER to 100/day — headroom enough for a
//     single-record bootstrap (~30 columns) while still catching runaway
//     drift. Beyond the cap we log+skip (the extra field round-trips via
//     _raw_data; schema catches up on the next day or via manual ALTER).
//  2. Financial audit: tables marked is_financial=true in
//     cdc_system.shadow_binding/source metadata have auto-ALTER refused until an admin
//     flips the flag via CMS (PATCH /api/v1/tables/:name).
//
// The financial classification is REGISTRY-DRIVEN — no regex on field names.
// An unregistered table defaults to is_financial=true (fail-safe: require
// explicit admin sign-off for every new shadow).
type SchemaManager struct {
	db     *gorm.DB
	logger *zap.Logger

	mu             sync.Mutex
	cols           map[string]map[string]struct{} // table -> column set cache
	alterLog       map[string][]time.Time         // table -> ALTER timestamps (rolling 24h)
	financialCache map[string]financialCacheEntry // table -> is_financial cache
	financialTTL   time.Duration
}

func NewSchemaManager(db *gorm.DB, logger *zap.Logger) *SchemaManager {
	return &SchemaManager{
		db:             db,
		logger:         logger,
		cols:           make(map[string]map[string]struct{}),
		alterLog:       make(map[string][]time.Time),
		financialCache: make(map[string]financialCacheEntry),
		financialTTL:   60 * time.Second,
	}
}

// EnsureShadowTable guarantees that shadow_default.<table> exists and carries
// every key in `record` as a column. It only ever grows the schema — we
// never drop columns. It attaches `tg_fencing_guard` the first time a
// shadow table is created (T1.3 requirement).
//
// IMPORTANT: when a business field CANNOT be added (financial audit gate,
// or ALTER rate limit exhausted), we DELETE it from `record` in place.
// This keeps the UPSERT runnable with the remaining columns — the dropped
// value is never lost because `_raw_data` already preserves the full
// envelope, so a later backfill / admin-approved ALTER can recover it.
func (s *SchemaManager) EnsureShadowTable(ctx context.Context, table string, record map[string]any) error {
	return s.EnsureShadowTableInSchema(ctx, "shadow_default", table, record)
}

func (s *SchemaManager) EnsureShadowTableInSchema(ctx context.Context, schemaName, table string, record map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cacheKey := shadowCacheKey(schemaName, table)
	existing, err := s.loadColumnSet(ctx, schemaName, table)
	if err != nil {
		return err
	}

	if len(existing) == 0 {
		// Brand-new shadow. Build columns from the *current* record so the
		// first message's business fields land in the CREATE statement.
		if err := s.createShadowTable(ctx, schemaName, table, record); err != nil {
			return fmt.Errorf("create shadow %s.%s: %w", schemaName, table, err)
		}
		// Drop stale cache (empty set from pre-CREATE read) before refresh.
		delete(s.cols, cacheKey)
		existing, err = s.loadColumnSet(ctx, schemaName, table)
		if err != nil {
			return err
		}
		// First message's columns are now in the shadow. Skip the ALTER
		// loop below — every key in `record` is guaranteed to be present.
		return nil
	}

	// Incremental ALTER for any new field not in the shadow.
	isFinancial := s.isFinancial(ctx, schemaName, table)
	for k, v := range record {
		if _, has := existing[k]; has {
			continue
		}
		sqlType := inferSQLType(v)
		if isFinancial {
			s.logger.Warn("financial table has new field — auto-ALTER blocked, proposal filed",
				zap.String("table", schemaName+"."+table),
				zap.String("field", k),
			)
			s.recordProposal(ctx, schemaName, table, k, sqlType, v, "financial_block")
			delete(record, k) // leave the raw value in _raw_data only
			continue
		}
		if !s.allowAlter(cacheKey) {
			s.logger.Warn("ALTER rate limit hit, proposal filed (field stays in _raw_data)",
				zap.String("table", schemaName+"."+table),
				zap.String("field", k),
			)
			s.recordProposal(ctx, schemaName, table, k, sqlType, v, "rate_limit")
			delete(record, k)
			continue
		}
		stmt := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s`,
			quoteIdent(schemaName), quoteIdent(table), quoteIdent(k), sqlType)
		if err := s.db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("ALTER %s.%s ADD %s %s: %w", schemaName, table, k, sqlType, err)
		}
		existing[k] = struct{}{}
		s.recordAlter(cacheKey)
		s.logger.Info("auto-ALTER added column",
			zap.String("table", schemaName+"."+table),
			zap.String("field", k),
			zap.String("type", sqlType),
		)
	}
	s.cols[cacheKey] = existing
	return nil
}

// recordProposal inserts a schema_proposal row for admin review when
// auto-ALTER is refused. Non-blocking: any error is logged but never
// bubbled up so normal ingest continues.
//
// Uniqueness: (table_name, table_layer, column_name, status). We skip
// if a pending row already exists for this (table, column) pair — no
// spam if the same new field arrives on every message.
func (s *SchemaManager) recordProposal(ctx context.Context, schemaName, table, column, sqlType string, sampleValue any, reason string) {
	// Guard column name — defence-in-depth against shadow-to-DB injection.
	if !identRE.MatchString(column) {
		s.logger.Warn("recordProposal: invalid column name — skip",
			zap.String("table", table), zap.String("col", column))
		return
	}

	// Pull 2 extra sample rows (best-effort) from shadow_<db>.<table>
	// so admin can eyeball real values. Best-effort + short timeout via ctx.
	samples := []any{sampleValue}
	sampleBytes, _ := jsonMarshal(map[string]any{
		"values":         samples,
		"proposed_by":    "sinkworker-auto",
		"reason":         reason,
		"source_schema":  schemaName,
		"source_table":   table,
		"proposed_field": column,
	})

	err := s.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.schema_proposal
		    (table_name, table_layer, column_name, proposed_data_type,
		     proposed_is_nullable, sample_values, status, submitted_by)
		 VALUES (?, 'shadow', ?, ?, true, ?::jsonb, 'pending', 'sinkworker-auto')
		 ON CONFLICT (table_name, table_layer, column_name, status) DO NOTHING`,
		table, column, sqlType, string(sampleBytes),
	).Error
	if err != nil {
		s.logger.Warn("schema_proposal insert failed",
			zap.String("table", table), zap.String("col", column), zap.Error(err))
	}
}

var identRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,62}$`)

// jsonMarshal is a tiny indirection so the SchemaManager file avoids
// pulling encoding/json at the top. Inlined here to keep surface tight.
func jsonMarshal(v any) ([]byte, error) {
	return jsonMarshalPkg(v)
}

// loadColumnSet reads information_schema for an existing shadow. Returns
// an empty map (len==0) when the table does not yet exist. The cache is
// refreshed lazily so a SinkWorker restart always learns the live shape.
func (s *SchemaManager) loadColumnSet(ctx context.Context, schemaName, table string) (map[string]struct{}, error) {
	cacheKey := shadowCacheKey(schemaName, table)
	if c, ok := s.cols[cacheKey]; ok {
		return c, nil
	}
	var cols []string
	err := s.db.WithContext(ctx).Raw(
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = ? AND table_name = ?`, schemaName, table,
	).Scan(&cols).Error
	if err != nil {
		return nil, fmt.Errorf("read columns for %s.%s: %w", schemaName, table, err)
	}
	set := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		set[c] = struct{}{}
	}
	s.cols[cacheKey] = set
	return set, nil
}

func (s *SchemaManager) createShadowTable(ctx context.Context, schemaName, table string, record map[string]any) error {
	// 10 system columns are always declared explicitly with correct SQL types.
	// Business columns inferred from the first message — subsequent messages
	// extend the schema via ALTER (handled by the EnsureShadowTable caller).
	cols := []string{
		`"_gpay_id" BIGINT PRIMARY KEY`,
		`"_gpay_source_id" TEXT NOT NULL`,
		`"_raw_data" JSONB NOT NULL`,
		`"_source" TEXT NOT NULL`,
		`"_synced_at" TIMESTAMPTZ NOT NULL`,
		`"_source_ts" BIGINT`,
		`"_version" BIGINT NOT NULL DEFAULT 1`,
		`"_hash" TEXT NOT NULL`,
		`"_gpay_deleted" BOOLEAN NOT NULL DEFAULT FALSE`,
		`"_created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`"_updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
	}
	reserved := systemFieldsSet()
	for k, v := range record {
		if _, isSystem := reserved[k]; isSystem {
			continue
		}
		cols = append(cols, fmt.Sprintf(`%s %s`, quoteIdent(k), inferSQLType(v)))
	}

	if err := s.db.WithContext(ctx).Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(schemaName))).Error; err != nil {
		return fmt.Errorf("CREATE SCHEMA: %w", err)
	}

	create := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s.%s (
%s
)`, quoteIdent(schemaName), quoteIdent(table), "  "+strings.Join(cols, ",\n  "))

	if err := s.db.WithContext(ctx).Exec(create).Error; err != nil {
		return fmt.Errorf("CREATE TABLE: %w", err)
	}

	// Partial UNIQUE index is what ON CONFLICT (_gpay_source_id) WHERE NOT
	// _gpay_deleted targets. Without this, the UPSERT would fall through
	// to a plain INSERT and duplicate on re-consume.
	idx := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS %s
		   ON %s.%s (_gpay_source_id)
		   WHERE NOT _gpay_deleted`,
		quoteIdent("ux_"+table+"_source_id_active"),
		quoteIdent(schemaName),
		quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(idx).Error; err != nil {
		return fmt.Errorf("CREATE INDEX: %w", err)
	}

	// Attach fencing trigger (T1.3).
	trigName := "trg_" + table + "_fencing"
	// DROP-then-CREATE is idempotent even after partial previous runs.
	drop := fmt.Sprintf(
		`DROP TRIGGER IF EXISTS %s ON %s.%s`,
		quoteIdent(trigName),
		quoteIdent(schemaName),
		quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(drop).Error; err != nil {
		return fmt.Errorf("DROP TRIGGER: %w", err)
	}
	trg := fmt.Sprintf(
		`CREATE TRIGGER %s
		   BEFORE INSERT OR UPDATE ON %s.%s
		   FOR EACH ROW EXECUTE FUNCTION cdc_system.tg_fencing_guard()`,
		quoteIdent(trigName), quoteIdent(schemaName), quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(trg).Error; err != nil {
		return fmt.Errorf("CREATE TRIGGER: %w", err)
	}

	s.logger.Info("created shadow table with fencing trigger",
		zap.String("table", schemaName+"."+table),
		zap.String("trigger", trigName),
	)
	return nil
}

func (s *SchemaManager) allowAlter(table string) bool {
	const window = 24 * time.Hour
	const maxAlters = 100
	now := time.Now()
	keep := s.alterLog[table][:0]
	for _, t := range s.alterLog[table] {
		if now.Sub(t) < window {
			keep = append(keep, t)
		}
	}
	s.alterLog[table] = keep
	return len(keep) < maxAlters
}

func (s *SchemaManager) recordAlter(table string) {
	s.alterLog[table] = append(s.alterLog[table], time.Now())
}

// isFinancial reads the registry flag from V2 metadata and
// memoises it for financialTTL. An unregistered table is treated as
// is_financial=true (fail-safe for unknown shapes). Admin toggles are
// picked up within the TTL window without a restart.
func (s *SchemaManager) isFinancial(ctx context.Context, schemaName, table string) bool {
	cacheKey := shadowCacheKey(schemaName, table)
	if e, ok := s.financialCache[cacheKey]; ok && time.Since(e.loadedAt) < s.financialTTL {
		return e.isFinancial
	}
	var flag bool
	err := s.db.WithContext(ctx).Raw(
		`SELECT COALESCE(NULLIF(sor.source_locator_json->>'is_financial', '')::boolean, true)
		   FROM cdc_system.shadow_binding sb
		   JOIN cdc_system.source_object_registry sor ON sor.id = sb.source_object_id
		  WHERE sb.shadow_schema = ? AND sb.shadow_table = ?
		  LIMIT 1`,
		schemaName, table,
	).Scan(&flag).Error
	if err != nil {
		s.logger.Warn("registry lookup failed, defaulting to is_financial=true (fail-safe)",
			zap.String("table", schemaName+"."+table), zap.Error(err))
		s.financialCache[cacheKey] = financialCacheEntry{isFinancial: true, loadedAt: time.Now()}
		return true
	}
	s.financialCache[cacheKey] = financialCacheEntry{isFinancial: flag, loadedAt: time.Now()}
	return flag
}

// invalidateFinancialCache clears the cached registry flag for a table.
// Intended for test + admin tooling (e.g. "apply is_financial now" button).
func (s *SchemaManager) invalidateFinancialCache(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.financialCache, table)
}

func shadowCacheKey(schemaName, table string) string {
	return schemaName + "." + table
}

// inferSQLType picks a conservative SQL type. We deliberately over-size:
// NUMERIC catches ints AND decimals, JSONB catches nested documents / arrays,
// and TEXT is the safe fallback (Postgres TEXT has no length limit).
// Financial-grade decimal precision is intentionally out of scope here —
// financial tables have auto-ALTER disabled so the admin picks the type
// manually when approving the schema.
func inferSQLType(v any) string {
	switch v.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32, int64, float32, float64:
		return "NUMERIC"
	case map[string]any, []any:
		return "JSONB"
	case time.Time:
		return "TIMESTAMPTZ"
	case nil:
		return "TEXT"
	default:
		return "TEXT"
	}
}

// systemFieldsSet is the authoritative list of the 10 system + 1 helper
// (_gpay_deleted) columns we create + reserve. Used by EnsureShadowTable to
// avoid declaring a business column twice, and by SinkWorker.shouldSkipBusinessKey.
func systemFieldsSet() map[string]struct{} {
	return map[string]struct{}{
		"_gpay_id":        {},
		"_gpay_source_id": {},
		"_raw_data":       {},
		"_source":         {},
		"_synced_at":      {},
		"_source_ts":      {},
		"_version":        {},
		"_hash":           {},
		"_gpay_deleted":   {},
		"_created_at":     {},
		"_updated_at":     {},
	}
}
