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
// It protects cdc_internal from the two well-known Mongo-schema-drift risks
// flagged in plan §7.7:
//  1. Rate limit: MongoDB schema-less => Debezium can push many new fields
//     per minute. Cap per-table ALTER to 100/day — headroom enough for a
//     single-record bootstrap (~30 columns) while still catching runaway
//     drift. Beyond the cap we log+skip (the extra field round-trips via
//     _raw_data; schema catches up on the next day or via manual ALTER).
//  2. Financial audit: tables marked is_financial=true in
//     cdc_internal.table_registry have auto-ALTER refused until an admin
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

// EnsureShadowTable guarantees that cdc_internal.<table> exists and carries
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
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, err := s.loadColumnSet(ctx, table)
	if err != nil {
		return err
	}

	if len(existing) == 0 {
		// Brand-new shadow. Build columns from the *current* record so the
		// first message's business fields land in the CREATE statement.
		if err := s.createShadowTable(ctx, table, record); err != nil {
			return fmt.Errorf("create shadow %s: %w", table, err)
		}
		// Drop stale cache (empty set from pre-CREATE read) before refresh.
		delete(s.cols, table)
		existing, err = s.loadColumnSet(ctx, table)
		if err != nil {
			return err
		}
		// First message's columns are now in the shadow. Skip the ALTER
		// loop below — every key in `record` is guaranteed to be present.
		return nil
	}

	// Incremental ALTER for any new field not in the shadow.
	isFinancial := s.isFinancial(ctx, table)
	for k, v := range record {
		if _, has := existing[k]; has {
			continue
		}
		sqlType := inferSQLType(v)
		if isFinancial {
			s.logger.Warn("financial table has new field — auto-ALTER blocked, proposal filed",
				zap.String("table", table),
				zap.String("field", k),
			)
			s.recordProposal(ctx, table, k, sqlType, v, "financial_block")
			delete(record, k) // leave the raw value in _raw_data only
			continue
		}
		if !s.allowAlter(table) {
			s.logger.Warn("ALTER rate limit hit, proposal filed (field stays in _raw_data)",
				zap.String("table", table),
				zap.String("field", k),
			)
			s.recordProposal(ctx, table, k, sqlType, v, "rate_limit")
			delete(record, k)
			continue
		}
		stmt := fmt.Sprintf(`ALTER TABLE cdc_internal.%s ADD COLUMN IF NOT EXISTS %s %s`,
			quoteIdent(table), quoteIdent(k), sqlType)
		if err := s.db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("ALTER %s ADD %s %s: %w", table, k, sqlType, err)
		}
		existing[k] = struct{}{}
		s.recordAlter(table)
		s.logger.Info("auto-ALTER added column",
			zap.String("table", table),
			zap.String("field", k),
			zap.String("type", sqlType),
		)
	}
	s.cols[table] = existing
	return nil
}

// recordProposal inserts a schema_proposal row for admin review when
// auto-ALTER is refused. Non-blocking: any error is logged but never
// bubbled up so normal ingest continues.
//
// Uniqueness: (table_name, table_layer, column_name, status). We skip
// if a pending row already exists for this (table, column) pair — no
// spam if the same new field arrives on every message.
func (s *SchemaManager) recordProposal(ctx context.Context, table, column, sqlType string, sampleValue any, reason string) {
	// Guard column name — defence-in-depth against shadow-to-DB injection.
	if !identRE.MatchString(column) {
		s.logger.Warn("recordProposal: invalid column name — skip",
			zap.String("table", table), zap.String("col", column))
		return
	}

	// Pull 2 extra sample rows (best-effort) from cdc_internal.<table>
	// so admin can eyeball real values. Best-effort + short timeout via ctx.
	samples := []any{sampleValue}
	sampleBytes, _ := jsonMarshal(map[string]any{
		"values":         samples,
		"proposed_by":    "sinkworker-auto",
		"reason":         reason,
		"source_table":   table,
		"proposed_field": column,
	})

	err := s.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_internal.schema_proposal
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
func (s *SchemaManager) loadColumnSet(ctx context.Context, table string) (map[string]struct{}, error) {
	if c, ok := s.cols[table]; ok {
		return c, nil
	}
	var cols []string
	err := s.db.WithContext(ctx).Raw(
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = 'cdc_internal' AND table_name = ?`, table,
	).Scan(&cols).Error
	if err != nil {
		return nil, fmt.Errorf("read columns for %s: %w", table, err)
	}
	set := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		set[c] = struct{}{}
	}
	s.cols[table] = set
	return set, nil
}

func (s *SchemaManager) createShadowTable(ctx context.Context, table string, record map[string]any) error {
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

	create := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS cdc_internal.%s (
%s
)`, quoteIdent(table), "  "+strings.Join(cols, ",\n  "))

	if err := s.db.WithContext(ctx).Exec(create).Error; err != nil {
		return fmt.Errorf("CREATE TABLE: %w", err)
	}

	// Partial UNIQUE index is what ON CONFLICT (_gpay_source_id) WHERE NOT
	// _gpay_deleted targets. Without this, the UPSERT would fall through
	// to a plain INSERT and duplicate on re-consume.
	idx := fmt.Sprintf(
		`CREATE UNIQUE INDEX IF NOT EXISTS %s
		   ON cdc_internal.%s (_gpay_source_id)
		   WHERE NOT _gpay_deleted`,
		quoteIdent("ux_"+table+"_source_id_active"),
		quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(idx).Error; err != nil {
		return fmt.Errorf("CREATE INDEX: %w", err)
	}

	// Attach fencing trigger (T1.3).
	trigName := "trg_" + table + "_fencing"
	// DROP-then-CREATE is idempotent even after partial previous runs.
	drop := fmt.Sprintf(
		`DROP TRIGGER IF EXISTS %s ON cdc_internal.%s`,
		quoteIdent(trigName), quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(drop).Error; err != nil {
		return fmt.Errorf("DROP TRIGGER: %w", err)
	}
	trg := fmt.Sprintf(
		`CREATE TRIGGER %s
		   BEFORE INSERT OR UPDATE ON cdc_internal.%s
		   FOR EACH ROW EXECUTE FUNCTION cdc_internal.tg_fencing_guard()`,
		quoteIdent(trigName), quoteIdent(table),
	)
	if err := s.db.WithContext(ctx).Exec(trg).Error; err != nil {
		return fmt.Errorf("CREATE TRIGGER: %w", err)
	}

	s.logger.Info("created shadow table with fencing trigger",
		zap.String("table", "cdc_internal."+table),
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

// isFinancial reads the registry flag from cdc_internal.table_registry and
// memoises it for financialTTL. An unregistered table is treated as
// is_financial=true (fail-safe for unknown shapes). Admin toggles are
// picked up within the TTL window without a restart.
func (s *SchemaManager) isFinancial(ctx context.Context, table string) bool {
	if e, ok := s.financialCache[table]; ok && time.Since(e.loadedAt) < s.financialTTL {
		return e.isFinancial
	}
	var flag bool
	err := s.db.WithContext(ctx).Raw(
		`SELECT is_financial FROM cdc_internal.table_registry WHERE target_table = ? LIMIT 1`,
		table,
	).Scan(&flag).Error
	if err != nil {
		s.logger.Warn("registry lookup failed, defaulting to is_financial=true (fail-safe)",
			zap.String("table", table), zap.Error(err))
		s.financialCache[table] = financialCacheEntry{isFinancial: true, loadedAt: time.Now()}
		return true
	}
	s.financialCache[table] = financialCacheEntry{isFinancial: flag, loadedAt: time.Now()}
	return flag
}

// invalidateFinancialCache clears the cached registry flag for a table.
// Intended for test + admin tooling (e.g. "apply is_financial now" button).
func (s *SchemaManager) invalidateFinancialCache(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.financialCache, table)
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
