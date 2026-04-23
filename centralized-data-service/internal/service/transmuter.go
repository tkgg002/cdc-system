package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransmuterModule reads from cdc_internal.<shadow> JSONB rows, applies
// approved cdc_mapping_rules (with JsonPath + transform_fn), and upserts
// typed rows into public.<master>.
//
// Idempotency contract:
//   - For each (shadow, master) pair, output row is keyed by _gpay_source_id
//     (copied from shadow).
//   - Master row holds its own _hash computed from the canonical extracted
//     payload. Second run with identical input → 0 UPDATE.
//
// Active-gate contract (§17 of plan v2):
//   - shadow `is_active=true` AND `profile_status='active'`
//   - master `is_active=true` AND `schema_status='approved'`
//   - Run skips with WARN log if either check fails — never error.
type TransmuterModule struct {
	db          *gorm.DB
	typeRes     *TypeResolver
	logger      *zap.Logger
	batchSize   int
	mu          sync.Mutex
	cache       map[string]cachedRules // master_table -> ruleset
	cacheTTL    time.Duration
	shadowCache map[string]shadowState
}

type cachedRules struct {
	rules    []mappingRuleRow
	loadedAt time.Time
}

type shadowState struct {
	isActive      bool
	profileStatus string
	loadedAt      time.Time
}

// MappingRuleRow mirrors the subset of cdc_mapping_rules columns the
// Transmuter needs at runtime. Distinct from the CMS model so the worker
// package has no CMS dependency.
type mappingRuleRow struct {
	ID            int64   `gorm:"column:id"`
	SourceTable   string  `gorm:"column:source_table"`
	MasterTable   *string `gorm:"column:master_table"`
	SourceField   string  `gorm:"column:source_field"`
	TargetColumn  string  `gorm:"column:target_column"`
	DataType      string  `gorm:"column:data_type"`
	SourceFormat  string  `gorm:"column:source_format"`
	JsonPath      *string `gorm:"column:jsonpath"`
	TransformFn   *string `gorm:"column:transform_fn"`
	IsNullable    bool    `gorm:"column:is_nullable"`
	DefaultValue  *string `gorm:"column:default_value"`
}

func NewTransmuterModule(db *gorm.DB, typeRes *TypeResolver, logger *zap.Logger) *TransmuterModule {
	return &TransmuterModule{
		db:          db,
		typeRes:     typeRes,
		logger:      logger,
		batchSize:   500,
		cache:       make(map[string]cachedRules),
		shadowCache: make(map[string]shadowState),
		cacheTTL:    60 * time.Second,
	}
}

// TransmuteResult summarises one run for callers (NATS handler, scheduler).
type TransmuteResult struct {
	Master      string `json:"master"`
	Source      string `json:"source"`
	Scanned     int64  `json:"scanned"`
	Inserted    int64  `json:"inserted"`
	Updated     int64  `json:"updated"`
	Skipped     int64  `json:"skipped"`
	RuleMisses  int64  `json:"rule_misses"`
	TypeErrors  int64  `json:"type_errors"`
	DurationMs  int64  `json:"duration_ms"`
	ActiveGate  string `json:"active_gate,omitempty"` // "" if pass, else reason
}

// Run materialises the full shadow → master set. This is the entry point
// for immediate + scheduled + post-ingest triggers. Caller passes
// `onlySourceIDs` (optional) to limit scope to a slice of _gpay_source_id,
// typical for post-ingest incremental sync.
func (t *TransmuterModule) Run(
	ctx context.Context,
	masterName string,
	onlySourceIDs []string,
) (TransmuteResult, error) {
	start := time.Now()
	res := TransmuteResult{Master: masterName}

	// 1. Master registry lookup + active gate L2.
	masterRow, err := t.loadMaster(ctx, masterName)
	if err != nil {
		return res, fmt.Errorf("master lookup: %w", err)
	}
	if !masterRow.IsActive || masterRow.SchemaStatus != "approved" {
		res.ActiveGate = fmt.Sprintf("master gate: is_active=%v schema_status=%s",
			masterRow.IsActive, masterRow.SchemaStatus)
		t.logger.Warn("transmute skipped — master inactive",
			zap.String("master", masterName),
			zap.Bool("is_active", masterRow.IsActive),
			zap.String("schema_status", masterRow.SchemaStatus))
		return res, nil
	}

	res.Source = masterRow.SourceShadow

	// 2. Shadow active gate L1.
	if ok, reason := t.shadowActive(ctx, masterRow.SourceShadow); !ok {
		res.ActiveGate = fmt.Sprintf("shadow gate: %s", reason)
		t.logger.Warn("transmute skipped — shadow inactive",
			zap.String("shadow", masterRow.SourceShadow), zap.String("reason", reason))
		return res, nil
	}

	// 3. Rule set.
	rules, err := t.loadRules(ctx, masterRow.SourceShadow, masterName)
	if err != nil {
		return res, fmt.Errorf("rule load: %w", err)
	}
	if len(rules) == 0 {
		t.logger.Warn("transmute: no approved rules — nothing to do",
			zap.String("master", masterName))
		res.DurationMs = time.Since(start).Milliseconds()
		return res, nil
	}

	// 4. Scan shadow rows in batches.
	var lastGpayID int64 // cursor-paginate by _gpay_id ascending
	for {
		shadowRows, err := t.fetchShadowBatch(ctx, masterRow.SourceShadow, lastGpayID, onlySourceIDs)
		if err != nil {
			return res, fmt.Errorf("fetch shadow batch: %w", err)
		}
		if len(shadowRows) == 0 {
			break
		}
		batchRes := t.processBatch(ctx, masterName, rules, shadowRows)
		res.Scanned += batchRes.scanned
		res.Inserted += batchRes.inserted
		res.Updated += batchRes.updated
		res.Skipped += batchRes.skipped
		res.RuleMisses += batchRes.ruleMisses
		res.TypeErrors += batchRes.typeErrors
		lastGpayID = batchRes.lastGpayID

		if len(onlySourceIDs) > 0 {
			// Targeted incremental: one batch is enough.
			break
		}
	}
	res.DurationMs = time.Since(start).Milliseconds()
	return res, nil
}

// ---- helpers ----

type masterRegistryRow struct {
	MasterName     string          `gorm:"column:master_name"`
	SourceShadow   string          `gorm:"column:source_shadow"`
	TransformType  string          `gorm:"column:transform_type"`
	Spec           json.RawMessage `gorm:"column:spec"`
	IsActive       bool            `gorm:"column:is_active"`
	SchemaStatus   string          `gorm:"column:schema_status"`
}

func (t *TransmuterModule) loadMaster(ctx context.Context, name string) (*masterRegistryRow, error) {
	var row masterRegistryRow
	err := t.db.WithContext(ctx).Raw(
		`SELECT master_name, source_shadow, transform_type, spec, is_active, schema_status
		   FROM cdc_internal.master_table_registry
		  WHERE master_name = ?`, name,
	).Scan(&row).Error
	if err != nil {
		return nil, err
	}
	if row.MasterName == "" {
		return nil, fmt.Errorf("master %q not registered", name)
	}
	return &row, nil
}

func (t *TransmuterModule) shadowActive(ctx context.Context, shadow string) (bool, string) {
	t.mu.Lock()
	if e, ok := t.shadowCache[shadow]; ok && time.Since(e.loadedAt) < t.cacheTTL {
		t.mu.Unlock()
		if !e.isActive {
			return false, "is_active=false"
		}
		if e.profileStatus != "active" {
			return false, fmt.Sprintf("profile_status=%s", e.profileStatus)
		}
		return true, ""
	}
	t.mu.Unlock()

	var row struct {
		IsActive      bool   `gorm:"column:is_active"`
		ProfileStatus string `gorm:"column:profile_status"`
	}
	err := t.db.WithContext(ctx).Raw(
		`SELECT is_active, profile_status FROM cdc_internal.table_registry
		  WHERE target_table = ?`, shadow,
	).Scan(&row).Error
	if err != nil {
		return false, fmt.Sprintf("lookup error: %v", err)
	}

	t.mu.Lock()
	t.shadowCache[shadow] = shadowState{
		isActive: row.IsActive, profileStatus: row.ProfileStatus,
		loadedAt: time.Now(),
	}
	t.mu.Unlock()

	if !row.IsActive {
		return false, "is_active=false"
	}
	if row.ProfileStatus != "active" {
		return false, fmt.Sprintf("profile_status=%s", row.ProfileStatus)
	}
	return true, ""
}

func (t *TransmuterModule) loadRules(ctx context.Context, shadow, master string) ([]mappingRuleRow, error) {
	cacheKey := shadow + "|" + master
	t.mu.Lock()
	if e, ok := t.cache[cacheKey]; ok && time.Since(e.loadedAt) < t.cacheTTL {
		defer t.mu.Unlock()
		return e.rules, nil
	}
	t.mu.Unlock()

	var rules []mappingRuleRow
	err := t.db.WithContext(ctx).Raw(
		`SELECT id, source_table, master_table, source_field, target_column,
		        data_type, source_format, jsonpath, transform_fn, is_nullable, default_value
		   FROM cdc_mapping_rules
		  WHERE source_table = ?
		    AND (master_table = ? OR master_table IS NULL)
		    AND is_active = true
		    AND status = 'approved'
		  ORDER BY id`, shadow, master,
	).Scan(&rules).Error
	if err != nil {
		return nil, err
	}
	// Reject rules with non-whitelisted transform_fn up front.
	valid := rules[:0]
	for _, r := range rules {
		if r.TransformFn != nil && !IsTransformWhitelisted(*r.TransformFn) {
			t.logger.Error("rule rejected — unknown transform_fn",
				zap.Int64("rule_id", r.ID), zap.String("fn", *r.TransformFn))
			continue
		}
		if !t.typeRes.Validate(r.DataType) {
			t.logger.Error("rule rejected — invalid data_type",
				zap.Int64("rule_id", r.ID), zap.String("data_type", r.DataType))
			continue
		}
		valid = append(valid, r)
	}
	t.mu.Lock()
	t.cache[cacheKey] = cachedRules{rules: valid, loadedAt: time.Now()}
	t.mu.Unlock()
	return valid, nil
}

type shadowBatchRow struct {
	GpayID       int64  `gorm:"column:_gpay_id"`
	SourceID     string `gorm:"column:_gpay_source_id"`
	RawData      []byte `gorm:"column:_raw_data"`
	SourceTs     int64  `gorm:"column:_source_ts"`
	GpayDeleted  bool   `gorm:"column:_gpay_deleted"`
}

func (t *TransmuterModule) fetchShadowBatch(
	ctx context.Context, shadow string, cursor int64, onlyIDs []string,
) ([]shadowBatchRow, error) {
	var rows []shadowBatchRow
	qt := `SELECT _gpay_id, _gpay_source_id, _raw_data, _source_ts, _gpay_deleted
	         FROM cdc_internal.` + quoteIdent(shadow) + `
	        WHERE _gpay_id > ?`
	args := []any{cursor}
	if len(onlyIDs) > 0 {
		qt += ` AND _gpay_source_id = ANY(?)`
		args = append(args, onlyIDs)
	}
	qt += ` ORDER BY _gpay_id LIMIT ?`
	args = append(args, t.batchSize)

	err := t.db.WithContext(ctx).Raw(qt, args...).Scan(&rows).Error
	return rows, err
}

type batchOutcome struct {
	scanned, inserted, updated, skipped, ruleMisses, typeErrors, lastGpayID int64
}

func (t *TransmuterModule) processBatch(
	ctx context.Context, master string, rules []mappingRuleRow, rows []shadowBatchRow,
) batchOutcome {
	out := batchOutcome{}
	for _, row := range rows {
		out.scanned++
		out.lastGpayID = row.GpayID

		record, hash, miss, typeErr := t.buildMasterRow(ctx, rules, row)
		out.ruleMisses += miss
		out.typeErrors += typeErr

		if len(record) == 0 {
			out.skipped++
			continue
		}

		// System columns stitched on unconditionally.
		record["_gpay_id"] = row.GpayID
		record["_gpay_source_id"] = row.SourceID
		record["_source"] = "debezium-transmute"
		record["_source_ts"] = row.SourceTs
		record["_synced_at"] = time.Now().UTC()
		record["_hash"] = hash
		record["_gpay_deleted"] = row.GpayDeleted
		record["_version"] = int64(1)

		upd, err := t.upsertMaster(ctx, master, record)
		if err != nil {
			t.logger.Error("master upsert failed",
				zap.String("master", master),
				zap.String("source_id", row.SourceID),
				zap.Error(err))
			out.skipped++
			continue
		}
		if upd > 0 {
			out.updated++
		} else {
			out.inserted++
		}
	}
	return out
}

// buildMasterRow evaluates JsonPath + transform per rule, returns
// (record, hash, ruleMissCount, typeErrorCount).
func (t *TransmuterModule) buildMasterRow(
	ctx context.Context, rules []mappingRuleRow, row shadowBatchRow,
) (map[string]any, string, int64, int64) {
	rec := make(map[string]any, len(rules)+4)
	rawStr := string(row.RawData)

	var missCount, typeErrCount int64

	for _, r := range rules {
		path := r.SourceField
		if r.JsonPath != nil && *r.JsonPath != "" {
			path = *r.JsonPath
		} else if r.SourceFormat == "debezium_after" {
			path = "after." + r.SourceField
		}

		gres := gjson.Get(rawStr, path)
		if !gres.Exists() {
			if r.IsNullable {
				rec[r.TargetColumn] = nil
			} else if r.DefaultValue != nil {
				rec[r.TargetColumn] = *r.DefaultValue
			} else {
				missCount++
				// Non-nullable missing = skip whole row for safety.
				return nil, "", missCount, typeErrCount
			}
			continue
		}

		val := gjsonValueToGo(gres)
		if r.TransformFn != nil && *r.TransformFn != "" {
			converted, err := ApplyTransform(*r.TransformFn, val)
			if err != nil {
				t.logger.Warn("transform failed",
					zap.Int64("rule_id", r.ID),
					zap.String("fn", *r.TransformFn),
					zap.Error(err))
				typeErrCount++
				if r.IsNullable {
					rec[r.TargetColumn] = nil
					continue
				}
				return nil, "", missCount, typeErrCount
			}
			val = converted
		}

		if violation := t.typeRes.ValidateValue(ctx, r.DataType, val); violation != "" {
			t.logger.Warn("type violation",
				zap.Int64("rule_id", r.ID),
				zap.String("data_type", r.DataType),
				zap.String("violation", violation))
			typeErrCount++
			if r.IsNullable {
				rec[r.TargetColumn] = nil
				continue
			}
			return nil, "", missCount, typeErrCount
		}

		rec[r.TargetColumn] = val
	}

	hash := computeMasterHash(rec)
	return rec, hash, missCount, typeErrCount
}

// upsertMaster builds dynamic INSERT ... ON CONFLICT (_gpay_source_id)
// DO UPDATE SET ... WHERE target._hash IS DISTINCT FROM EXCLUDED._hash.
// Returns RowsAffected (1 = update/insert happened, 0 = identical row).
func (t *TransmuterModule) upsertMaster(
	ctx context.Context, master string, record map[string]any,
) (int64, error) {
	keys := sortedKeys(record)
	cols := make([]string, len(keys))
	placeholders := make([]string, len(keys))
	values := make([]any, len(keys))
	sets := make([]string, 0, len(keys))
	for i, k := range keys {
		cols[i] = quoteIdent(k)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		values[i] = sqlBindValueTransmute(record[k])
		if k == "_gpay_id" || k == "_gpay_source_id" {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(k), quoteIdent(k)))
	}

	sqlText := fmt.Sprintf(
		`INSERT INTO public.%s (%s) VALUES (%s)
ON CONFLICT (_gpay_source_id) DO UPDATE SET %s
WHERE public.%s._hash IS DISTINCT FROM EXCLUDED._hash`,
		quoteIdent(master),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(sets, ", "),
		quoteIdent(master),
	)

	res := t.db.WithContext(ctx).Exec(sqlText, values...)
	return res.RowsAffected, res.Error
}

func gjsonValueToGo(r gjson.Result) any {
	switch r.Type {
	case gjson.Null:
		return nil
	case gjson.False:
		return false
	case gjson.True:
		return true
	case gjson.Number:
		if r.Raw != "" && strings.ContainsAny(r.Raw, ".eE") {
			return r.Float()
		}
		return r.Int()
	case gjson.String:
		return r.String()
	case gjson.JSON:
		// Object or array — return raw so jsonb_passthrough can marshal.
		var v any
		_ = json.Unmarshal([]byte(r.Raw), &v)
		return v
	}
	return r.Value()
}

func computeMasterHash(rec map[string]any) string {
	keys := sortedKeys(rec)
	buf := make([]byte, 0, 256)
	for _, k := range keys {
		// Skip system columns from hash — they are derivatives / timestamps.
		switch k {
		case "_gpay_id", "_synced_at", "_source", "_version", "_hash":
			continue
		}
		b, _ := json.Marshal(rec[k])
		buf = append(buf, []byte(k)...)
		buf = append(buf, ':')
		buf = append(buf, b...)
		buf = append(buf, '|')
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

// sqlBindValueTransmute marshals maps/slices to JSON for JSONB columns —
// mirrors the SinkWorker helper but keeps the package self-contained.
func sqlBindValueTransmute(v any) any {
	switch v.(type) {
	case map[string]any, []any, []map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
	return v
}
