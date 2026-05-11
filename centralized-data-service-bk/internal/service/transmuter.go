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

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/pkgs/idgen"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MasterDestinationEnsurer interface {
	EnsureMaster(ctx context.Context, masterName string) error
}

type TransmuterModule struct {
	systemDB    *gorm.DB
	connMgr     *ConnectionManager
	runtimeRepo *repository.SyncRuntimeStateRepo
	ddlEnsurer  MasterDestinationEnsurer
	typeRes     *TypeResolver
	logger      *zap.Logger
	batchSize   int
	mu          sync.Mutex
	cache       map[string]cachedRules
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

type mappingRuleRow struct {
	ID              int64   `gorm:"column:id"`
	SourceObjectID  int64   `gorm:"column:source_object_id"`
	MasterBindingID *int64  `gorm:"column:master_binding_id"`
	SourceField     string  `gorm:"column:source_field"`
	TargetColumn    string  `gorm:"column:target_column"`
	DataType        string  `gorm:"column:data_type"`
	SourceFormat    string  `gorm:"column:source_format"`
	SourcePath      *string `gorm:"column:source_path"`
	TransformFn     *string `gorm:"column:transform_fn"`
	IsNullable      bool    `gorm:"column:is_nullable"`
	DefaultValue    *string `gorm:"column:default_value"`
}

type masterBindingRuntime struct {
	ID                  int64  `gorm:"column:id"`
	SourceObjectID      int64  `gorm:"column:source_object_id"`
	ShadowBindingID     *int64 `gorm:"column:shadow_binding_id"`
	MasterConnectionKey string `gorm:"column:master_connection_key"`
	MasterSchema        string `gorm:"column:master_schema"`
	MasterTable         string `gorm:"column:master_table"`
	PhysicalTableFQN    string `gorm:"column:physical_table_fqn"`
	TransformType       string `gorm:"column:transform_type"`
	IsActive            bool   `gorm:"column:is_active"`
	SchemaStatus        string `gorm:"column:schema_status"`
	ShadowConnectionKey string `gorm:"column:shadow_connection_key"`
	ShadowSchema        string `gorm:"column:shadow_schema"`
	ShadowTable         string `gorm:"column:shadow_table"`
	ShadowIsActive      bool   `gorm:"column:shadow_is_active"`
	SourceProfileStatus string `gorm:"column:source_profile_status"`
	SourcePK            string `gorm:"column:source_pk"`
	ShadowPK            string `gorm:"column:shadow_pk"`
}

type shadowBatchRow struct {
	GpayID      int64  `gorm:"column:_gpay_id"`
	SourceID    string `gorm:"column:_gpay_source_id"`
	RawData     []byte `gorm:"column:_raw_data"`
	SourceTs    int64  `gorm:"column:_source_ts"`
	GpayDeleted bool   `gorm:"column:_gpay_deleted"`
}

type batchOutcome struct {
	scanned, inserted, updated, skipped, ruleMisses, typeErrors, lastGpayID int64
}

type TransmuteResult struct {
	Master     string `json:"master"`
	Source     string `json:"source"`
	Scanned    int64  `json:"scanned"`
	Inserted   int64  `json:"inserted"`
	Updated    int64  `json:"updated"`
	Skipped    int64  `json:"skipped"`
	RuleMisses int64  `json:"rule_misses"`
	TypeErrors int64  `json:"type_errors"`
	DurationMs int64  `json:"duration_ms"`
	ActiveGate string `json:"active_gate,omitempty"`
}

func NewTransmuterModule(
	systemDB *gorm.DB,
	connMgr *ConnectionManager,
	runtimeRepo *repository.SyncRuntimeStateRepo,
	ddlEnsurer MasterDestinationEnsurer,
	typeRes *TypeResolver,
	logger *zap.Logger,
) *TransmuterModule {
	return &TransmuterModule{
		systemDB:    systemDB,
		connMgr:     connMgr,
		runtimeRepo: runtimeRepo,
		ddlEnsurer:  ddlEnsurer,
		typeRes:     typeRes,
		logger:      logger,
		batchSize:   500,
		cache:       make(map[string]cachedRules),
		shadowCache: make(map[string]shadowState),
		cacheTTL:    60 * time.Second,
	}
}

func (t *TransmuterModule) Run(ctx context.Context, masterName string, onlySourceIDs []string) (TransmuteResult, error) {
	start := time.Now()
	res := TransmuteResult{Master: masterName}

	masterRow, err := t.loadMaster(ctx, masterName)
	if err != nil {
		t.markRuntimeFailure(ctx, 0, err)
		return res, fmt.Errorf("master lookup: %w", err)
	}
	if !masterRow.IsActive || masterRow.SchemaStatus != "approved" {
		res.ActiveGate = fmt.Sprintf("master gate: is_active=%v schema_status=%s", masterRow.IsActive, masterRow.SchemaStatus)
		t.markRuntimeSkipped(ctx, masterRow.ID, res.ActiveGate)
		return res, nil
	}
	res.Source = masterRow.ShadowTable

	if ok, reason := t.shadowActive(masterRow); !ok {
		res.ActiveGate = fmt.Sprintf("shadow gate: %s", reason)
		t.markRuntimeSkipped(ctx, masterRow.ID, res.ActiveGate)
		return res, nil
	}
	if t.ddlEnsurer != nil {
		if err := t.ddlEnsurer.EnsureMaster(ctx, masterRow.MasterTable); err != nil {
			t.markRuntimeFailure(ctx, masterRow.ID, err)
			return res, fmt.Errorf("ensure master destination: %w", err)
		}
	}

	rules, err := t.loadRules(ctx, masterRow)
	if err != nil {
		t.markRuntimeFailure(ctx, masterRow.ID, err)
		return res, fmt.Errorf("rule load: %w", err)
	}
	if len(rules) == 0 {
		res.DurationMs = time.Since(start).Milliseconds()
		t.markRuntimeSuccess(ctx, masterRow.ID, res)
		return res, nil
	}

	var lastGpayID int64
	for {
		shadowRows, err := t.fetchShadowBatch(ctx, masterRow, lastGpayID, onlySourceIDs)
		if err != nil {
			t.markRuntimeFailure(ctx, masterRow.ID, err)
			return res, fmt.Errorf("fetch shadow batch: %w", err)
		}
		if len(shadowRows) == 0 {
			break
		}
		batchRes := t.processBatch(ctx, masterRow, rules, shadowRows)
		res.Scanned += batchRes.scanned
		res.Inserted += batchRes.inserted
		res.Updated += batchRes.updated
		res.Skipped += batchRes.skipped
		res.RuleMisses += batchRes.ruleMisses
		res.TypeErrors += batchRes.typeErrors
		lastGpayID = batchRes.lastGpayID
		if len(onlySourceIDs) > 0 {
			break
		}
	}
	res.DurationMs = time.Since(start).Milliseconds()
	t.markRuntimeSuccess(ctx, masterRow.ID, res)
	return res, nil
}

func (t *TransmuterModule) loadMaster(ctx context.Context, name string) (*masterBindingRuntime, error) {
	var row masterBindingRuntime
	err := t.systemDB.WithContext(ctx).Raw(`
		SELECT mb.id,
		       mb.source_object_id,
		       mb.shadow_binding_id,
		       COALESCE(mc.connection_code, 'default') AS master_connection_key,
		       mb.master_schema,
		       mb.master_table,
		       mb.physical_table_fqn,
		       mb.transform_type,
		       mb.is_active,
		       mb.schema_status,
		       COALESCE(sc.connection_code, 'default') AS shadow_connection_key,
		       sb.shadow_schema,
		       sb.shadow_table,
		       sb.is_active AS shadow_is_active,
		       sor.profile_status AS source_profile_status,
		       COALESCE(sor.primary_key_field, 'id') AS source_pk,
		       CASE 
		         WHEN EXISTS (
		           SELECT 1 FROM information_schema.columns 
		           WHERE table_schema = sb.shadow_schema 
		             AND table_name = sb.shadow_table 
		             AND column_name = '_gpay_id'
		         ) THEN '_gpay_id'
		         ELSE COALESCE(sor.primary_key_field, 'id')
		       END AS shadow_pk
		  FROM cdc_system.master_binding mb
		  LEFT JOIN cdc_system.connection_registry mc ON mc.id = mb.master_connection_id
		  LEFT JOIN cdc_system.shadow_binding sb ON sb.id = mb.shadow_binding_id
		  LEFT JOIN cdc_system.connection_registry sc ON sc.id = sb.shadow_connection_id
		  LEFT JOIN cdc_system.source_object_registry sor ON sor.id = mb.source_object_id
		 WHERE mb.master_table = ?
		 LIMIT 1`, name).Scan(&row).Error
	if err != nil {
		return nil, err
	}
	if row.MasterTable == "" {
		return nil, fmt.Errorf("master %q not registered", name)
	}
	return &row, nil
}

func (t *TransmuterModule) shadowActive(row *masterBindingRuntime) (bool, string) {
	if row == nil {
		return false, "missing master row"
	}
	cacheKey := row.ShadowConnectionKey + "|" + row.ShadowSchema + "|" + row.ShadowTable
	t.mu.Lock()
	if e, ok := t.shadowCache[cacheKey]; ok && time.Since(e.loadedAt) < t.cacheTTL {
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

	state := shadowState{
		isActive:      row.ShadowIsActive,
		profileStatus: row.SourceProfileStatus,
		loadedAt:      time.Now(),
	}
	t.mu.Lock()
	t.shadowCache[cacheKey] = state
	t.mu.Unlock()
	if !state.isActive {
		return false, "is_active=false"
	}
	if state.profileStatus != "active" {
		return false, fmt.Sprintf("profile_status=%s", state.profileStatus)
	}
	return true, ""
}

func (t *TransmuterModule) loadRules(ctx context.Context, row *masterBindingRuntime) ([]mappingRuleRow, error) {
	cacheKey := fmt.Sprintf("%d|%s", row.ID, row.MasterTable)
	t.mu.Lock()
	if e, ok := t.cache[cacheKey]; ok && time.Since(e.loadedAt) < t.cacheTTL {
		defer t.mu.Unlock()
		return e.rules, nil
	}
	t.mu.Unlock()

	var rules []mappingRuleRow
	err := t.systemDB.WithContext(ctx).Raw(`
		SELECT id, source_object_id, master_binding_id, source_field, target_column,
		       data_type, source_format, source_path, transform_fn, is_nullable, default_value
		  FROM cdc_system.mapping_rule_v2
		 WHERE source_object_id = ?
		   AND (master_binding_id = ? OR master_binding_id IS NULL)
		   AND is_active = true
		   AND status = 'approved'
		 ORDER BY id`, row.SourceObjectID, row.ID).Scan(&rules).Error
	if err != nil {
		return nil, err
	}
	valid := rules[:0]
	for _, r := range rules {
		if r.TransformFn != nil && !IsTransformWhitelisted(*r.TransformFn) {
			continue
		}
		if !t.typeRes.Validate(r.DataType) {
			continue
		}
		valid = append(valid, r)
	}
	t.mu.Lock()
	t.cache[cacheKey] = cachedRules{rules: valid, loadedAt: time.Now()}
	t.mu.Unlock()
	return valid, nil
}

func (t *TransmuterModule) fetchShadowBatch(ctx context.Context, row *masterBindingRuntime, cursor int64, onlyIDs []string) ([]shadowBatchRow, error) {
	shadowDB, err := t.connMgr.GetShadowDB(ctx, row.ShadowConnectionKey)
	if err != nil {
		return nil, err
	}
	var rows []shadowBatchRow
	// PK dynamic handling: if ShadowPK is not _gpay_id, we must cast to BIGINT for cursor comparison
	pkIdent := quoteTransmuteIdent(row.ShadowPK)
	pkSelect := pkIdent
	if row.ShadowPK != "_gpay_id" {
		pkSelect = fmt.Sprintf("%s::bigint AS _gpay_id", pkIdent)
	}

	qt := fmt.Sprintf(`SELECT %s, _gpay_source_id, _raw_data, _source_ts, _gpay_deleted
		FROM %s
		WHERE %s > ?`, pkSelect, quoteTransmuteQualified(row.ShadowSchema, row.ShadowTable), 
		fmt.Sprintf("(%s)::bigint", pkIdent))

	args := []any{cursor}
	if len(onlyIDs) > 0 {
		qt += ` AND _gpay_source_id = ANY(?)`
		args = append(args, onlyIDs)
	}
	qt += ` ORDER BY 1 LIMIT ?`
	args = append(args, t.batchSize)
	err = shadowDB.WithContext(ctx).Raw(qt, args...).Scan(&rows).Error
	return rows, err
}

func (t *TransmuterModule) processBatch(ctx context.Context, binding *masterBindingRuntime, rules []mappingRuleRow, rows []shadowBatchRow) batchOutcome {
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
		if sfID, err := idgen.NextID(); err == nil {
			record["_gpay_id"] = int64(sfID)
		} else {
			// Fallback (though Sonyflake should not fail if initialized)
			record["_gpay_id"] = row.GpayID
		}
		record["_gpay_source_id"] = row.SourceID
		record["_source"] = "debezium-transmute"
		record["_source_ts"] = row.SourceTs
		record["_synced_at"] = time.Now().UTC()
		record["_hash"] = hash
		record["_gpay_deleted"] = row.GpayDeleted
		record["_version"] = int64(1)

		upd, err := t.upsertMaster(ctx, binding, record)
		if err != nil {
			t.logger.Error("master upsert failed", zap.String("master", binding.MasterTable), zap.String("source_id", row.SourceID), zap.Error(err))
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

func (t *TransmuterModule) buildMasterRow(ctx context.Context, rules []mappingRuleRow, row shadowBatchRow) (map[string]any, string, int64, int64) {
	rec := make(map[string]any, len(rules)+4)
	rawStr := string(row.RawData)
	var missCount, typeErrCount int64
	for _, r := range rules {
		path := r.SourceField
		if r.SourcePath != nil && *r.SourcePath != "" {
			path = *r.SourcePath
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
				return nil, "", missCount, typeErrCount
			}
			continue
		}
		val := gjsonValueToGo(gres)
		if r.TransformFn != nil && *r.TransformFn != "" {
			converted, err := ApplyTransform(*r.TransformFn, val)
			if err != nil {
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

func (t *TransmuterModule) upsertMaster(ctx context.Context, binding *masterBindingRuntime, record map[string]any) (int64, error) {
	masterDB, err := t.connMgr.GetMasterDB(ctx, binding.MasterConnectionKey)
	if err != nil {
		return 0, err
	}
	keys := sortedKeysAny(record)
	cols := make([]string, len(keys))
	placeholders := make([]string, len(keys))
	values := make([]any, len(keys))
	sets := make([]string, 0, len(keys))
	for i, k := range keys {
		cols[i] = quoteTransmuteIdent(k)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		values[i] = sqlBindValueTransmute(record[k])
		if k == "_gpay_id" || k == "_gpay_source_id" {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quoteTransmuteIdent(k), quoteTransmuteIdent(k)))
	}
	qt := quoteTransmuteQualified(binding.MasterSchema, binding.MasterTable)
	sqlText := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)
ON CONFLICT (_gpay_source_id) DO UPDATE SET %s
WHERE %s._hash IS DISTINCT FROM EXCLUDED._hash`,
		qt,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(sets, ", "),
		qt,
	)
	res := masterDB.WithContext(ctx).Exec(sqlText, values...)
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
		var v any
		_ = json.Unmarshal([]byte(r.Raw), &v)
		return v
	}
	return r.Value()
}

func computeMasterHash(rec map[string]any) string {
	keys := sortedKeysAny(rec)
	buf := make([]byte, 0, 256)
	for _, k := range keys {
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

func sortedKeysAny(m map[string]any) []string {
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

func quoteTransmuteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func quoteTransmuteQualified(schemaName, tableName string) string {
	return quoteTransmuteIdent(schemaName) + "." + quoteTransmuteIdent(tableName)
}

func (t *TransmuterModule) markRuntimeSuccess(ctx context.Context, masterBindingID int64, res TransmuteResult) {
	stats, _ := json.Marshal(map[string]any{
		"master":      res.Master,
		"source":      res.Source,
		"scanned":     res.Scanned,
		"inserted":    res.Inserted,
		"updated":     res.Updated,
		"skipped":     res.Skipped,
		"rule_misses": res.RuleMisses,
		"type_errors": res.TypeErrors,
		"duration_ms": res.DurationMs,
		"active_gate": res.ActiveGate,
	})
	t.persistRuntimeState(ctx, masterBindingID, func(item *model.SyncRuntimeState, now time.Time) {
		item.LastSuccessAt = &now
		item.LastErrorAt = nil
		item.LastErrorMessage = nil
		item.StatsJSON = stats
	})
}

func (t *TransmuterModule) markRuntimeFailure(ctx context.Context, masterBindingID int64, opErr error) {
	if opErr == nil {
		return
	}
	msg := SanitizeFreeformText(opErr.Error(), 2000)
	t.persistRuntimeState(ctx, masterBindingID, func(item *model.SyncRuntimeState, now time.Time) {
		item.LastErrorAt = &now
		item.LastErrorMessage = &msg
	})
}

func (t *TransmuterModule) markRuntimeSkipped(ctx context.Context, masterBindingID int64, reason string) {
	stats, _ := json.Marshal(map[string]any{
		"active_gate": reason,
		"status":      "skipped",
	})
	t.persistRuntimeState(ctx, masterBindingID, func(item *model.SyncRuntimeState, now time.Time) {
		item.LastSuccessAt = &now
		item.LastErrorAt = nil
		item.LastErrorMessage = nil
		item.StatsJSON = stats
	})
}

func (t *TransmuterModule) persistRuntimeState(ctx context.Context, masterBindingID int64, mutate func(*model.SyncRuntimeState, time.Time)) {
	if t.runtimeRepo == nil || masterBindingID == 0 {
		return
	}
	item, err := t.runtimeRepo.GetByMasterBinding(ctx, masterBindingID)
	if err != nil && err != gorm.ErrRecordNotFound {
		t.logger.Warn("transmuter runtime lookup failed",
			zap.Int64("master_binding_id", masterBindingID),
			zap.Error(err))
		return
	}
	now := time.Now().UTC()
	if err == gorm.ErrRecordNotFound || item == nil || item.ID == 0 {
		item = &model.SyncRuntimeState{
			MasterBindingID: &masterBindingID,
			RuntimeScope:    "master",
			StatsJSON:       []byte(`{}`),
		}
	}
	item.UpdatedAt = now
	mutate(item, now)
	if item.ID == 0 {
		if createErr := t.runtimeRepo.Create(ctx, item); createErr != nil {
			t.logger.Warn("transmuter runtime create failed",
				zap.Int64("master_binding_id", masterBindingID),
				zap.Error(createErr))
		}
		return
	}
	if updateErr := t.runtimeRepo.Update(ctx, item); updateErr != nil {
		t.logger.Warn("transmuter runtime update failed",
			zap.Int64("master_binding_id", masterBindingID),
			zap.Error(updateErr))
	}
}
