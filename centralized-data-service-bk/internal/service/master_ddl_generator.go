package service

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MasterDDLGenerator struct {
	systemDB    *gorm.DB
	connMgr     *ConnectionManager
	mappingRepo *repository.MappingRuleV2Repo
	runtimeRepo *repository.SyncRuntimeStateRepo
	logger      *zap.Logger
}

type masterDDLBindingRow struct {
	ID                  int64  `gorm:"column:id"`
	SourceObjectID      int64  `gorm:"column:source_object_id"`
	MasterConnectionKey string `gorm:"column:master_connection_key"`
	MasterSchema        string `gorm:"column:master_schema"`
	MasterTable         string `gorm:"column:master_table"`
	SchemaStatus        string `gorm:"column:schema_status"`
	IsActive            bool   `gorm:"column:is_active"`
}

func NewMasterDDLGenerator(systemDB *gorm.DB, connMgr *ConnectionManager, mappingRepo *repository.MappingRuleV2Repo, runtimeRepo *repository.SyncRuntimeStateRepo, logger *zap.Logger) *MasterDDLGenerator {
	return &MasterDDLGenerator{
		systemDB:    systemDB,
		connMgr:     connMgr,
		mappingRepo: mappingRepo,
		runtimeRepo: runtimeRepo,
		logger:      logger,
	}
}

var financialIndexRe = regexp.MustCompile(`(?i)^(amount|fee|balance|total|price|refund|subtotal|discount|tax|cost)[_a-z0-9]*$|_amount$|_fee$|_balance$|_price$`)
var ddlIdentRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type MasterDDLResult struct {
	MasterName    string   `json:"master_name"`
	MasterSchema  string   `json:"master_schema"`
	CreateSQL     string   `json:"create_sql"`
	AlterSQL      []string `json:"alter_sql,omitempty"`
	IndexSQL      []string `json:"index_sql"`
	RLSApplied    bool     `json:"rls_applied"`
	RuleCount     int      `json:"rule_count"`
	FinancialCols []string `json:"financial_cols"`
	Err           string   `json:"error,omitempty"`
}

func (g *MasterDDLGenerator) Generate(ctx context.Context, masterName string) (*MasterDDLResult, error) {
	if !ddlIdentRe.MatchString(masterName) {
		return nil, fmt.Errorf("invalid master_name: %q", masterName)
	}
	reg, err := g.loadBinding(ctx, masterName)
	if err != nil {
		return nil, err
	}
	if reg.SchemaStatus != "approved" {
		return nil, fmt.Errorf("master %q schema_status=%s — must be approved", masterName, reg.SchemaStatus)
	}
	rules, err := g.mappingRepo.ListActiveByMasterBinding(ctx, reg.ID)
	if err != nil {
		return nil, fmt.Errorf("rules load: %w", err)
	}
	if len(rules) == 0 {
		rules, err = g.mappingRepo.ListActiveBySourceObject(ctx, reg.SourceObjectID)
		if err != nil {
			return nil, fmt.Errorf("source rules load: %w", err)
		}
	}
	res := &MasterDDLResult{
		MasterName:   reg.MasterTable,
		MasterSchema: reg.MasterSchema,
		RuleCount:    len(rules),
	}
	cols := []string{
		`"_gpay_id" BIGINT PRIMARY KEY`,
		`"_gpay_source_id" TEXT NOT NULL`,
		`"_raw_data" JSONB`,
		`"_source" TEXT NOT NULL`,
		`"_source_ts" BIGINT`,
		`"_synced_at" TIMESTAMPTZ NOT NULL`,
		`"_version" BIGINT NOT NULL DEFAULT 1`,
		`"_hash" TEXT NOT NULL`,
		`"_gpay_deleted" BOOLEAN NOT NULL DEFAULT FALSE`,
		`"_created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`"_updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
	}
	seen := map[string]bool{"_gpay_id": true, "_gpay_source_id": true, "_raw_data": true, "_source": true, "_source_ts": true, "_synced_at": true, "_version": true, "_hash": true, "_gpay_deleted": true, "_created_at": true, "_updated_at": true}
	var financialCols []string
	for _, r := range rules {
		if !ddlIdentRe.MatchString(r.TargetColumn) || seen[r.TargetColumn] {
			continue
		}
		seen[r.TargetColumn] = true
		parts := []string{fmt.Sprintf(`%s %s`, quoteDDLIdent(r.TargetColumn), r.DataType)}
		if !r.IsNullable {
			parts = append(parts, "NOT NULL")
		}
		if r.DefaultValue != nil && *r.DefaultValue != "" {
			parts = append(parts, fmt.Sprintf("DEFAULT %s", quoteDefaultValue(*r.DefaultValue, r.DataType)))
		}
		cols = append(cols, strings.Join(parts, " "))
		if financialIndexRe.MatchString(r.TargetColumn) {
			financialCols = append(financialCols, r.TargetColumn)
		}
	}
	sort.Strings(financialCols)
	res.FinancialCols = financialCols
	res.CreateSQL = fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s;\nCREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quoteDDLIdent(reg.MasterSchema),
		quoteDDLQualified(reg.MasterSchema, reg.MasterTable),
		strings.Join(cols, ",\n  "),
	)
	// V2 rules can land AFTER the master table was first CREATEd (e.g.
	// when discover bridges V2 only after master_bind already ran the
	// initial empty CREATE). Emit additive ALTERs so a re-Apply lifts the
	// rule columns onto the existing master without recreating it.
	for _, r := range rules {
		if !ddlIdentRe.MatchString(r.TargetColumn) {
			continue
		}
		switch r.TargetColumn {
		case "_gpay_id", "_gpay_source_id", "_raw_data", "_source", "_source_ts",
			"_synced_at", "_version", "_hash", "_gpay_deleted", "_created_at", "_updated_at":
			continue
		}
		res.AlterSQL = append(res.AlterSQL, fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;`,
			quoteDDLQualified(reg.MasterSchema, reg.MasterTable),
			quoteDDLIdent(r.TargetColumn),
			r.DataType,
		))
	}
	idx := []string{
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (_gpay_source_id);`, quoteDDLIdent("ux_"+reg.MasterTable+"_source_id"), quoteDDLQualified(reg.MasterSchema, reg.MasterTable)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (_created_at);`, quoteDDLIdent("ix_"+reg.MasterTable+"_created_at"), quoteDDLQualified(reg.MasterSchema, reg.MasterTable)),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (_updated_at);`, quoteDDLIdent("ix_"+reg.MasterTable+"_updated_at"), quoteDDLQualified(reg.MasterSchema, reg.MasterTable)),
	}
	for _, fc := range financialCols {
		idx = append(idx, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s);`, quoteDDLIdent("ix_"+reg.MasterTable+"_"+fc), quoteDDLQualified(reg.MasterSchema, reg.MasterTable), quoteDDLIdent(fc)))
	}
	res.IndexSQL = idx
	return res, nil
}

func (g *MasterDDLGenerator) Apply(ctx context.Context, masterName string) (*MasterDDLResult, error) {
	reg, err := g.loadBinding(ctx, masterName)
	if err != nil {
		return nil, err
	}
	res, err := g.Generate(ctx, masterName)
	if err != nil {
		g.markDDLStatus(ctx, reg.ID, "failed", err)
		return nil, err
	}
	db, err := g.connMgr.GetMasterDB(ctx, reg.MasterConnectionKey)
	if err != nil {
		g.markDDLStatus(ctx, reg.ID, "failed", err)
		return nil, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(res.CreateSQL).Error; err != nil {
			return fmt.Errorf("create table: %w", err)
		}
		for _, s := range res.AlterSQL {
			if err := tx.Exec(s).Error; err != nil {
				return fmt.Errorf("alter add column: %w", err)
			}
		}
		for _, s := range res.IndexSQL {
			if err := tx.Exec(s).Error; err != nil {
				return fmt.Errorf("create index: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		res.Err = err.Error()
		g.markDDLStatus(ctx, reg.ID, "failed", err)
		return res, err
	}
	if reg.MasterSchema == "public" {
		if err := db.WithContext(ctx).Exec(`SELECT cdc_system.enable_master_rls(?)`, reg.MasterTable).Error; err == nil {
			res.RLSApplied = true
		}
	}
	g.markDDLStatus(ctx, reg.ID, "created", nil)
	return res, nil
}

func (g *MasterDDLGenerator) EnsureMaster(ctx context.Context, masterName string) error {
	_, err := g.Apply(ctx, masterName)
	return err
}

func (g *MasterDDLGenerator) loadBinding(ctx context.Context, masterName string) (*masterDDLBindingRow, error) {
	var row masterDDLBindingRow
	err := g.systemDB.WithContext(ctx).Raw(`
		SELECT mb.id,
		       mb.source_object_id,
		       COALESCE(cr.connection_code, 'default') AS master_connection_key,
		       mb.master_schema,
		       mb.master_table,
		       mb.schema_status,
		       mb.is_active
		  FROM cdc_system.master_binding mb
		  LEFT JOIN cdc_system.connection_registry cr ON cr.id = mb.master_connection_id
		 WHERE mb.master_table = ?
		 LIMIT 1`, masterName).Scan(&row).Error
	if err != nil {
		return nil, fmt.Errorf("registry lookup: %w", err)
	}
	if row.MasterTable == "" {
		return nil, fmt.Errorf("master %q not registered", masterName)
	}
	return &row, nil
}

func (g *MasterDDLGenerator) markDDLStatus(ctx context.Context, masterBindingID int64, status string, opErr error) {
	if g.runtimeRepo == nil {
		return
	}
	item, err := g.runtimeRepo.GetByMasterBinding(ctx, masterBindingID)
	if err != nil && err != gorm.ErrRecordNotFound {
		g.logger.Warn("master ddl runtime lookup failed",
			zap.Int64("master_binding_id", masterBindingID),
			zap.Error(err))
		return
	}
	current := time.Now().UTC()
	if err == gorm.ErrRecordNotFound || item == nil || item.ID == 0 {
		item = &model.SyncRuntimeState{
			MasterBindingID: &masterBindingID,
			RuntimeScope:    "master",
			StatsJSON:       []byte(`{}`),
		}
	}
	item.DDLStatus = &status
	item.UpdatedAt = current
	if opErr != nil {
		item.LastErrorAt = &current
		msg := SanitizeFreeformText(opErr.Error(), 2000)
		item.LastErrorMessage = &msg
	} else {
		item.LastSuccessAt = &current
		item.LastErrorAt = nil
		item.LastErrorMessage = nil
	}
	if item.ID == 0 {
		if createErr := g.runtimeRepo.Create(ctx, item); createErr != nil {
			g.logger.Warn("master ddl runtime create failed",
				zap.Int64("master_binding_id", masterBindingID),
				zap.Error(createErr))
		}
		return
	}
	if updateErr := g.runtimeRepo.Update(ctx, item); updateErr != nil {
		g.logger.Warn("master ddl runtime update failed",
			zap.Int64("master_binding_id", masterBindingID),
			zap.Error(updateErr))
	}
}

func quoteDefaultValue(v, dataType string) string {
	trimmed := strings.TrimSpace(v)
	if strings.Contains(trimmed, "(") && strings.Contains(trimmed, ")") {
		return trimmed
	}
	upper := strings.ToUpper(dataType)
	if strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DECIMAL") ||
		strings.HasPrefix(upper, "INT") || strings.HasPrefix(upper, "BIGINT") ||
		strings.HasPrefix(upper, "SMALLINT") || strings.HasPrefix(upper, "REAL") ||
		strings.HasPrefix(upper, "DOUBLE") || strings.HasPrefix(upper, "BOOLEAN") {
		return trimmed
	}
	return "'" + strings.ReplaceAll(trimmed, "'", "''") + "'"
}

func quoteDDLIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func quoteDDLQualified(schemaName, tableName string) string {
	return quoteDDLIdent(schemaName) + "." + quoteDDLIdent(tableName)
}
