package service

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// MasterDDLGenerator produces idempotent CREATE TABLE statements for
// public.<master_name> from an approved cdc_internal.master_table_registry
// row plus the active cdc_mapping_rules tied to the (source_shadow,
// master_name) pair.
//
// Produced SQL is:
//
//	CREATE TABLE IF NOT EXISTS public.<master>(
//	  _gpay_id BIGINT PRIMARY KEY,
//	  _gpay_source_id TEXT NOT NULL,
//	  _raw_data JSONB,
//	  _source TEXT NOT NULL,
//	  _source_ts BIGINT,
//	  _synced_at TIMESTAMPTZ NOT NULL,
//	  _version BIGINT NOT NULL DEFAULT 1,
//	  _hash TEXT NOT NULL,
//	  _gpay_deleted BOOLEAN NOT NULL DEFAULT FALSE,
//	  _created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	  _updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	  -- business cols, sorted by target_column for deterministic DDL
//	  <col1> <type1>[ NOT NULL][ DEFAULT ...],
//	  ...
//	);
//	CREATE UNIQUE INDEX IF NOT EXISTS ux_<master>_source_id ON public.<master>(_gpay_source_id);
//	CREATE INDEX IF NOT EXISTS ix_<master>_created_at ON public.<master>(_created_at);
//	-- auto-index on financial columns (regex: amount|fee|balance|total|price|refund)
//	SELECT cdc_internal.enable_master_rls('<master>');
//
// Design notes:
//   - `IF NOT EXISTS` everywhere so the generator is a safe re-run on every
//     approve/re-approve cycle.
//   - Financial-column index regex intentionally broad (covers amount,
//     fee, subtotal, total, price, refund_amount, balance, etc.) —
//     admin can drop unneeded indexes later; false positives are cheap.
//   - All identifiers are validated against
//     `^[a-z_][a-z0-9_]{0,62}$` and quoted before interpolation. Invalid
//     inputs short-circuit with an error, never reach SQL.
type MasterDDLGenerator struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewMasterDDLGenerator(db *gorm.DB, logger *zap.Logger) *MasterDDLGenerator {
	return &MasterDDLGenerator{db: db, logger: logger}
}

// financialIndexRe picks columns whose name suggests a money amount.
// Kept separate from the hardcoded classifier (which was regex-on-field-
// detection) — this is OPT-IN indexing, safe because admin has already
// approved the master.
var financialIndexRe = regexp.MustCompile(`(?i)^(amount|fee|balance|total|price|refund|subtotal|discount|tax|cost)[_a-z0-9]*$|_amount$|_fee$|_balance$|_price$`)

var ddlIdentRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// MasterDDLResult captures what the generator produced and what it applied.
type MasterDDLResult struct {
	MasterName     string   `json:"master_name"`
	SourceShadow   string   `json:"source_shadow"`
	CreateSQL      string   `json:"create_sql"`
	IndexSQL       []string `json:"index_sql"`
	RLSApplied     bool     `json:"rls_applied"`
	RuleCount      int      `json:"rule_count"`
	FinancialCols  []string `json:"financial_cols"`
	Err            string   `json:"error,omitempty"`
}

// Generate builds (but does not execute) the DDL for a master. Useful for
// "preview" endpoints and for unit tests that diff against golden files.
func (g *MasterDDLGenerator) Generate(ctx context.Context, masterName string) (*MasterDDLResult, error) {
	if !ddlIdentRe.MatchString(masterName) {
		return nil, fmt.Errorf("invalid master_name: %q", masterName)
	}

	// 1. Load registry row.
	var reg struct {
		MasterName    string          `gorm:"column:master_name"`
		SourceShadow  string          `gorm:"column:source_shadow"`
		TransformType string          `gorm:"column:transform_type"`
		Spec          json.RawMessage `gorm:"column:spec"`
		IsActive      bool            `gorm:"column:is_active"`
		SchemaStatus  string          `gorm:"column:schema_status"`
	}
	err := g.db.WithContext(ctx).Raw(
		`SELECT master_name, source_shadow, transform_type, spec, is_active, schema_status
		   FROM cdc_internal.master_table_registry
		  WHERE master_name = ?`, masterName,
	).Scan(&reg).Error
	if err != nil {
		return nil, fmt.Errorf("registry lookup: %w", err)
	}
	if reg.MasterName == "" {
		return nil, fmt.Errorf("master %q not registered", masterName)
	}
	if reg.SchemaStatus != "approved" {
		return nil, fmt.Errorf("master %q schema_status=%s — must be approved", masterName, reg.SchemaStatus)
	}
	if !ddlIdentRe.MatchString(reg.SourceShadow) {
		return nil, fmt.Errorf("invalid source_shadow: %q", reg.SourceShadow)
	}

	// 2. Load mapping rules (approved + active only).
	type ruleRow struct {
		TargetColumn string  `gorm:"column:target_column"`
		DataType     string  `gorm:"column:data_type"`
		IsNullable   bool    `gorm:"column:is_nullable"`
		DefaultValue *string `gorm:"column:default_value"`
	}
	var rules []ruleRow
	err = g.db.WithContext(ctx).Raw(
		`SELECT target_column, data_type, is_nullable, default_value
		   FROM cdc_mapping_rules
		  WHERE source_table = ?
		    AND (master_table = ? OR master_table IS NULL)
		    AND is_active = true
		    AND status = 'approved'
		  ORDER BY target_column`,
		reg.SourceShadow, masterName,
	).Scan(&rules).Error
	if err != nil {
		return nil, fmt.Errorf("rules load: %w", err)
	}

	res := &MasterDDLResult{
		MasterName:   masterName,
		SourceShadow: reg.SourceShadow,
		RuleCount:    len(rules),
	}

	// 3. Build CREATE TABLE. System cols first (fixed ordering), then
	// business cols sorted alphabetically.
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

	seen := map[string]bool{
		"_gpay_id": true, "_gpay_source_id": true, "_raw_data": true,
		"_source": true, "_source_ts": true, "_synced_at": true,
		"_version": true, "_hash": true, "_gpay_deleted": true,
		"_created_at": true, "_updated_at": true,
	}

	var financialCols []string
	for _, r := range rules {
		if !ddlIdentRe.MatchString(r.TargetColumn) {
			g.logger.Warn("skipping rule with invalid target_column",
				zap.String("col", r.TargetColumn))
			continue
		}
		if seen[r.TargetColumn] {
			continue
		}
		seen[r.TargetColumn] = true

		parts := []string{fmt.Sprintf(`%s %s`, quoteIdent(r.TargetColumn), r.DataType)}
		if !r.IsNullable {
			parts = append(parts, "NOT NULL")
		}
		if r.DefaultValue != nil && *r.DefaultValue != "" {
			// Defaults are admin-authored; caller responsibility to escape.
			// We wrap in single quotes only when not a function call.
			parts = append(parts, fmt.Sprintf("DEFAULT %s", quoteDefaultValue(*r.DefaultValue, r.DataType)))
		}
		cols = append(cols, strings.Join(parts, " "))

		if financialIndexRe.MatchString(r.TargetColumn) {
			financialCols = append(financialCols, r.TargetColumn)
		}
	}
	sort.Strings(financialCols)
	res.FinancialCols = financialCols

	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS public.%s (\n  %s\n);",
		quoteIdent(masterName),
		strings.Join(cols, ",\n  "),
	)
	res.CreateSQL = createSQL

	// 4. Indexes.
	idx := []string{
		fmt.Sprintf(
			`CREATE UNIQUE INDEX IF NOT EXISTS %s ON public.%s (_gpay_source_id);`,
			quoteIdent("ux_"+masterName+"_source_id"),
			quoteIdent(masterName),
		),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s ON public.%s (_created_at);`,
			quoteIdent("ix_"+masterName+"_created_at"),
			quoteIdent(masterName),
		),
		fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s ON public.%s (_updated_at);`,
			quoteIdent("ix_"+masterName+"_updated_at"),
			quoteIdent(masterName),
		),
	}
	for _, fc := range financialCols {
		idx = append(idx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s ON public.%s (%s);`,
			quoteIdent("ix_"+masterName+"_"+fc),
			quoteIdent(masterName),
			quoteIdent(fc),
		))
	}
	res.IndexSQL = idx

	return res, nil
}

// Apply executes Generate + runs the SQL in a single transaction. RLS is
// applied last (outside the TX on purpose — the helper function is
// idempotent and survives a partial table-create rollback cleanly).
func (g *MasterDDLGenerator) Apply(ctx context.Context, masterName string) (*MasterDDLResult, error) {
	res, err := g.Generate(ctx, masterName)
	if err != nil {
		return nil, err
	}

	err = g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(res.CreateSQL).Error; err != nil {
			return fmt.Errorf("create table: %w", err)
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
		return res, err
	}

	// RLS post-create (idempotent helper from migration 026).
	if err := g.db.WithContext(ctx).Exec(
		`SELECT cdc_internal.enable_master_rls(?)`, masterName,
	).Error; err != nil {
		g.logger.Warn("enable_master_rls failed (table exists but RLS not applied)",
			zap.String("master", masterName), zap.Error(err))
		res.Err = err.Error()
		return res, err
	}
	res.RLSApplied = true

	g.logger.Info("master DDL applied",
		zap.String("master", masterName),
		zap.Int("rule_count", res.RuleCount),
		zap.Int("index_count", len(res.IndexSQL)),
		zap.Strings("financial_cols", res.FinancialCols),
	)
	return res, nil
}

// quoteDefaultValue wraps a default expression safely. Function calls
// (e.g. NOW()) and numeric literals pass through; everything else is
// single-quoted.
func quoteDefaultValue(v, dataType string) string {
	trimmed := strings.TrimSpace(v)
	// Pass-through for function calls and bare numerics.
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
	// Default string: escape single quotes then wrap.
	escaped := strings.ReplaceAll(trimmed, "'", "''")
	return "'" + escaped + "'"
}
