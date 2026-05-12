package persistence

import (
	"context"
	"fmt"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ShadowAutomator owns synchronous shadow-table provisioning for the
// Systematic Flow. Replaces the fire-and-forget NATS path used by the
// legacy Register flow with a Register-time call: CMS returns 202 only
// after DDL + trigger + registry flag are set.
//
// Phase 39 (Option A): the automator is schema-aware. Caller resolves
// shadowSchema (= "shadow_<source_db>") before invoking. Sonyflake
// helpers live in cdc_system via migration 028 — no inline bootstrap.
type ShadowAutomator struct {
	shadowDB  *gorm.DB // DDL + trigger (cdc_shadow)
	controlDB *gorm.DB // markCreated → cdc_table_registry (cdc_dw)
	logger    *zap.Logger
}

// NewShadowAutomator constructs the automator.
// shadowDB: kết nối tới cdc_shadow — dùng cho DDL + trigger.
// controlDB: kết nối tới cdc_dw — dùng cho markCreated (cdc_table_registry).
func NewShadowAutomator(shadowDB, controlDB *gorm.DB, logger *zap.Logger) *ShadowAutomator {
	return &ShadowAutomator{shadowDB: shadowDB, controlDB: controlDB, logger: logger}
}

// EnsureShadowTable creates <shadowSchema>.<target> + attaches sonyflake
// trigger via cdc_system.ensure_shadow_sonyflake_trigger(p_schema, p_table).
// shadowSchema MUST be resolved by caller (e.g. "shadow_<source_db>").
// Idempotent: safe on re-Register.
func (s *ShadowAutomator) EnsureShadowTable(
	ctx context.Context, reg *model.TableRegistry, shadowSchema string,
) error {
	if err := validateIdent(reg.TargetTable); err != nil {
		return fmt.Errorf("invalid target_table: %w", err)
	}
	if err := validateIdent(shadowSchema); err != nil {
		return fmt.Errorf("invalid shadow_schema: %w", err)
	}
	if err := s.createShadowDDL(ctx, reg, shadowSchema); err != nil {
		return fmt.Errorf("create shadow ddl: %w", err)
	}
	if err := s.attachSonyflakeTrigger(ctx, shadowSchema, reg.TargetTable); err != nil {
		return fmt.Errorf("attach trigger: %w", err)
	}
	// markCreated phải dùng controlDB (cdc_dw) vì cdc_table_registry
	// là control-plane table, không có trong shadow DB.
	if err := s.markCreated(ctx, reg); err != nil {
		return fmt.Errorf("mark created: %w", err)
	}
	s.logger.Info("shadow table ensured",
		zap.String("schema", shadowSchema),
		zap.String("target", reg.TargetTable),
		zap.Uint("registry_id", reg.ID))
	return nil
}

// createShadowDDL builds <schema>.<target> with the 8-col CDC layout.
// target_table + schema validated upstream via validateIdent.
//
// Statements are exec'd individually because the global GORM session
// runs with PrepareStmt=true (pkgs/database/postgres.go), and PostgreSQL
// rejects multi-statement prepared queries with SQLSTATE 42601 ("cannot
// insert multiple commands into a prepared statement").
func (s *ShadowAutomator) createShadowDDL(
	ctx context.Context, reg *model.TableRegistry, schema string,
) error {
	target := reg.TargetTable
	stmts := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %[1]q`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]q.%[2]q (
            id BIGINT PRIMARY KEY,
            source_id VARCHAR(200) NOT NULL,
            _raw_data JSONB NOT NULL,
            _source VARCHAR(20) NOT NULL DEFAULT 'debezium',
            _synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            _version BIGINT NOT NULL DEFAULT 1,
            _hash VARCHAR(64),
            _deleted BOOLEAN DEFAULT FALSE,
            _created_at TIMESTAMP DEFAULT NOW(),
            _updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT %[3]q UNIQUE (source_id)
        )`, schema, target, target+"_source_id_unique"),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]q ON %[2]q.%[3]q (_synced_at)`,
			"idx_"+target+"_synced_at", schema, target),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]q ON %[2]q.%[3]q (_source)`,
			"idx_"+target+"_source", schema, target),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]q ON %[2]q.%[3]q USING GIN(_raw_data)`,
			"idx_"+target+"_raw", schema, target),
	}
	for _, stmt := range stmts {
		if err := s.shadowDB.WithContext(ctx).Exec(stmt).Error; err != nil {
			return err
		}
	}
	return nil
}

// attachSonyflakeTrigger ensures a BEFORE INSERT fallback trigger lives on
// <schema>.<table>. Under A3 hybrid the shadow cluster is independent of
// the control plane and must not host cdc_system.* — so the trigger body
// + sequence are placed inside the shadow schema itself. One function per
// shadow schema is reused by every table inside that schema.
//
// Schema/table validated upstream via validateIdent → safe to interpolate.
// Statements run individually because the pool uses PrepareStmt=true
// which rejects multi-statement queries (SQLSTATE 42601).
func (s *ShadowAutomator) attachSonyflakeTrigger(
	ctx context.Context, schema, table string,
) error {
	triggerName := "trg_" + table + "_sonyflake_fallback"
	stmts := []string{
		fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %[1]q.fencing_token_seq`, schema),

		fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]q.gen_sonyflake_id()
RETURNS BIGINT AS $fn$
DECLARE
  v_ts_ms   BIGINT;
  v_machine INTEGER;
  v_seq     BIGINT;
BEGIN
  v_ts_ms := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - 1767225600000;
  BEGIN
    v_machine := COALESCE(NULLIF(current_setting('cdc.machine_id', true), '')::INTEGER, 0) & 65535;
  EXCEPTION WHEN OTHERS THEN
    v_machine := 0;
  END;
  v_seq := nextval('%[1]s.fencing_token_seq') & 65535;
  RETURN ((v_ts_ms & 4398046511103) << 22) | ((v_machine::BIGINT & 65535) << 6) | (v_seq & 63);
END;
$fn$ LANGUAGE plpgsql VOLATILE`, schema),

		fmt.Sprintf(`CREATE OR REPLACE FUNCTION %[1]q.tg_sonyflake_fallback()
RETURNS TRIGGER AS $fn$
BEGIN
  IF NEW.id IS NULL OR NEW.id = 0 THEN
    NEW.id := %[1]q.gen_sonyflake_id();
  END IF;
  RETURN NEW;
END;
$fn$ LANGUAGE plpgsql`, schema),

		fmt.Sprintf(`DROP TRIGGER IF EXISTS %[1]q ON %[2]q.%[3]q`,
			triggerName, schema, table),

		fmt.Sprintf(`CREATE TRIGGER %[1]q BEFORE INSERT ON %[2]q.%[3]q
FOR EACH ROW EXECUTE FUNCTION %[2]q.tg_sonyflake_fallback()`,
			triggerName, schema, table),
	}
	for _, stmt := range stmts {
		if err := s.shadowDB.WithContext(ctx).Exec(stmt).Error; err != nil {
			return err
		}
	}
	return nil
}

// markCreated flips cdc_system.cdc_table_registry.is_table_created so
// the legacy Worker NATS path short-circuits when it eventually runs.
func (s *ShadowAutomator) markCreated(ctx context.Context, reg *model.TableRegistry) error {
	return s.controlDB.WithContext(ctx).Model(&model.TableRegistry{}).
		Where("id = ?", reg.ID).
		Update("is_table_created", true).Error
}

// validateIdent guards against SQL injection via schema/table name.
// Accepts the lowercase/underscore/digit subset used by Debezium target
// tables and shadow_<src> schema identifiers.
func validateIdent(s string) error {
	if len(s) == 0 || len(s) > 63 {
		return fmt.Errorf("identifier length")
	}
	for _, c := range s {
		if !(c == '_' || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return fmt.Errorf("identifier char: %q", c)
		}
	}
	return nil
}
