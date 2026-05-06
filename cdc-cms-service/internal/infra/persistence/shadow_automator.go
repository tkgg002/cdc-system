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
	db     *gorm.DB
	logger *zap.Logger
}

func NewShadowAutomator(db *gorm.DB, logger *zap.Logger) *ShadowAutomator {
	return &ShadowAutomator{db: db, logger: logger}
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
func (s *ShadowAutomator) createShadowDDL(
	ctx context.Context, reg *model.TableRegistry, schema string,
) error {
	target := reg.TargetTable
	ddl := fmt.Sprintf(`
        CREATE SCHEMA IF NOT EXISTS %[2]q;
        CREATE TABLE IF NOT EXISTS %[2]q.%[1]q (
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
        );
        CREATE INDEX IF NOT EXISTS %[4]q ON %[2]q.%[1]q (_synced_at);
        CREATE INDEX IF NOT EXISTS %[5]q ON %[2]q.%[1]q (_source);
        CREATE INDEX IF NOT EXISTS %[6]q ON %[2]q.%[1]q USING GIN(_raw_data);
    `,
		target,
		schema,
		target+"_source_id_unique",
		"idx_"+target+"_synced_at",
		"idx_"+target+"_source",
		"idx_"+target+"_raw",
	)
	return s.db.WithContext(ctx).Exec(ddl).Error
}

// attachSonyflakeTrigger invokes the schema-aware SQL helper. Helper
// itself is idempotent (DROP IF EXISTS + CREATE TRIGGER inside the fn).
func (s *ShadowAutomator) attachSonyflakeTrigger(
	ctx context.Context, schema, table string,
) error {
	return s.db.WithContext(ctx).Exec(
		"SELECT cdc_system.ensure_shadow_sonyflake_trigger(?, ?)", schema, table,
	).Error
}

// markCreated flips cdc_system.cdc_table_registry.is_table_created so
// the legacy Worker NATS path short-circuits when it eventually runs.
func (s *ShadowAutomator) markCreated(ctx context.Context, reg *model.TableRegistry) error {
	return s.db.WithContext(ctx).Model(&model.TableRegistry{}).
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
