package service

import (
	"context"
	"fmt"
	"strings"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SourceObjectV2SyncService struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewSourceObjectV2SyncService(db *gorm.DB, logger *zap.Logger) *SourceObjectV2SyncService {
	return &SourceObjectV2SyncService{db: db, logger: logger}
}

type connectionLookupRow struct {
	ID int64 `gorm:"column:id"`
}

type sourceObjectUpsertRow struct {
	ID         int64  `gorm:"column:id"`
	ObjectCode string `gorm:"column:object_code"`
}

type shadowBindingUpsertRow struct {
	ID          int64  `gorm:"column:id"`
	BindingCode string `gorm:"column:binding_code"`
}

func (s *SourceObjectV2SyncService) SyncFromLegacy(ctx context.Context, entry *model.TableRegistry) error {
	if entry == nil {
		return nil
	}

	sourceEngine := normalizeSourceEngine(entry.SourceType)
	sourceObjectType := "table"
	if sourceEngine == "mongodb" {
		sourceObjectType = "collection"
	}

	sourceDB := strings.TrimSpace(entry.SourceDB)
	sourceTable := strings.TrimSpace(entry.SourceTable)
	targetTable := strings.TrimSpace(entry.TargetTable)
	sourceNamespace := sourceDB
	shadowSchema := normalizeShadowSchema(sourceDB)
	physicalTableFQN := shadowSchema + "." + targetTable
	normalizedSourceKey := strings.ToLower(fmt.Sprintf("%s:%s:%s", sourceEngine, sourceDB, sourceTable))

	sourceConnectionID, err := s.resolveSourceConnectionID(ctx, sourceEngine, sourceDB)
	if err != nil {
		return err
	}
	shadowConnectionID, err := s.resolveShadowConnectionID(ctx)
	if err != nil {
		return err
	}

	objectCode := buildSourceObjectCode(sourceEngine, sourceDB, sourceTable)
	bindingCode := buildShadowBindingCode(sourceEngine, sourceDB, targetTable)

	profileStatus := "active"
	if !entry.IsActive {
		profileStatus = "paused"
	}

	locatorJSON := []byte(fmt.Sprintf(
		`{"legacy_registry_id":%d,"legacy_target_table":"%s","source_db":"%s","source_table":"%s"}`,
		entry.ID,
		escapeJSON(targetTable),
		escapeJSON(sourceDB),
		escapeJSON(sourceTable),
	))

	var sourceObject sourceObjectUpsertRow
	if err := s.db.WithContext(ctx).Raw(`
		INSERT INTO cdc_system.source_object_registry (
		  object_code,
		  source_connection_id,
		  source_engine_type,
		  source_database,
		  source_schema,
		  source_namespace,
		  source_object_name,
		  source_object_type,
		  source_locator_json,
		  normalized_source_key,
		  primary_key_field,
		  primary_key_type,
		  timestamp_field,
		  timestamp_candidates_json,
		  cdc_mode,
		  sync_engine,
		  is_active,
		  profile_status,
		  notes,
		  updated_at
		)
		VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, '[]'::jsonb, 'incremental', 'debezium', ?, ?, ?, NOW())
		ON CONFLICT (normalized_source_key) DO UPDATE
		SET
		  source_connection_id = EXCLUDED.source_connection_id,
		  source_engine_type = EXCLUDED.source_engine_type,
		  source_database = EXCLUDED.source_database,
		  source_namespace = EXCLUDED.source_namespace,
		  source_object_name = EXCLUDED.source_object_name,
		  source_object_type = EXCLUDED.source_object_type,
		  source_locator_json = EXCLUDED.source_locator_json,
		  primary_key_field = EXCLUDED.primary_key_field,
		  primary_key_type = EXCLUDED.primary_key_type,
		  timestamp_field = EXCLUDED.timestamp_field,
		  sync_engine = EXCLUDED.sync_engine,
		  is_active = EXCLUDED.is_active,
		  profile_status = EXCLUDED.profile_status,
		  notes = EXCLUDED.notes,
		  updated_at = NOW()
		RETURNING id, object_code
	`,
		objectCode,
		sourceConnectionID,
		sourceEngine,
		sourceDB,
		sourceNamespace,
		sourceTable,
		sourceObjectType,
		locatorJSON,
		normalizedSourceKey,
		defaultString(entry.PrimaryKeyField, "id"),
		entry.PrimaryKeyType,
		entry.TimestampField,
		entry.IsActive,
		profileStatus,
		entry.Notes,
	).Scan(&sourceObject).Error; err != nil {
		s.logger.Error("sync v2 source object failed", zap.Error(err), zap.String("target_table", targetTable))
		return err
	}

	ddlStatus := "pending"
	if entry.IsTableCreated {
		ddlStatus = "created"
	}

	var shadowBinding shadowBindingUpsertRow
	if err := s.db.WithContext(ctx).Raw(`
		INSERT INTO cdc_system.shadow_binding (
		  binding_code,
		  source_object_id,
		  shadow_connection_id,
		  shadow_database,
		  shadow_schema,
		  shadow_table,
		  physical_table_fqn,
		  namespace_strategy,
		  write_mode,
		  ddl_status,
		  is_active,
		  updated_at
		)
		VALUES (?, ?, ?, current_database(), ?, ?, ?, 'preserve', 'upsert', ?, ?, NOW())
		ON CONFLICT (source_object_id, shadow_connection_id, shadow_schema, shadow_table) DO UPDATE
		SET
		  binding_code = EXCLUDED.binding_code,
		  shadow_database = EXCLUDED.shadow_database,
		  physical_table_fqn = EXCLUDED.physical_table_fqn,
		  ddl_status = EXCLUDED.ddl_status,
		  is_active = EXCLUDED.is_active,
		  updated_at = NOW()
		RETURNING id, binding_code
	`,
		bindingCode,
		sourceObject.ID,
		shadowConnectionID,
		shadowSchema,
		targetTable,
		physicalTableFQN,
		ddlStatus,
		entry.IsActive,
	).Scan(&shadowBinding).Error; err != nil {
		s.logger.Error("sync v2 shadow binding failed", zap.Error(err), zap.String("target_table", targetTable))
		return err
	}

	return nil
}

func (s *SourceObjectV2SyncService) resolveSourceConnectionID(ctx context.Context, engine, sourceDB string) (int64, error) {
	var row connectionLookupRow
	err := s.db.WithContext(ctx).Raw(`
		SELECT id
		FROM cdc_system.connection_registry
		WHERE role_type IN ('source', 'mixed')
		  AND engine_type = ?
		  AND status = 'active'
		ORDER BY
		  CASE WHEN COALESCE(default_database, '') = ? THEN 0 ELSE 1 END,
		  id ASC
		LIMIT 1
	`, engine, sourceDB).Scan(&row).Error
	if err != nil {
		return 0, err
	}
	if row.ID == 0 {
		return 0, fmt.Errorf("no active source connection for engine=%s source_db=%s", engine, sourceDB)
	}
	return row.ID, nil
}

func (s *SourceObjectV2SyncService) resolveShadowConnectionID(ctx context.Context) (int64, error) {
	var row connectionLookupRow
	err := s.db.WithContext(ctx).Raw(`
		SELECT id
		FROM cdc_system.connection_registry
		WHERE role_type IN ('shadow', 'mixed')
		  AND status = 'active'
		ORDER BY id ASC
		LIMIT 1
	`).Scan(&row).Error
	if err != nil {
		return 0, err
	}
	if row.ID == 0 {
		return 0, fmt.Errorf("no active shadow connection configured")
	}
	return row.ID, nil
}

func normalizeSourceEngine(sourceType string) string {
	switch strings.ToLower(strings.TrimSpace(sourceType)) {
	case "postgres", "postgresql":
		return "postgresql"
	case "mariadb":
		return "mariadb"
	case "mysql":
		return "mysql"
	case "mongodb", "mongo":
		return "mongodb"
	case "clickhouse":
		return "clickhouse"
	default:
		return "postgresql"
	}
}

func normalizeShadowSchema(sourceDB string) string {
	return "shadow_" + slugifyIdentifier(sourceDB)
}

func buildSourceObjectCode(engine, sourceDB, sourceTable string) string {
	return "src_" + slugifyIdentifier(engine) + "_" + slugifyIdentifier(sourceDB) + "_" + slugifyIdentifier(sourceTable)
}

func buildShadowBindingCode(engine, sourceDB, targetTable string) string {
	return "sb_" + slugifyIdentifier(engine) + "_" + slugifyIdentifier(sourceDB) + "_" + slugifyIdentifier(targetTable)
}

func slugifyIdentifier(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range normalized {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

func defaultString(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func escapeJSON(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(value)
}
