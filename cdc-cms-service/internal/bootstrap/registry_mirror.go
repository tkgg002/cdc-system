package bootstrap

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SyncLegacyToV2Bootstrap ensures that all legacy metadata (sources, table
// registries, and mapping rules) are mirrored to the V2 registry tables.
// This is required for V2 registration to resolve source connection IDs
// and mapping context for objects created before Phase 2.
func SyncLegacyToV2Bootstrap(ctx context.Context, db *gorm.DB, logger *zap.Logger) error {
	var sources []model.Source
	if err := db.WithContext(ctx).Where("status != ?", "deleted").Find(&sources).Error; err != nil {
		return fmt.Errorf("list sources: %w", err)
	}

	for _, s := range sources {
		host, port := splitHostPort(s.ServerAddress)
		engine := normalizeSourceEngine(s.SourceType)

		err := db.WithContext(ctx).Exec(`
			INSERT INTO cdc_system.connection_registry (
				connection_code, display_name, role_type, engine_type,
				host, port, default_database, secret_ref, status, updated_at
			)
			VALUES (?, ?, 'source', ?, ?, ?, ?, ?, 'active', NOW())
			ON CONFLICT (connection_code) DO UPDATE SET
				engine_type = EXCLUDED.engine_type,
				host = EXCLUDED.host,
				port = EXCLUDED.port,
				default_database = EXCLUDED.default_database,
				status = 'active',
				updated_at = NOW()
		`,
			s.ConnectorName,
			s.ConnectorName,
			engine,
			nullIfEmpty(host),
			nullIfZero(port),
			nullIfEmpty(s.DatabaseIncludeList),
			"v1:"+s.ConnectorName,
		).Error

		if err != nil {
			logger.Error("failed to mirror connector to registry", 
				zap.String("connector", s.ConnectorName), zap.Error(err))
			continue
		}
	}

	logger.Info("connector-registry sync complete", zap.Int("count", len(sources)))

	// 2. Sync cdc_table_registry -> source_object_registry + shadow_binding
	var registries []model.TableRegistry
	if err := db.WithContext(ctx).Find(&registries).Error; err != nil {
		return fmt.Errorf("list registries: %w", err)
	}

	for _, reg := range registries {
		// Use raw SQL to avoid dependency on persistence package
		// This mirrors the logic in SourceObjectV2SyncService.SyncFromLegacyTx
		
		// 2.1 Resolve source connection ID
		var sourceConnID int64
		engine := normalizeSourceEngine(reg.SourceType)
		err := db.WithContext(ctx).Raw(`
			SELECT id FROM cdc_system.connection_registry
			WHERE role_type IN ('source', 'mixed')
			  AND engine_type = ?
			  AND status = 'active'
			ORDER BY CASE WHEN COALESCE(default_database, '') = ? THEN 0 ELSE 1 END, id ASC
			LIMIT 1
		`, engine, reg.SourceDB).Scan(&sourceConnID).Error
		if err != nil || sourceConnID == 0 {
			logger.Warn("bootstrap sync: skipping registry (no source connection)", 
				zap.String("table", reg.SourceTable), zap.String("db", reg.SourceDB))
			continue
		}

		// 2.2 Resolve shadow connection ID
		var shadowConnID int64
		err = db.WithContext(ctx).Raw(`
			SELECT id FROM cdc_system.connection_registry
			WHERE role_type IN ('shadow', 'mixed')
			  AND status = 'active'
			ORDER BY id ASC LIMIT 1
		`).Scan(&shadowConnID).Error
		if err != nil || shadowConnID == 0 {
			logger.Warn("bootstrap sync: skipping registry (no shadow connection)", 
				zap.String("table", reg.SourceTable))
			continue
		}

		// 2.3 Upsert Source Object
		sourceObjectType := "table"
		if engine == "mongodb" {
			sourceObjectType = "collection"
		}
		objectCode := "src_" + slugify(engine) + "_" + slugify(reg.SourceDB) + "_" + slugify(reg.SourceTable)
		normKey := strings.ToLower(fmt.Sprintf("%s:%s:%s", engine, reg.SourceDB, reg.SourceTable))
		locator := fmt.Sprintf(`{"legacy_registry_id":%d,"legacy_target_table":"%s","source_db":"%s","source_table":"%s"}`,
			reg.ID, reg.TargetTable, reg.SourceDB, reg.SourceTable)

		var soID int64
		err = db.WithContext(ctx).Raw(`
			INSERT INTO cdc_system.source_object_registry (
				object_code, source_connection_id, source_engine_type, source_database,
				source_namespace, source_object_name, source_object_type, source_locator_json,
				normalized_source_key, primary_key_field, primary_key_type, timestamp_field,
				sync_engine, is_active, profile_status, updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, 'debezium', ?, 'active', NOW())
			ON CONFLICT (normalized_source_key) DO UPDATE SET
				updated_at = NOW()
			RETURNING id
		`, objectCode, sourceConnID, engine, reg.SourceDB, reg.SourceDB, reg.SourceTable, sourceObjectType, 
		   locator, normKey, reg.PrimaryKeyField, reg.PrimaryKeyType, reg.TimestampField, reg.IsActive).Scan(&soID).Error

		if err != nil || soID == 0 {
			logger.Error("bootstrap sync: failed to upsert source object", zap.Error(err))
			continue
		}

		// 2.4 Upsert Shadow Binding
		bindingCode := "sb_" + slugify(engine) + "_" + slugify(reg.SourceDB) + "_" + slugify(reg.TargetTable)
		shadowSchema := "shadow_" + slugify(reg.SourceDB)
		fqn := shadowSchema + "." + reg.TargetTable
		ddlStatus := "pending"
		if reg.IsTableCreated {
			ddlStatus = "created"
		}

		err = db.WithContext(ctx).Exec(`
			INSERT INTO cdc_system.shadow_binding (
				binding_code, source_object_id, shadow_connection_id, shadow_database,
				shadow_schema, shadow_table, physical_table_fqn, namespace_strategy,
				write_mode, ddl_status, is_active, updated_at
			)
			VALUES (?, ?, ?, current_database(), ?, ?, ?, 'preserve', 'upsert', ?, ?, NOW())
			ON CONFLICT (source_object_id, shadow_connection_id, shadow_schema, shadow_table) DO UPDATE SET
				updated_at = NOW()
		`, bindingCode, soID, shadowConnID, shadowSchema, reg.TargetTable, fqn, ddlStatus, reg.IsActive).Error

		if err != nil {
			logger.Error("bootstrap sync: failed to upsert shadow binding", zap.Error(err))
			continue
		}

		// 2.5 Sync Mapping Rules
		err = db.WithContext(ctx).Exec(`
			INSERT INTO cdc_system.mapping_rule_v2 (
				source_object_id, source_field, target_column,
				data_type, is_active, is_nullable, default_value,
				transform_fn, status, notes, created_at, updated_at
			)
			SELECT ?, source_field, target_column,
				data_type, is_active, is_nullable, default_value,
				enrichment_function, status, notes, created_at, NOW()
			FROM cdc_system.cdc_mapping_rules
			WHERE source_table = ?
			ON CONFLICT (source_object_id, COALESCE(master_binding_id, 0), target_column) DO NOTHING
		`, soID, reg.SourceTable).Error

		if err != nil {
			logger.Error("bootstrap sync: failed to sync mapping rules", zap.Error(err))
		}
	}

	logger.Info("legacy-registry mirror complete", zap.Int("count", len(registries)))
	return nil
}

// Helpers duplicated from persistence package to avoid circular dependency
// and keep bootstrap self-contained.

func splitHostPort(addr string) (string, int) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", 0
	}
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, 0
	}
	host := addr[:idx]
	port, err := strconv.Atoi(addr[idx+1:])
	if err != nil {
		return addr, 0
	}
	return host, port
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

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullIfZero(n int) interface{} {
	if n == 0 {
		return nil
	}
	return n
}

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var out strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			out.WriteRune(r)
			lastUnderscore = false
		} else {
			if !lastUnderscore {
				out.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(out.String(), "_")
}
