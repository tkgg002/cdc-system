package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// BridgeService handles Airbyte → CDC data bridge operations
type BridgeService struct {
	db           *gorm.DB
	registryRepo *repository.RegistryRepo
	logger       *zap.Logger
}

func NewBridgeService(db *gorm.DB, registryRepo *repository.RegistryRepo, logger *zap.Logger) *BridgeService {
	return &BridgeService{db: db, registryRepo: registryRepo, logger: logger}
}

// EnsureCDCColumns adds CDC system columns to an existing table if missing
func (s *BridgeService) EnsureCDCColumns(tableName string) error {
	if !s.TableExists(tableName) {
		return fmt.Errorf("table %s does not exist", tableName)
	}
	cdcColumns := []struct{ name, def string }{
		{"_raw_data", "JSONB"},
		{"_source", "VARCHAR(20) DEFAULT 'airbyte'"},
		{"_synced_at", "TIMESTAMP DEFAULT NOW()"},
		{"_version", "BIGINT DEFAULT 1"},
		{"_hash", "VARCHAR(64)"},
		{"_deleted", "BOOLEAN DEFAULT FALSE"},
		{"_created_at", "TIMESTAMP DEFAULT NOW()"},
		{"_updated_at", "TIMESTAMP DEFAULT NOW()"},
	}
	for _, col := range cdcColumns {
		s.db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS %s %s`, tableName, col.name, col.def))
	}
	return nil
}

// TableExists checks if a table exists in public schema
func (s *BridgeService) TableExists(tableName string) bool {
	var exists bool
	s.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", tableName).Scan(&exists)
	return exists
}

// HasColumn checks if a column exists in a table
func (s *BridgeService) HasColumn(tableName, columnName string) bool {
	var exists bool
	s.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?)", tableName, columnName).Scan(&exists)
	return exists
}

// BridgeInPlace populates _raw_data from existing typed columns (for Airbyte tables)
func (s *BridgeService) BridgeInPlace(tableName string) (int64, error) {
	sql := fmt.Sprintf(`
		UPDATE "%s" SET
			_raw_data = to_jsonb("%s".*) - '_raw_data' - '_source' - '_synced_at' - '_version' - '_hash' - '_deleted' - '_created_at' - '_updated_at' - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
			_source = COALESCE(_source, 'airbyte'),
			_synced_at = COALESCE(_synced_at, NOW()),
			_hash = md5((to_jsonb("%s".*))::text),
			_version = COALESCE(_version, 1),
			_updated_at = NOW()
		WHERE _raw_data IS NULL OR _raw_data = '{}'::jsonb`,
		tableName, tableName, tableName,
	)
	result := s.db.Exec(sql)
	return result.RowsAffected, result.Error
}

// BuildBridgeSQL constructs the bridge SQL for copying from Airbyte table to CDC table
func (s *BridgeService) BuildBridgeSQL(targetTable, airbyteTable, pkField, whereClause string, hasSonyflakeSchema bool) string {
	_ = json.Compact // suppress unused import
	_ = strings.ReplaceAll
	_ = model.TableRegistry{}
	_ = context.Background

	if hasSonyflakeSchema {
		return fmt.Sprintf(`
			INSERT INTO "%s" (source_id, _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
			SELECT src."%s"::VARCHAR,
				to_jsonb(src) - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
				'airbyte', COALESCE(src._airbyte_extracted_at, NOW()), md5((to_jsonb(src))::text), 1, NOW(), NOW()
			FROM "%s" src WHERE src."%s" IS NOT NULL %s
			ON CONFLICT (source_id) DO UPDATE SET
				_raw_data = EXCLUDED._raw_data, _synced_at = EXCLUDED._synced_at, _hash = EXCLUDED._hash,
				_version = "%s"._version + 1, _updated_at = NOW()
			WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
			targetTable, pkField, airbyteTable, pkField, whereClause, targetTable, targetTable)
	}

	cdcPKColumn := "id"
	if pkField != "_id" && pkField != "_airbyte_raw_id" {
		cdcPKColumn = pkField
	}
	return fmt.Sprintf(`
		INSERT INTO "%s" ("%s", _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
		SELECT src."%s"::VARCHAR,
			to_jsonb(src) - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
			'airbyte', COALESCE(src._airbyte_extracted_at, NOW()), md5((to_jsonb(src))::text), 1, NOW(), NOW()
		FROM "%s" src WHERE src."%s" IS NOT NULL %s
		ON CONFLICT ("%s") DO UPDATE SET
			_raw_data = EXCLUDED._raw_data, _synced_at = EXCLUDED._synced_at, _hash = EXCLUDED._hash,
			_version = "%s"._version + 1, _updated_at = NOW()
		WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
		targetTable, cdcPKColumn, pkField, airbyteTable, pkField, whereClause, cdcPKColumn, targetTable, targetTable)
}
