package service

import (
	"context"
	"fmt"
	"strings"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ScanService handles field discovery from _raw_data
type ScanService struct {
	db           *gorm.DB
	mappingRepo  *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	bridge       *BridgeService
	logger       *zap.Logger
}

func NewScanService(db *gorm.DB, mappingRepo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, bridge *BridgeService, logger *zap.Logger) *ScanService {
	return &ScanService{db: db, mappingRepo: mappingRepo, registryRepo: registryRepo, bridge: bridge, logger: logger}
}

// ScanRawData scans _raw_data JSONB for unmapped fields in a specific table
func (s *ScanService) ScanRawData(ctx context.Context, targetTable string) ([]string, error) {
	if !s.bridge.TableExists(targetTable) || !s.bridge.HasColumn(targetTable, "_raw_data") {
		return nil, fmt.Errorf("table or _raw_data not found")
	}

	var rawKeys []string
	sql := fmt.Sprintf(`SELECT DISTINCT key FROM (
		SELECT jsonb_object_keys(_raw_data) AS key FROM "%s"
		WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb LIMIT 1000
	) sub ORDER BY key`, targetTable)
	s.db.Raw(sql).Scan(&rawKeys)

	reg, _ := s.registryRepo.GetByTargetTable(ctx, targetTable)
	sourceTable := targetTable
	if reg != nil {
		sourceTable = reg.SourceTable
	}

	existingRules, _ := s.mappingRepo.GetByTable(ctx, sourceTable)
	mapped := make(map[string]bool)
	for _, r := range existingRules {
		mapped[r.SourceField] = true
	}

	skipFields := map[string]bool{
		"_raw_data": true, "_source": true, "_synced_at": true,
		"_version": true, "_hash": true, "_deleted": true,
		"_created_at": true, "_updated_at": true,
	}

	var unmapped []string
	for _, key := range rawKeys {
		if !mapped[key] && !skipFields[key] && !strings.HasPrefix(key, "_airbyte_") {
			unmapped = append(unmapped, key)
		}
	}
	return unmapped, nil
}

// PeriodicScan scans all active tables for new unmapped fields
func (s *ScanService) PeriodicScan(ctx context.Context) int {
	entries, err := s.registryRepo.GetAllActive(ctx)
	if err != nil {
		return 0
	}

	totalNew := 0
	for _, entry := range entries {
		if !s.bridge.TableExists(entry.TargetTable) || !s.bridge.HasColumn(entry.TargetTable, "_raw_data") {
			continue
		}

		unmapped, _ := s.ScanRawData(ctx, entry.TargetTable)
		for _, key := range unmapped {
			rule := model.MappingRule{
				SourceTable:  entry.SourceTable,
				SourceField:  key,
				TargetColumn: key,
				DataType:     "TEXT",
				IsActive:     false,
				Status:       "pending",
				RuleType:     "discovered",
			}
			if created, _ := s.mappingRepo.CreateIfNotExists(ctx, &rule); created {
				totalNew++
			}
		}
	}
	return totalNew
}
