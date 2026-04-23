package service

import (
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// BridgeService retained as a thin schema-probe helper after the legacy
// CDC bridge was retired (plan v2 §R4 / Sprint 4 4A.1). Only TableExists +
// HasColumn remain — consumers in scan_service.go + transform_service.go.
//
// Historical name kept to avoid churn; `SchemaProbeService` would be more
// accurate but the rename is out-of-scope for Sprint 4.
type BridgeService struct {
	db           *gorm.DB
	registryRepo *repository.RegistryRepo
	logger       *zap.Logger
}

func NewBridgeService(db *gorm.DB, registryRepo *repository.RegistryRepo, logger *zap.Logger) *BridgeService {
	return &BridgeService{db: db, registryRepo: registryRepo, logger: logger}
}

// TableExists checks if a table exists in public schema.
func (s *BridgeService) TableExists(tableName string) bool {
	var exists bool
	s.db.Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')",
		tableName,
	).Scan(&exists)
	return exists
}

// HasColumn checks if a column exists in a table.
func (s *BridgeService) HasColumn(tableName, columnName string) bool {
	var exists bool
	s.db.Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?)",
		tableName, columnName,
	).Scan(&exists)
	return exists
}
