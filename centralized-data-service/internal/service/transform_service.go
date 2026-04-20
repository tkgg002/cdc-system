package service

import (
	"context"
	"fmt"
	"strings"

	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransformService handles _raw_data → typed columns transformation
type TransformService struct {
	db          *gorm.DB
	mappingRepo *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	bridge      *BridgeService
	logger      *zap.Logger
}

func NewTransformService(db *gorm.DB, mappingRepo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, bridge *BridgeService, logger *zap.Logger) *TransformService {
	return &TransformService{db: db, mappingRepo: mappingRepo, registryRepo: registryRepo, bridge: bridge, logger: logger}
}

// BatchTransform applies mapping rules to populate typed columns from _raw_data
func (s *TransformService) BatchTransform(ctx context.Context, targetTable string) (int64, error) {
	if !s.bridge.TableExists(targetTable) {
		return 0, fmt.Errorf("table does not exist")
	}
	if !s.bridge.HasColumn(targetTable, "_raw_data") {
		return 0, fmt.Errorf("table has no _raw_data column")
	}

	reg, _ := s.registryRepo.GetByTargetTable(ctx, targetTable)
	sourceTable := targetTable
	if reg != nil {
		sourceTable = reg.SourceTable
	}

	rules, err := s.mappingRepo.GetByTable(ctx, sourceTable)
	if err != nil || len(rules) == 0 {
		return 0, fmt.Errorf("no active mapping rules for %s (source: %s)", targetTable, sourceTable)
	}

	var setClauses, whereClauses []string
	for _, rule := range rules {
		if !rule.IsActive {
			continue
		}
		castExpr := BuildCastExpr(rule.SourceField, rule.DataType)
		setClauses = append(setClauses, fmt.Sprintf(`"%s" = %s`, rule.TargetColumn, castExpr))
		whereClauses = append(whereClauses, fmt.Sprintf(`"%s" IS NULL`, rule.TargetColumn))
	}

	if len(setClauses) == 0 {
		return 0, nil
	}

	setClauses = append(setClauses, "_updated_at = NOW()")
	sql := fmt.Sprintf(`UPDATE "%s" SET %s WHERE _raw_data IS NOT NULL AND (%s)`,
		targetTable, strings.Join(setClauses, ", "), strings.Join(whereClauses, " OR "))

	result := s.db.Exec(sql)
	return result.RowsAffected, result.Error
}

// BuildCastExpr creates type-cast expression for _raw_data field extraction
func BuildCastExpr(field, dataType string) string {
	base := fmt.Sprintf(`(_raw_data->>'%s')`, field)
	switch strings.ToLower(dataType) {
	case "integer", "int", "int4", "int8", "bigint", "smallint":
		return fmt.Sprintf("(%s)::INTEGER", base)
	case "numeric", "decimal", "float", "float8", "double precision":
		return fmt.Sprintf("(%s)::NUMERIC", base)
	case "boolean", "bool":
		return fmt.Sprintf("(%s)::BOOLEAN", base)
	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return fmt.Sprintf("(%s)::TIMESTAMP", base)
	default:
		return fmt.Sprintf("(%s)::TEXT", base)
	}
}
