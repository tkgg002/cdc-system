// Package persistence — schema_log_repo_gorm.go is the GORM-backed
// adapter for ports.SchemaLogRepo. SQL is lifted verbatim from the
// legacy `internal/repository/schema_log_repo.go` so the audit-row
// shape and ordering remain byte-identical after Task #19 đợt A.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type schemaLogRepoGorm struct {
	db *gorm.DB
}

// NewSchemaLogRepo constructs the GORM-backed adapter for
// ports.SchemaLogRepo.
func NewSchemaLogRepo(db *gorm.DB) ports.SchemaLogRepo {
	return &schemaLogRepoGorm{db: db}
}

func (r *schemaLogRepoGorm) Create(ctx context.Context, log *model.SchemaChangeLog) error {
	return r.db.WithContext(ctx).Create(log).Error
}

func (r *schemaLogRepoGorm) GetByTable(ctx context.Context, tableName *string, sourceDB *string) ([]model.SchemaChangeLog, error) {
	query := r.db.WithContext(ctx)
	if tableName != nil {
		query = query.Where("table_name = ?", *tableName)
	}
	if sourceDB != nil {
		query = query.Where("source_db = ?", *sourceDB)
	}
	var logs []model.SchemaChangeLog
	err := query.Order("executed_at DESC").Limit(100).Find(&logs).Error
	return logs, err
}
