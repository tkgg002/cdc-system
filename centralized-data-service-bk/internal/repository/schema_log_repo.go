package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type SchemaLogRepo struct {
	db *gorm.DB
}

func NewSchemaLogRepo(db *gorm.DB) *SchemaLogRepo {
	return &SchemaLogRepo{db: db}
}

func (r *SchemaLogRepo) Create(ctx context.Context, log *model.SchemaChangeLog) error {
	return r.db.WithContext(ctx).Create(log).Error
}

func (r *SchemaLogRepo) GetByTable(ctx context.Context, tableName *string, sourceDB *string) ([]model.SchemaChangeLog, error) {
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

