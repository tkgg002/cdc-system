package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type SourceObjectRegistryRepo struct {
	db *gorm.DB
}

func NewSourceObjectRegistryRepo(db *gorm.DB) *SourceObjectRegistryRepo {
	return &SourceObjectRegistryRepo{db: db}
}

func (r *SourceObjectRegistryRepo) GetAll(ctx context.Context) ([]model.SourceObjectRegistry, error) {
	var items []model.SourceObjectRegistry
	err := r.db.WithContext(ctx).Order("normalized_source_key").Find(&items).Error
	return items, err
}

func (r *SourceObjectRegistryRepo) GetActive(ctx context.Context) ([]model.SourceObjectRegistry, error) {
	var items []model.SourceObjectRegistry
	err := r.db.WithContext(ctx).Where("is_active = ?", true).Order("normalized_source_key").Find(&items).Error
	return items, err
}

func (r *SourceObjectRegistryRepo) GetByID(ctx context.Context, id int64) (*model.SourceObjectRegistry, error) {
	var item model.SourceObjectRegistry
	err := r.db.WithContext(ctx).First(&item, id).Error
	return &item, err
}

func (r *SourceObjectRegistryRepo) GetByNormalizedKey(ctx context.Context, key string) (*model.SourceObjectRegistry, error) {
	var item model.SourceObjectRegistry
	err := r.db.WithContext(ctx).Where("normalized_source_key = ?", key).First(&item).Error
	return &item, err
}

func (r *SourceObjectRegistryRepo) Create(ctx context.Context, item *model.SourceObjectRegistry) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *SourceObjectRegistryRepo) Update(ctx context.Context, item *model.SourceObjectRegistry) error {
	return r.db.WithContext(ctx).Save(item).Error
}
