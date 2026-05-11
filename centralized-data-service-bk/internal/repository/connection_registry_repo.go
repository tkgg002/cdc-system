package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type ConnectionRegistryRepo struct {
	db *gorm.DB
}

func NewConnectionRegistryRepo(db *gorm.DB) *ConnectionRegistryRepo {
	return &ConnectionRegistryRepo{db: db}
}

func (r *ConnectionRegistryRepo) GetAll(ctx context.Context) ([]model.ConnectionRegistry, error) {
	var items []model.ConnectionRegistry
	err := r.db.WithContext(ctx).Order("connection_code").Find(&items).Error
	return items, err
}

func (r *ConnectionRegistryRepo) GetActive(ctx context.Context) ([]model.ConnectionRegistry, error) {
	var items []model.ConnectionRegistry
	err := r.db.WithContext(ctx).Where("status = ?", "active").Order("connection_code").Find(&items).Error
	return items, err
}

func (r *ConnectionRegistryRepo) GetByID(ctx context.Context, id int64) (*model.ConnectionRegistry, error) {
	var item model.ConnectionRegistry
	err := r.db.WithContext(ctx).First(&item, id).Error
	return &item, err
}

func (r *ConnectionRegistryRepo) GetByCode(ctx context.Context, code string) (*model.ConnectionRegistry, error) {
	var item model.ConnectionRegistry
	err := r.db.WithContext(ctx).Where("connection_code = ?", code).First(&item).Error
	return &item, err
}

func (r *ConnectionRegistryRepo) Create(ctx context.Context, item *model.ConnectionRegistry) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *ConnectionRegistryRepo) Update(ctx context.Context, item *model.ConnectionRegistry) error {
	return r.db.WithContext(ctx).Save(item).Error
}
