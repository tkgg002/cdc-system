package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type SyncRuntimeStateRepo struct {
	db *gorm.DB
}

func NewSyncRuntimeStateRepo(db *gorm.DB) *SyncRuntimeStateRepo {
	return &SyncRuntimeStateRepo{db: db}
}

func (r *SyncRuntimeStateRepo) ListBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.SyncRuntimeState, error) {
	var items []model.SyncRuntimeState
	err := r.db.WithContext(ctx).
		Where("source_object_id = ?", sourceObjectID).
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *SyncRuntimeStateRepo) GetByShadowBinding(ctx context.Context, shadowBindingID int64) (*model.SyncRuntimeState, error) {
	var item model.SyncRuntimeState
	err := r.db.WithContext(ctx).
		Where("shadow_binding_id = ? AND runtime_scope = ?", shadowBindingID, "shadow").
		First(&item).Error
	return &item, err
}

func (r *SyncRuntimeStateRepo) GetByMasterBinding(ctx context.Context, masterBindingID int64) (*model.SyncRuntimeState, error) {
	var item model.SyncRuntimeState
	err := r.db.WithContext(ctx).
		Where("master_binding_id = ? AND runtime_scope = ?", masterBindingID, "master").
		First(&item).Error
	return &item, err
}

func (r *SyncRuntimeStateRepo) Create(ctx context.Context, item *model.SyncRuntimeState) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *SyncRuntimeStateRepo) Update(ctx context.Context, item *model.SyncRuntimeState) error {
	return r.db.WithContext(ctx).Save(item).Error
}
