package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type ShadowBindingRepo struct {
	db *gorm.DB
}

func NewShadowBindingRepo(db *gorm.DB) *ShadowBindingRepo {
	return &ShadowBindingRepo{db: db}
}

func (r *ShadowBindingRepo) GetByID(ctx context.Context, id int64) (*model.ShadowBinding, error) {
	var item model.ShadowBinding
	err := r.db.WithContext(ctx).First(&item, id).Error
	return &item, err
}

func (r *ShadowBindingRepo) GetByCode(ctx context.Context, code string) (*model.ShadowBinding, error) {
	var item model.ShadowBinding
	err := r.db.WithContext(ctx).Where("binding_code = ?", code).First(&item).Error
	return &item, err
}

func (r *ShadowBindingRepo) GetActiveBySourceObject(ctx context.Context, sourceObjectID int64) (*model.ShadowBinding, error) {
	var item model.ShadowBinding
	err := r.db.WithContext(ctx).
		Where("source_object_id = ? AND is_active = ?", sourceObjectID, true).
		Order("id DESC").
		First(&item).Error
	return &item, err
}

func (r *ShadowBindingRepo) ListBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.ShadowBinding, error) {
	var items []model.ShadowBinding
	err := r.db.WithContext(ctx).
		Where("source_object_id = ?", sourceObjectID).
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *ShadowBindingRepo) Create(ctx context.Context, item *model.ShadowBinding) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *ShadowBindingRepo) Update(ctx context.Context, item *model.ShadowBinding) error {
	return r.db.WithContext(ctx).Save(item).Error
}
