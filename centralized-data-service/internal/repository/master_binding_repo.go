package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type MasterBindingRepo struct {
	db *gorm.DB
}

func NewMasterBindingRepo(db *gorm.DB) *MasterBindingRepo {
	return &MasterBindingRepo{db: db}
}

func (r *MasterBindingRepo) GetByID(ctx context.Context, id int64) (*model.MasterBinding, error) {
	var item model.MasterBinding
	err := r.db.WithContext(ctx).First(&item, id).Error
	return &item, err
}

func (r *MasterBindingRepo) GetByCode(ctx context.Context, code string) (*model.MasterBinding, error) {
	var item model.MasterBinding
	err := r.db.WithContext(ctx).Where("binding_code = ?", code).First(&item).Error
	return &item, err
}

func (r *MasterBindingRepo) GetByMasterTable(ctx context.Context, masterTable string) (*model.MasterBinding, error) {
	var item model.MasterBinding
	err := r.db.WithContext(ctx).Where("master_table = ?", masterTable).First(&item).Error
	return &item, err
}

func (r *MasterBindingRepo) ListBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.MasterBinding, error) {
	var items []model.MasterBinding
	err := r.db.WithContext(ctx).
		Where("source_object_id = ?", sourceObjectID).
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MasterBindingRepo) ListActiveBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.MasterBinding, error) {
	var items []model.MasterBinding
	err := r.db.WithContext(ctx).
		Where("source_object_id = ? AND is_active = ?", sourceObjectID, true).
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MasterBindingRepo) ListActiveByShadowBinding(ctx context.Context, shadowBindingID int64) ([]model.MasterBinding, error) {
	var items []model.MasterBinding
	err := r.db.WithContext(ctx).
		Where("shadow_binding_id = ? AND is_active = ? AND schema_status = ?", shadowBindingID, true, "approved").
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MasterBindingRepo) Create(ctx context.Context, item *model.MasterBinding) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *MasterBindingRepo) Update(ctx context.Context, item *model.MasterBinding) error {
	return r.db.WithContext(ctx).Save(item).Error
}
