package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type MappingRuleV2Repo struct {
	db *gorm.DB
}

func NewMappingRuleV2Repo(db *gorm.DB) *MappingRuleV2Repo {
	return &MappingRuleV2Repo{db: db}
}

func (r *MappingRuleV2Repo) ListBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.MappingRuleV2, error) {
	var items []model.MappingRuleV2
	err := r.db.WithContext(ctx).
		Where("source_object_id = ?", sourceObjectID).
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MappingRuleV2Repo) ListActiveByMasterBinding(ctx context.Context, masterBindingID int64) ([]model.MappingRuleV2, error) {
	var items []model.MappingRuleV2
	err := r.db.WithContext(ctx).
		Where("master_binding_id = ? AND is_active = ? AND status = ?", masterBindingID, true, "approved").
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MappingRuleV2Repo) ListActiveBySourceObject(ctx context.Context, sourceObjectID int64) ([]model.MappingRuleV2, error) {
	var items []model.MappingRuleV2
	err := r.db.WithContext(ctx).
		Where("source_object_id = ? AND is_active = ? AND status = ?", sourceObjectID, true, "approved").
		Order("id").
		Find(&items).Error
	return items, err
}

func (r *MappingRuleV2Repo) Create(ctx context.Context, item *model.MappingRuleV2) error {
	return r.db.WithContext(ctx).Create(item).Error
}

func (r *MappingRuleV2Repo) Update(ctx context.Context, item *model.MappingRuleV2) error {
	return r.db.WithContext(ctx).Save(item).Error
}
