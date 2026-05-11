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

func (r *MappingRuleV2Repo) GetActiveRulesBySourceTable(ctx context.Context, sourceTable string) ([]model.MappingRuleV2, error) {
	var items []model.MappingRuleV2
	err := r.db.WithContext(ctx).
		Joins("JOIN cdc_system.source_object_registry so ON cdc_system.mapping_rule_v2.source_object_id = so.id").
		Where("so.source_object_name = ? AND cdc_system.mapping_rule_v2.is_active = ? AND cdc_system.mapping_rule_v2.status = ?", sourceTable, true, "approved").
		Find(&items).Error
	return items, err
}
