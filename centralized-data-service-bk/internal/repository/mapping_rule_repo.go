package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type MappingRuleRepo struct {
	db *gorm.DB
}

func NewMappingRuleRepo(db *gorm.DB) *MappingRuleRepo {
	return &MappingRuleRepo{db: db}
}

func (r *MappingRuleRepo) GetAllActive(ctx context.Context) ([]model.MappingRule, error) {
	var rules []model.MappingRule
	err := r.db.WithContext(ctx).Where("is_active = ?", true).Find(&rules).Error
	return rules, err
}

func (r *MappingRuleRepo) GetByTable(ctx context.Context, tableName string) ([]model.MappingRule, error) {
	var rules []model.MappingRule
	err := r.db.WithContext(ctx).Where("source_table = ? AND is_active = ?", tableName, true).Find(&rules).Error
	return rules, err
}

func (r *MappingRuleRepo) Create(ctx context.Context, rule *model.MappingRule) error {
	return r.db.WithContext(ctx).Create(rule).Error
}

// CreateIfNotExists inserts a mapping rule only if (source_table, source_field) doesn't exist
func (r *MappingRuleRepo) CreateIfNotExists(ctx context.Context, rule *model.MappingRule) (bool, error) {
	var existing model.MappingRule
	result := r.db.WithContext(ctx).
		Where("source_table = ? AND source_field = ?", rule.SourceTable, rule.SourceField).
		First(&existing)
	if result.Error == nil {
		return false, nil // Already exists
	}
	err := r.db.WithContext(ctx).Create(rule).Error
	return err == nil, err
}

func (r *MappingRuleRepo) GetAll(ctx context.Context, sourceTable *string) ([]model.MappingRule, error) {
	query := r.db.WithContext(ctx)
	if sourceTable != nil {
		query = query.Where("source_table = ?", *sourceTable)
	}
	var rules []model.MappingRule
	err := query.Order("source_table, source_field").Find(&rules).Error
	return rules, err
}
