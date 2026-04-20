package repository

import (
	"context"

	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type MappingRuleRepo struct {
	db *gorm.DB
}

func NewMappingRuleRepo(db *gorm.DB) *MappingRuleRepo {
	return &MappingRuleRepo{db: db}
}

func (r *MappingRuleRepo) GetByID(ctx context.Context, id uint) (*model.MappingRule, error) {
	var rule model.MappingRule
	err := r.db.WithContext(ctx).First(&rule, id).Error
	return &rule, err
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

func (r *MappingRuleRepo) GetAll(ctx context.Context, sourceTable *string) ([]model.MappingRule, error) {
	query := r.db.WithContext(ctx)
	if sourceTable != nil {
		query = query.Where("source_table = ?", *sourceTable)
	}
	var rules []model.MappingRule
	err := query.Order("source_table, source_field").Find(&rules).Error
	return rules, err
}

// GetAllFiltered supports filtering by table, status, rule_type
func (r *MappingRuleRepo) GetAllFiltered(ctx context.Context, sourceTable, status, ruleType *string) ([]model.MappingRule, error) {
	query := r.db.WithContext(ctx)
	if sourceTable != nil && *sourceTable != "" {
		query = query.Where("source_table = ?", *sourceTable)
	}
	if status != nil && *status != "" {
		query = query.Where("status = ?", *status)
	}
	if ruleType != nil && *ruleType != "" {
		query = query.Where("rule_type = ?", *ruleType)
	}
	var rules []model.MappingRule
	err := query.Order("source_table, source_field").Find(&rules).Error
	return rules, err
}

// GetAllFilteredPaginated supports filtering + pagination, returns (rules, totalCount, error)
func (r *MappingRuleRepo) GetAllFilteredPaginated(ctx context.Context, sourceTable, status, ruleType *string, page, pageSize int) ([]model.MappingRule, int64, error) {
	query := r.db.WithContext(ctx).Model(&model.MappingRule{})
	if sourceTable != nil && *sourceTable != "" {
		query = query.Where("source_table = ?", *sourceTable)
	}
	if status != nil && *status != "" {
		query = query.Where("status = ?", *status)
	}
	if ruleType != nil && *ruleType != "" {
		query = query.Where("rule_type = ?", *ruleType)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var rules []model.MappingRule
	offset := (page - 1) * pageSize
	err := query.Order("source_table, source_field").Offset(offset).Limit(pageSize).Find(&rules).Error
	return rules, total, err
}

// CreateIfNotExists inserts a mapping rule only if (source_table, source_field) doesn't exist
func (r *MappingRuleRepo) CreateIfNotExists(ctx context.Context, rule *model.MappingRule) (created bool, err error) {
	var existing model.MappingRule
	result := r.db.WithContext(ctx).
		Where("source_table = ? AND source_field = ?", rule.SourceTable, rule.SourceField).
		First(&existing)
	if result.Error == nil {
		// Already exists — skip
		return false, nil
	}
	// Not found — create
	err = r.db.WithContext(ctx).Create(rule).Error
	return err == nil, err
}

// UpdateStatus updates the status of a mapping rule
func (r *MappingRuleRepo) UpdateStatus(ctx context.Context, id uint, status string, updatedBy string) error {
	return r.db.WithContext(ctx).Model(&model.MappingRule{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{"status": status, "updated_by": updatedBy}).Error
}

