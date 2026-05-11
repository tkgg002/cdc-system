package repository

import (
	"context"

	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SourceRepo struct{ db *gorm.DB }

func NewSourceRepo(db *gorm.DB) *SourceRepo { return &SourceRepo{db: db} }

// Upsert by connector_name. Safe to retry — mirrors NF1 idempotency.
func (r *SourceRepo) Upsert(ctx context.Context, s *model.Source) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "connector_name"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"source_type", "connector_class", "topic_prefix", "server_address",
			"database_include_list", "collection_include_list", "raw_config_sanitized",
			"status", "updated_at",
		}),
	}).Create(s).Error
}

func (r *SourceRepo) List(ctx context.Context) ([]model.Source, error) {
	var out []model.Source
	err := r.db.WithContext(ctx).
		Where("status != ?", "deleted").
		Order("created_at DESC").
		Find(&out).Error
	return out, err
}

func (r *SourceRepo) GetByID(ctx context.Context, id int64) (*model.Source, error) {
	var s model.Source
	err := r.db.WithContext(ctx).First(&s, id).Error
	return &s, err
}

func (r *SourceRepo) GetByConnectorName(ctx context.Context, name string) (*model.Source, error) {
	var s model.Source
	err := r.db.WithContext(ctx).Where("connector_name = ?", name).First(&s).Error
	return &s, err
}

func (r *SourceRepo) MarkDeleted(ctx context.Context, connectorName string) error {
	return r.db.WithContext(ctx).Model(&model.Source{}).
		Where("connector_name = ?", connectorName).
		Update("status", "deleted").Error
}
