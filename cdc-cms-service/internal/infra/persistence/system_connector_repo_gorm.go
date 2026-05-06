// Package persistence — system_connector_repo_gorm.go is the GORM-
// backed adapter for ports.SystemConnectorRepo (the Connection-
// Fingerprint registry, table `cdc_sources`). SQL is lifted verbatim
// from the legacy `internal/repository/source_repo.go` so the upsert
// column-list and ordering remain byte-identical after Task #19 đợt D.
//
// The legacy `GetByConnectorName` method is NOT migrated — zero
// callers in CMS at the time of move.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type systemConnectorRepoGorm struct {
	db *gorm.DB
}

// NewSystemConnectorRepo constructs the GORM-backed adapter for
// ports.SystemConnectorRepo.
func NewSystemConnectorRepo(db *gorm.DB) ports.SystemConnectorRepo {
	return &systemConnectorRepoGorm{db: db}
}

func (r *systemConnectorRepoGorm) Upsert(ctx context.Context, s *model.Source) error {
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "connector_name"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"source_type", "connector_class", "topic_prefix", "server_address",
			"database_include_list", "collection_include_list", "raw_config_sanitized",
			"status", "updated_at",
		}),
	}).Create(s).Error
}

func (r *systemConnectorRepoGorm) List(ctx context.Context) ([]model.Source, error) {
	var out []model.Source
	err := r.db.WithContext(ctx).
		Where("status != ?", "deleted").
		Order("created_at DESC").
		Find(&out).Error
	return out, err
}

func (r *systemConnectorRepoGorm) GetByID(ctx context.Context, id int64) (*model.Source, error) {
	var s model.Source
	err := r.db.WithContext(ctx).First(&s, id).Error
	return &s, err
}

func (r *systemConnectorRepoGorm) MarkDeleted(ctx context.Context, connectorName string) error {
	return r.db.WithContext(ctx).Model(&model.Source{}).
		Where("connector_name = ?", connectorName).
		Update("status", "deleted").Error
}
