// Package persistence — pending_field_repo_gorm.go is the GORM-backed
// adapter for ports.PendingFieldRepo. SQL is lifted verbatim from the
// legacy `internal/repository/pending_field_repo.go` so the row shape
// and ordering remain byte-identical after Task #19 đợt B.
//
// Dead methods on the legacy struct (UpsertPendingField, GetTableColumns)
// are NOT migrated: in cdc-cms-service they have zero callers — Worker
// (centralized-data-service) keeps its own copy for Schema Inspector.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type pendingFieldRepoGorm struct {
	db *gorm.DB
}

// NewPendingFieldRepo constructs the GORM-backed adapter for
// ports.PendingFieldRepo.
func NewPendingFieldRepo(db *gorm.DB) ports.PendingFieldRepo {
	return &pendingFieldRepoGorm{db: db}
}

func (r *pendingFieldRepoGorm) GetByID(ctx context.Context, id uint) (*model.PendingField, error) {
	var pf model.PendingField
	err := r.db.WithContext(ctx).First(&pf, id).Error
	return &pf, err
}

func (r *pendingFieldRepoGorm) GetByStatus(ctx context.Context, status string, sourceDB *string, tableName *string, page, pageSize int) ([]model.PendingField, int64, error) {
	query := r.db.WithContext(ctx).Model(&model.PendingField{}).Where("status = ?", status)
	if sourceDB != nil {
		query = query.Where("source_db = ?", *sourceDB)
	}
	if tableName != nil {
		query = query.Where("table_name = ?", *tableName)
	}

	var total int64
	query.Count(&total)

	if pageSize <= 0 {
		pageSize = 20
	}
	if page <= 0 {
		page = 1
	}

	var fields []model.PendingField
	err := query.Offset((page - 1) * pageSize).Limit(pageSize).
		Order("detected_at DESC").Find(&fields).Error

	return fields, total, err
}

func (r *pendingFieldRepoGorm) Update(ctx context.Context, pf *model.PendingField) error {
	return r.db.WithContext(ctx).Save(pf).Error
}
