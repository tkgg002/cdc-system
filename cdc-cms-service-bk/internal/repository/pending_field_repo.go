package repository

import (
	"context"

	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type PendingFieldRepo struct {
	db *gorm.DB
}

func NewPendingFieldRepo(db *gorm.DB) *PendingFieldRepo {
	return &PendingFieldRepo{db: db}
}

func (r *PendingFieldRepo) GetByID(ctx context.Context, id uint) (*model.PendingField, error) {
	var pf model.PendingField
	err := r.db.WithContext(ctx).First(&pf, id).Error
	return &pf, err
}

func (r *PendingFieldRepo) GetByStatus(ctx context.Context, status string, sourceDB *string, tableName *string, page, pageSize int) ([]model.PendingField, int64, error) {
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

func (r *PendingFieldRepo) Update(ctx context.Context, pf *model.PendingField) error {
	return r.db.WithContext(ctx).Save(pf).Error
}

// UpsertPendingField inserts or increments detection_count
func (r *PendingFieldRepo) UpsertPendingField(ctx context.Context, tableName, sourceDB, fieldName, sampleValue, suggestedType string) error {
	sql := `
		INSERT INTO pending_fields (table_name, source_db, field_name, sample_value, suggested_type, detected_at, status, detection_count)
		VALUES (?, ?, ?, ?, ?, NOW(), 'pending', 1)
		ON CONFLICT (table_name, field_name) DO UPDATE SET
			detection_count = pending_fields.detection_count + 1,
			sample_value = EXCLUDED.sample_value,
			suggested_type = CASE
				WHEN pending_fields.detection_count < 5 THEN EXCLUDED.suggested_type
				ELSE pending_fields.suggested_type
			END
		WHERE pending_fields.status = 'pending'
	`
	return r.db.WithContext(ctx).Exec(sql, tableName, sourceDB, fieldName, sampleValue, suggestedType).Error
}

// GetTableColumns returns column names from information_schema
func (r *PendingFieldRepo) GetTableColumns(ctx context.Context, tableName string) (map[string]bool, error) {
	var columns []string
	err := r.db.WithContext(ctx).Raw(
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ?",
		tableName,
	).Scan(&columns).Error
	if err != nil {
		return nil, err
	}

	schema := make(map[string]bool, len(columns))
	for _, col := range columns {
		schema[col] = true
	}
	return schema, nil
}
