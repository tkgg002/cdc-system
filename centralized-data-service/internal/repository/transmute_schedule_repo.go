package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type TransmuteScheduleRepo struct {
	db *gorm.DB
}

func NewTransmuteScheduleRepo(db *gorm.DB) *TransmuteScheduleRepo {
	return &TransmuteScheduleRepo{db: db}
}

func (r *TransmuteScheduleRepo) GetByMasterBinding(ctx context.Context, masterBindingID int64) ([]model.TransmuteSchedule, error) {
	var items []model.TransmuteSchedule
	err := r.db.WithContext(ctx).
		Where("master_binding_id = ?", masterBindingID).
		Order("id").
		Find(&items).Error
	return items, err
}
