package persistence

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type RegistryRepo struct {
	db *gorm.DB
}

func NewRegistryRepo(db *gorm.DB) *RegistryRepo {
	return &RegistryRepo{db: db}
}

var _ ports.RegistryRepo = (*RegistryRepo)(nil)

func (r *RegistryRepo) GetByID(ctx context.Context, id uint) (*model.TableRegistry, error) {
	var entry model.TableRegistry
	err := r.db.WithContext(ctx).First(&entry, id).Error
	return &entry, err
}

func (r *RegistryRepo) GetAll(ctx context.Context, filter ports.RegistryFilter) ([]model.TableRegistry, int64, error) {
	query := r.db.WithContext(ctx).Model(&model.TableRegistry{})

	if filter.SourceDB != nil {
		query = query.Where("source_db = ?", *filter.SourceDB)
	}
	if filter.SyncEngine != nil {
		query = query.Where("sync_engine = ?", *filter.SyncEngine)
	}
	if filter.Priority != nil {
		query = query.Where("priority = ?", *filter.Priority)
	}
	if filter.IsActive != nil {
		query = query.Where("is_active = ?", *filter.IsActive)
	}
	if filter.DestinationID != nil {
		query = query.Where("airbyte_destination_id = ?", *filter.DestinationID)
	}

	var total int64
	query.Count(&total)

	if filter.PageSize <= 0 {
		filter.PageSize = 20
	}
	if filter.Page <= 0 {
		filter.Page = 1
	}

	var entries []model.TableRegistry
	err := query.Offset((filter.Page - 1) * filter.PageSize).Limit(filter.PageSize).
		Order("source_db, source_table").Find(&entries).Error

	return entries, total, err
}

func (r *RegistryRepo) GetStats(ctx context.Context) (*ports.RegistryStats, error) {
	stats := &ports.RegistryStats{
		BySourceDB:   make(map[string]int),
		BySyncEngine: make(map[string]int),
		ByPriority:   make(map[string]int),
	}

	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Count(&stats.Total)
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Where("is_table_created = ?", true).Count(&stats.TablesCreated)

	type groupCount struct {
		Key   string
		Count int
	}

	var dbCounts []groupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("source_db as key, count(*) as count").Group("source_db").Scan(&dbCounts)
	for _, c := range dbCounts {
		stats.BySourceDB[c.Key] = c.Count
	}

	var engineCounts []groupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("sync_engine as key, count(*) as count").Group("sync_engine").Scan(&engineCounts)
	for _, c := range engineCounts {
		stats.BySyncEngine[c.Key] = c.Count
	}

	var priorityCounts []groupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("priority as key, count(*) as count").Group("priority").Scan(&priorityCounts)
	for _, c := range priorityCounts {
		stats.ByPriority[c.Key] = c.Count
	}

	return stats, nil
}
