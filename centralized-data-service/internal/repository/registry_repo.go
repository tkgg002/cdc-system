package repository

import (
	"context"

	"centralized-data-service/internal/model"

	"gorm.io/gorm"
)

type RegistryRepo struct {
	db *gorm.DB
}

func NewRegistryRepo(db *gorm.DB) *RegistryRepo {
	return &RegistryRepo{db: db}
}

func (r *RegistryRepo) GetAllActive(ctx context.Context) ([]model.TableRegistry, error) {
	var entries []model.TableRegistry
	err := r.db.WithContext(ctx).Where("is_active = ?", true).Find(&entries).Error
	return entries, err
}

func (r *RegistryRepo) GetByID(ctx context.Context, id uint) (*model.TableRegistry, error) {
	var entry model.TableRegistry
	err := r.db.WithContext(ctx).First(&entry, id).Error
	return &entry, err
}

func (r *RegistryRepo) GetByTargetTable(ctx context.Context, targetTable string) (*model.TableRegistry, error) {
	var entry model.TableRegistry
	err := r.db.WithContext(ctx).Where("target_table = ?", targetTable).First(&entry).Error
	return &entry, err
}

type RegistryFilter struct {
	SourceDB   *string
	SyncEngine *string
	Priority   *string
	IsActive   *bool
	Page       int
	PageSize   int
}

func (r *RegistryRepo) GetAll(ctx context.Context, filter RegistryFilter) ([]model.TableRegistry, int64, error) {
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

func (r *RegistryRepo) Create(ctx context.Context, entry *model.TableRegistry) error {
	return r.db.WithContext(ctx).Create(entry).Error
}

func (r *RegistryRepo) Update(ctx context.Context, entry *model.TableRegistry) error {
	return r.db.WithContext(ctx).Save(entry).Error
}

func (r *RegistryRepo) BulkCreate(ctx context.Context, entries []model.TableRegistry) (int, error) {
	result := r.db.WithContext(ctx).CreateInBatches(entries, 50)
	return int(result.RowsAffected), result.Error
}

type RegistryStats struct {
	Total         int64          `json:"total"`
	BySourceDB    map[string]int `json:"by_source_db"`
	BySyncEngine  map[string]int `json:"by_sync_engine"`
	ByPriority    map[string]int `json:"by_priority"`
	TablesCreated int64          `json:"tables_created"`
}

func (r *RegistryRepo) GetStats(ctx context.Context) (*RegistryStats, error) {
	stats := &RegistryStats{
		BySourceDB:   make(map[string]int),
		BySyncEngine: make(map[string]int),
		ByPriority:   make(map[string]int),
	}

	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Count(&stats.Total)
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Where("is_table_created = ?", true).Count(&stats.TablesCreated)

	type GroupCount struct {
		Key   string
		Count int
	}

	var dbCounts []GroupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("source_db as key, count(*) as count").Group("source_db").Scan(&dbCounts)
	for _, c := range dbCounts {
		stats.BySourceDB[c.Key] = c.Count
	}

	var engineCounts []GroupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("sync_engine as key, count(*) as count").Group("sync_engine").Scan(&engineCounts)
	for _, c := range engineCounts {
		stats.BySyncEngine[c.Key] = c.Count
	}

	var priorityCounts []GroupCount
	r.db.WithContext(ctx).Model(&model.TableRegistry{}).Select("priority as key, count(*) as count").Group("priority").Scan(&priorityCounts)
	for _, c := range priorityCounts {
		stats.ByPriority[c.Key] = c.Count
	}

	return stats, nil
}
