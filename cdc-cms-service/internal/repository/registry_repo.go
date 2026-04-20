package repository

import (
	"context"

	"cdc-cms-service/internal/model"

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

func (r *RegistryRepo) GetBySourceTable(ctx context.Context, sourceTable string) (*model.TableRegistry, error) {
	var entry model.TableRegistry
	err := r.db.WithContext(ctx).Where("source_table = ?", sourceTable).First(&entry).Error
	return &entry, err
}

type RegistryFilter struct {
	SourceDB      *string
	SyncEngine    *string
	Priority      *string
	IsActive      *bool
	DestinationID *string
	Page          int
	PageSize      int
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

func (r *RegistryRepo) ScanRawKeys(ctx context.Context, targetTable string) ([]string, error) {
	var keys []string
	// SQL injection protection: targetTable comes from registry, but we should be careful.
	// Since targetTable is an identifier, we don't use parameter binding for it.
	query := `SELECT DISTINCT jsonb_object_keys(_raw_data) as key FROM "` + targetTable + `" LIMIT 100`
	err := r.db.WithContext(ctx).Raw(query).Scan(&keys).Error
	return keys, err
}

func (r *RegistryRepo) PerformBackfill(ctx context.Context, targetTable, sourceField, targetColumn, dataType string) (int64, error) {
	// Use Raw SQL for complex JSONB casting
	// Caution: targetTable and targetColumn should be validated against registry before calling this
	query := `UPDATE "` + targetTable + `" SET ` + targetColumn + ` = (_raw_data->>'` + sourceField + `')::` + dataType + ` WHERE ` + targetColumn + ` IS NULL`
	result := r.db.WithContext(ctx).Exec(query)
	return result.RowsAffected, result.Error
}

type DBColumn struct {
	ColumnName string `gorm:"column:column_name"`
	DataType   string `gorm:"column:data_type"`
}

func (r *RegistryRepo) GetDBColumns(ctx context.Context, tableName string) ([]DBColumn, error) {
	var columns []DBColumn
	query := `
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_name = ? AND table_schema = 'public'
	`
	err := r.db.WithContext(ctx).Raw(query, tableName).Scan(&columns).Error
	return columns, err
}

func (r *RegistryRepo) UpdateActiveStatusByTable(ctx context.Context, sourceTable string, isActive bool) error {
	return r.db.WithContext(ctx).Model(&model.TableRegistry{}).
		Where("source_table = ?", sourceTable).
		Update("is_active", isActive).Error
}

// CountActiveByConnectionID đếm số entries đang is_active=true trong cùng connection,
// loại trừ entry có excludeID (là entry đang được update).
// Dùng để quyết định connection status (active nếu còn ít nhất 1 entry active khác).
func (r *RegistryRepo) CountActiveByConnectionID(ctx context.Context, connectionID string, excludeID uint) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&model.TableRegistry{}).
		Where("airbyte_connection_id = ? AND is_active = ? AND id != ?", connectionID, true, excludeID).
		Count(&count).Error
	return count, err
}
