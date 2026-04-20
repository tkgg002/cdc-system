package service

import (
	"context"
	"sync"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
)

// RegistryService caches table registry and mapping rules in-memory for fast lookup
type RegistryService struct {
	registryRepo *repository.RegistryRepo
	mappingRepo  *repository.MappingRuleRepo
	logger       *zap.Logger

	mu            sync.RWMutex
	registryCache map[string]*model.TableRegistry // target_table → registry entry
	sourceCache   map[string]*model.TableRegistry // source_table → registry entry (reverse index)
	mappingCache  map[string][]model.MappingRule  // target_table → mapping rules
}

func NewRegistryService(
	regRepo *repository.RegistryRepo,
	mapRepo *repository.MappingRuleRepo,
	logger *zap.Logger,
) *RegistryService {
	rs := &RegistryService{
		registryRepo:  regRepo,
		mappingRepo:   mapRepo,
		logger:        logger,
		registryCache: make(map[string]*model.TableRegistry),
		sourceCache:   make(map[string]*model.TableRegistry),
		mappingCache:  make(map[string][]model.MappingRule),
	}
	if err := rs.ReloadAll(context.Background()); err != nil {
		logger.Error("failed to load initial registry", zap.Error(err))
	}
	return rs
}

func (rs *RegistryService) ReloadAll(ctx context.Context) error {
	entries, err := rs.registryRepo.GetAllActive(ctx)
	if err != nil {
		return err
	}

	rules, err := rs.mappingRepo.GetAllActive(ctx)
	if err != nil {
		return err
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	// 1. Rebuild registry cache (target_table → registry entry) + source reverse index
	rs.registryCache = make(map[string]*model.TableRegistry, len(entries))
	rs.sourceCache = make(map[string]*model.TableRegistry, len(entries))
	sourceToTarget := make(map[string]string, len(entries))
	for i := range entries {
		rs.registryCache[entries[i].TargetTable] = &entries[i]
		rs.sourceCache[entries[i].SourceTable] = &entries[i]
		sourceToTarget[entries[i].SourceTable] = entries[i].TargetTable
	}

	// 2. Rebuild mapping cache (target_table → mapping rules)
	rs.mappingCache = make(map[string][]model.MappingRule)
	for _, r := range rules {
		targetTable := sourceToTarget[r.SourceTable]
		if targetTable != "" {
			rs.mappingCache[targetTable] = append(rs.mappingCache[targetTable], r)
		} else {
			rs.logger.Warn("mapping rule for source table has no registry entry",
				zap.String("source_table", r.SourceTable),
			)
		}
	}

	rs.logger.Info("registry reloaded",
		zap.Int("tables", len(rs.registryCache)),
		zap.Int("mapping_rules", len(rules)),
	)
	return nil
}

func (rs *RegistryService) GetTableConfig(targetTable string) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.registryCache[targetTable]
}

func (rs *RegistryService) GetTableConfigBySource(sourceTable string) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.sourceCache[sourceTable]
}

func (rs *RegistryService) GetMappingRules(targetTable string) []model.MappingRule {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.mappingCache[targetTable]
}

// GetDebeziumTables returns source_table names where sync_engine = debezium or both
func (rs *RegistryService) GetDebeziumTables() []string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var tables []string
	for _, reg := range rs.registryCache {
		if reg.SyncEngine == "debezium" || reg.SyncEngine == "both" {
			tables = append(tables, reg.SourceTable)
		}
	}
	return tables
}
