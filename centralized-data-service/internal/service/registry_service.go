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

func (rs *RegistryService) ResolveSourceRoute(sourceDB, sourceTable string) *ResolvedSourceRoute {
	_ = sourceDB
	cfg := rs.GetTableConfigBySource(sourceTable)
	if cfg == nil {
		return nil
	}
	return &ResolvedSourceRoute{TableConfig: cfg}
}

// ResolveSourceRoutes returns the single route for the legacy RegistryService.
// Legacy V1 service has no logical-clone concept; returns master route only.
func (rs *RegistryService) ResolveSourceRoutes(sourceDB, sourceTable string) []*ResolvedSourceRoute {
	route := rs.ResolveSourceRoute(sourceDB, sourceTable)
	if route == nil {
		return nil
	}
	return []*ResolvedSourceRoute{route}
}

func (rs *RegistryService) ResolveTargetRoute(targetTable string) *ResolvedSourceRoute {
	cfg := rs.GetTableConfig(targetTable)
	if cfg == nil {
		return nil
	}
	return &ResolvedSourceRoute{TableConfig: cfg}
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

func (rs *RegistryService) GetTableConfigByID(id uint) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	for _, item := range rs.registryCache {
		if item != nil && item.ID == id {
			return item
		}
	}
	return nil
}

func (rs *RegistryService) GetTableConfigBySource(sourceTable string) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.sourceCache[sourceTable]
}

func (rs *RegistryService) ListTableConfigs() []model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]model.TableRegistry, 0, len(rs.registryCache))
	for _, item := range rs.registryCache {
		if item == nil {
			continue
		}
		out = append(out, *item)
	}
	return out
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

// DebeziumNamespace identifies a CDC source object by the full
// `(engine, database, namespace, object)` tuple. Multi-engine
// unified pipeline uses this so the consumer can disambiguate when
// two engines happen to expose objects under the same name (e.g.
// PG `public.orders` vs Mongo `payment-bill-service.orders`).
//
// Engine    — `postgresql` | `mongodb` | `mysql` (mariadb-via-mysql).
// Database  — Debezium `database.server.name` (PG/MySQL) or
//             Mongo database name.
// Namespace — PG schema (`public`); for Mongo/MySQL identical to
//             Database (kept separate for future homogenisation).
// Object    — table or collection name.
type DebeziumNamespace struct {
	Engine    string
	Database  string
	Namespace string
	Object    string
}

// GetDebeziumNamespaces returns one DebeziumNamespace tuple per
// active V1 registry row whose sync_engine is `debezium` or `both`.
//
// Multi-engine unified pipeline (Phase suffix `multi_engine_unified`).
// V1 registry has no explicit `namespace` column; we synthesise it
// from `source_db` for non-PG engines so callers always receive a
// 4-tuple.
func (rs *RegistryService) GetDebeziumNamespaces() []DebeziumNamespace {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]DebeziumNamespace, 0, len(rs.registryCache))
	for _, reg := range rs.registryCache {
		if reg == nil {
			continue
		}
		if reg.SyncEngine != "debezium" && reg.SyncEngine != "both" {
			continue
		}
		ns := reg.SourceDB
		if reg.SourceType == "postgresql" || reg.SourceType == "postgres" {
			// PG default schema; richer schema discovery is a V2 concern
			// and lives in source_object_registry. V1 callers only need
			// "is this object under PG?" — `public` is the safe default.
			ns = "public"
		}
		out = append(out, DebeziumNamespace{
			Engine:    reg.SourceType,
			Database:  reg.SourceDB,
			Namespace: ns,
			Object:    reg.SourceTable,
		})
	}
	return out
}
