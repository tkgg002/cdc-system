package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"

	"go.uber.org/zap"
)

type MetadataRegistry interface {
	ReloadAll(ctx context.Context) error
	GetTableConfigByID(id uint) *model.TableRegistry
	GetTableConfig(targetTable string) *model.TableRegistry
	GetTableConfigBySource(sourceTable string) *model.TableRegistry
	ListTableConfigs() []model.TableRegistry
	GetMappingRules(targetTable string) []model.MappingRule
	GetDebeziumTables() []string
	ResolveSourceRoute(sourceDB, sourceTable string) *ResolvedSourceRoute
	// ResolveSourceRoutes returns master route + all logical-clone routes
	// for the given (sourceDB, sourceTable). Used by B3 fan-out dispatch.
	// Returns a slice of length >= 1 when the source is known, or nil.
	ResolveSourceRoutes(sourceDB, sourceTable string) []*ResolvedSourceRoute
	ResolveTargetRoute(targetTable string) *ResolvedSourceRoute
}

type ResolvedSourceRoute struct {
	SourceObject        *model.SourceObjectRegistry
	ShadowBinding       *model.ShadowBinding
	TableConfig         *model.TableRegistry
	ShadowConnectionKey string
}

// MetadataRegistryService is the V2-aware registry/cache layer.
//
// Phase scope:
//   - source/shadow routing is loaded from V2 tables
//   - mapping rules remain compatibility-backed from legacy cdc_mapping_rules
//     so the current shadow ingest path does not break while master-centric
//     V2 rules settle in later phases
type MetadataRegistryService struct {
	connectionRepo *repository.ConnectionRegistryRepo
	sourceRepo     *repository.SourceObjectRegistryRepo
	shadowRepo     *repository.ShadowBindingRepo
	legacyMapping  *repository.MappingRuleRepo
	logger         *zap.Logger

	mu             sync.RWMutex
	idCache        map[uint]*model.TableRegistry
	targetCache    map[string]*model.TableRegistry
	sourceCache    map[string]*model.TableRegistry
	routeCache     map[string]*ResolvedSourceRoute
	targetRouteMap map[string]*ResolvedSourceRoute
	mappingCache   map[string][]model.MappingRule
	debeziumTables []string
	// B3 fan-out: masterSourceID → clone routes (logical_clone_of key in source_locator_json).
	// Populated during ReloadAll. ResolveSourceRoutes returns master route + cloneRoutes.
	cloneRoutes    map[int64][]*ResolvedSourceRoute
}

func NewMetadataRegistryService(
	connectionRepo *repository.ConnectionRegistryRepo,
	sourceRepo *repository.SourceObjectRegistryRepo,
	shadowRepo *repository.ShadowBindingRepo,
	legacyMapping *repository.MappingRuleRepo,
	logger *zap.Logger,
) *MetadataRegistryService {
	rs := &MetadataRegistryService{
		connectionRepo: connectionRepo,
		sourceRepo:     sourceRepo,
		shadowRepo:     shadowRepo,
		legacyMapping:  legacyMapping,
		logger:         logger,
		idCache:        make(map[uint]*model.TableRegistry),
		targetCache:    make(map[string]*model.TableRegistry),
		sourceCache:    make(map[string]*model.TableRegistry),
		routeCache:     make(map[string]*ResolvedSourceRoute),
		targetRouteMap: make(map[string]*ResolvedSourceRoute),
		mappingCache:   make(map[string][]model.MappingRule),
		cloneRoutes:    make(map[int64][]*ResolvedSourceRoute),
	}
	if err := rs.ReloadAll(context.Background()); err != nil {
		logger.Error("failed to load initial V2 metadata registry", zap.Error(err))
	}
	return rs
}

func (rs *MetadataRegistryService) ReloadAll(ctx context.Context) error {
	sources, err := rs.sourceRepo.GetActive(ctx)
	if err != nil {
		return err
	}
	connections, err := rs.connectionRepo.GetAll(ctx)
	if err != nil {
		return err
	}

	var allBindings []model.ShadowBinding
	for _, src := range sources {
		items, listErr := rs.shadowRepo.ListBySourceObject(ctx, src.ID)
		if listErr != nil {
			return listErr
		}
		for _, item := range items {
			if item.IsActive {
				allBindings = append(allBindings, item)
			}
		}
	}

	rules, err := rs.legacyMapping.GetAllActive(ctx)
	if err != nil {
		return err
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.idCache = make(map[uint]*model.TableRegistry)
	rs.targetCache = make(map[string]*model.TableRegistry)
	rs.sourceCache = make(map[string]*model.TableRegistry)
	rs.routeCache = make(map[string]*ResolvedSourceRoute)
	rs.targetRouteMap = make(map[string]*ResolvedSourceRoute)
	rs.mappingCache = make(map[string][]model.MappingRule)
	rs.debeziumTables = rs.debeziumTables[:0]
	rs.cloneRoutes = make(map[int64][]*ResolvedSourceRoute)

	sourceByID := make(map[int64]*model.SourceObjectRegistry, len(sources))
	connectionCodeByID := make(map[int64]string, len(connections))
	sourceNameToTarget := make(map[string]string, len(sources))
	routeBySourceID := make(map[int64]*ResolvedSourceRoute, len(sources))

	for i := range sources {
		src := sources[i]
		sourceByID[src.ID] = &src
	}
	for i := range connections {
		connectionCodeByID[connections[i].ID] = strings.TrimSpace(connections[i].ConnectionCode)
	}

	for i := range allBindings {
		binding := allBindings[i]
		src := sourceByID[binding.SourceObjectID]
		if src == nil {
			rs.logger.Warn("shadow binding points to unknown source object",
				zap.Int64("source_object_id", binding.SourceObjectID),
				zap.String("binding_code", binding.BindingCode),
			)
			continue
		}

		cfg := synthesizeLegacyTableRegistry(src, &binding)
		route := &ResolvedSourceRoute{
			SourceObject:        src,
			ShadowBinding:       &binding,
			TableConfig:         cfg,
			ShadowConnectionKey: connectionCodeByID[binding.ShadowConnectionID],
		}

		rs.targetCache[cfg.TargetTable] = cfg
		rs.targetRouteMap[cfg.TargetTable] = route
		rs.idCache[cfg.ID] = cfg
		routeBySourceID[src.ID] = route
		sourceNameToTarget[strings.TrimSpace(src.SourceObjectName)] = cfg.TargetTable
		for _, sourceKey := range buildSourceLookupKeys(src) {
			if _, exists := rs.sourceCache[sourceKey]; !exists {
				rs.sourceCache[sourceKey] = cfg
			}
			if _, exists := rs.routeCache[sourceKey]; !exists {
				rs.routeCache[sourceKey] = route
			}
		}

		if strings.EqualFold(src.SyncEngine, "debezium") || strings.EqualFold(src.SyncEngine, "both") {
			rs.debeziumTables = appendIfMissing(rs.debeziumTables, strings.TrimSpace(src.SourceObjectName))
		}
	}

	// B3 fan-out: build cloneRoutes index.
	// For each source with logical_clone_of in source_locator_json, register its
	// route under the master source ID. ResolveSourceRoutes then returns both.
	for _, src := range sourceByID {
		masterID := extractLogicalCloneOf(src.SourceLocatorJSON)
		if masterID <= 0 {
			continue
		}
		cloneRoute, hasClone := routeBySourceID[src.ID]
		if !hasClone {
			continue // clone has no active shadow binding; skip
		}
		rs.cloneRoutes[masterID] = append(rs.cloneRoutes[masterID], cloneRoute)
		rs.logger.Debug("B3 fan-out: registered clone route",
			zap.Int64("master_source_id", masterID),
			zap.Int64("clone_source_id", src.ID),
			zap.String("clone_target_table", cloneRoute.TableConfig.TargetTable),
		)
	}

	for _, rule := range rules {
		targetTable := sourceNameToTarget[strings.TrimSpace(rule.SourceTable)]
		if targetTable == "" {
			rs.logger.Warn("legacy mapping rule has no V2 shadow route",
				zap.String("source_table", rule.SourceTable),
			)
			continue
		}
		rs.mappingCache[targetTable] = append(rs.mappingCache[targetTable], rule)
	}

	rs.logger.Info("V2 metadata registry reloaded",
		zap.Int("sources", len(sources)),
		zap.Int("connections", len(connections)),
		zap.Int("shadow_bindings", len(allBindings)),
		zap.Int("legacy_mapping_rules", len(rules)),
	)
	return nil
}

func (rs *MetadataRegistryService) GetTableConfigByID(id uint) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.idCache[id]
}

func (rs *MetadataRegistryService) GetTableConfig(targetTable string) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.targetCache[targetTable]
}

func (rs *MetadataRegistryService) GetTableConfigBySource(sourceTable string) *model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.sourceCache[strings.TrimSpace(sourceTable)]
}

func (rs *MetadataRegistryService) ListTableConfigs() []model.TableRegistry {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]model.TableRegistry, 0, len(rs.targetCache))
	for _, item := range rs.targetCache {
		if item == nil {
			continue
		}
		out = append(out, *item)
	}
	return out
}

func (rs *MetadataRegistryService) GetMappingRules(targetTable string) []model.MappingRule {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.mappingCache[targetTable]
}

func (rs *MetadataRegistryService) GetDebeziumTables() []string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	out := make([]string, len(rs.debeziumTables))
	copy(out, rs.debeziumTables)
	return out
}

func (rs *MetadataRegistryService) ResolveSourceRoute(sourceDB, sourceTable string) *ResolvedSourceRoute {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	for _, key := range buildRouteLookupKeys(sourceDB, sourceTable) {
		if route, ok := rs.routeCache[key]; ok {
			return route
		}
	}
	return nil
}

// ResolveSourceRoutes returns the master route for (sourceDB, sourceTable) plus
// all logical-clone routes registered via source_locator_json.logical_clone_of.
// Returns nil when the source is not in the registry.
func (rs *MetadataRegistryService) ResolveSourceRoutes(sourceDB, sourceTable string) []*ResolvedSourceRoute {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	var masterRoute *ResolvedSourceRoute
	for _, key := range buildRouteLookupKeys(sourceDB, sourceTable) {
		if route, ok := rs.routeCache[key]; ok {
			masterRoute = route
			break
		}
	}
	if masterRoute == nil {
		return nil
	}

	routes := []*ResolvedSourceRoute{masterRoute}

	// Append logical-clone routes keyed by this master's source ID.
	if masterRoute.SourceObject != nil {
		clones := rs.cloneRoutes[masterRoute.SourceObject.ID]
		routes = append(routes, clones...)
	}
	return routes
}

func (rs *MetadataRegistryService) ResolveTargetRoute(targetTable string) *ResolvedSourceRoute {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.targetRouteMap[strings.TrimSpace(targetTable)]
}

func synthesizeLegacyTableRegistry(src *model.SourceObjectRegistry, binding *model.ShadowBinding) *model.TableRegistry {
	sourceDB := ""
	if src.SourceDatabase != nil {
		sourceDB = strings.TrimSpace(*src.SourceDatabase)
	}

	sourceTable := strings.TrimSpace(src.SourceObjectName)
	if src.SourceSchema != nil && strings.TrimSpace(*src.SourceSchema) != "" {
		sourceTable = strings.TrimSpace(*src.SourceSchema) + "." + sourceTable
	}

	cfg := &model.TableRegistry{
		ID:              uint(src.ID),
		SourceDB:        sourceDB,
		SourceType:      normalizeEngineType(src.SourceEngineType),
		SourceTable:     sourceTable,
		TargetTable:     strings.TrimSpace(binding.ShadowTable),
		SyncEngine:      strings.TrimSpace(src.SyncEngine),
		PrimaryKeyField: strings.TrimSpace(src.PrimaryKeyField),
		IsActive:        src.IsActive && binding.IsActive,
		IsTableCreated:  strings.EqualFold(binding.DDLStatus, "created"),
		SyncStatus:      strings.TrimSpace(src.ProfileStatus),
		Notes:           src.Notes,
	}

	if cfg.PrimaryKeyField == "" {
		cfg.PrimaryKeyField = "id"
	}
	if src.PrimaryKeyType != nil {
		cfg.PrimaryKeyType = *src.PrimaryKeyType
	}
	if src.TimestampField != nil {
		cfg.TimestampField = src.TimestampField
	}
	if len(src.TimestampCandidatesJSON) > 0 {
		cfg.TimestampFieldCandidates = src.TimestampCandidatesJSON
	}
	return cfg
}

func buildSourceLookupKeys(src *model.SourceObjectRegistry) []string {
	keys := []string{
		strings.TrimSpace(src.SourceObjectName),
		fmt.Sprintf("%s|%s", optionalString(src.SourceDatabase), strings.TrimSpace(src.SourceObjectName)),
	}
	if src.SourceSchema != nil && strings.TrimSpace(*src.SourceSchema) != "" {
		qualified := strings.TrimSpace(*src.SourceSchema) + "." + strings.TrimSpace(src.SourceObjectName)
		keys = append(keys, qualified)
		keys = append(keys, fmt.Sprintf("%s|%s", optionalString(src.SourceDatabase), qualified))
	}
	return dedupeStrings(keys)
}

func buildRouteLookupKeys(sourceDB, sourceTable string) []string {
	sourceDB = strings.TrimSpace(sourceDB)
	sourceTable = strings.TrimSpace(sourceTable)
	return dedupeStrings([]string{
		sourceTable,
		fmt.Sprintf("%s|%s", sourceDB, sourceTable),
	})
}

func optionalString(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func dedupeStrings(items []string) []string {
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func appendIfMissing(items []string, item string) []string {
	item = strings.TrimSpace(item)
	if item == "" {
		return items
	}
	for _, existing := range items {
		if existing == item {
			return items
		}
	}
	return append(items, item)
}

func normalizeEngineType(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "postgresql":
		return "postgresql"
	case "mariadb":
		return "mariadb"
	case "mysql":
		return "mysql"
	case "mongodb":
		return "mongodb"
	default:
		return strings.ToLower(strings.TrimSpace(v))
	}
}

func decodeJSONToMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// extractLogicalCloneOf reads the `logical_clone_of` integer from a
// source_locator_json JSONB blob. Returns 0 when not present or invalid.
// B3 fan-out: clone sources declare their master ID via this key.
func extractLogicalCloneOf(raw json.RawMessage) int64 {
	m := decodeJSONToMap(raw)
	if m == nil {
		return 0
	}
	v, ok := m["logical_clone_of"]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	case json.Number:
		n, _ := val.Int64()
		return n
	}
	return 0
}
