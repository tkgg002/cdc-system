package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"centralized-data-service/config"
	"centralized-data-service/pkgs/database"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ConnectionManager caches GORM connections for the V2 database roles.
//
// Phase 01 split E2E (T-C4) — connection ownership now lives in
// pkgs/database.Registry. This manager is a thin façade so existing
// callers keep their (system / shadow[key] / master[key]) API while
// the underlying pools are built exactly once per physical instance.
//
// Routing rules:
//   - GetSystemDB                     → registry.GetDB("cdc")
//   - GetShadowDB(""/default key)     → registry.GetDB("shadow")
//   - GetShadowDB(other key)          → multi-tenant URL map (legacy)
//                                       fallback registry.GetDB("shadow")
//   - GetMasterDB(""/default key)     → registry.GetDB("dest")
//   - GetMasterDB(other key)          → multi-tenant URL map (legacy)
//
// Phase B5.5c (2026-05-05): "shadow" is now a first-class registry role
// (RoleShadow). When CDC_SHADOW_DB_URL is unset, the registry falls back
// to the control plane DSN — preserves legacy collocated behavior. When
// set (e.g. gpay-postgres-shadow split), shadow writes go to the new
// physical instance with zero call-site changes.
type ConnectionManager struct {
	cfg    *config.AppConfig
	logger *zap.Logger
	reg    *database.Registry

	mu        sync.Mutex
	shadowDBs map[string]*gorm.DB
	masterDBs map[string]*gorm.DB
}

func NewConnectionManager(cfg *config.AppConfig, logger *zap.Logger) *ConnectionManager {
	return NewConnectionManagerWithRegistry(cfg, logger, database.NewRegistry(cfg))
}

// NewConnectionManagerWithRegistry lets the caller share an existing
// Registry across services (e.g. so the worker bootstrap can also
// hand the same registry to non-CM consumers).
func NewConnectionManagerWithRegistry(cfg *config.AppConfig, logger *zap.Logger, reg *database.Registry) *ConnectionManager {
	if reg == nil {
		reg = database.NewRegistry(cfg)
	}
	return &ConnectionManager{
		cfg:       cfg,
		logger:    logger,
		reg:       reg,
		shadowDBs: make(map[string]*gorm.DB),
		masterDBs: make(map[string]*gorm.DB),
	}
}

// Registry exposes the underlying physical-instance registry so
// callers needing GetDB("cdc") / GetDB("dest") directly (or pgx
// pools) don't have to round-trip through the legacy API.
func (m *ConnectionManager) Registry() *database.Registry {
	return m.reg
}

func (m *ConnectionManager) GetSystemDB(ctx context.Context) (*gorm.DB, error) {
	_ = ctx
	return m.reg.GetDB(database.RoleControlPlane)
}

func (m *ConnectionManager) GetShadowDB(ctx context.Context, key string) (*gorm.DB, error) {
	_ = ctx
	if m.isDefaultKey(key, m.cfg.ShadowDBDefaultKey()) {
		return m.reg.GetDB(database.RoleShadow)
	}
	// Multi-tenant explicit override → per-key pool from URL map.
	if dsn := strings.TrimSpace(m.cfg.ShadowDBURLs()[strings.TrimSpace(key)]); dsn != "" {
		return m.getNamedDB("shadow", key, m.cfg.ShadowDBDefaultKey(), m.cfg.ShadowDBURLs(), m.shadowDBs)
	}
	// Phase B5.5c — connection_code key without explicit URL override
	// resolves to RoleShadow (data-lake instance). When CDC_SHADOW_DB_URL
	// is unset, RoleShadow itself falls back to control plane DSN — so
	// legacy collocated layouts still work without code changes.
	return m.reg.GetDB(database.RoleShadow)
}

func (m *ConnectionManager) GetMasterDB(ctx context.Context, key string) (*gorm.DB, error) {
	_ = ctx
	if m.isDefaultKey(key, m.cfg.MasterDBDefaultKey()) {
		return m.reg.GetDB(database.RoleDestination)
	}
	if dsn := strings.TrimSpace(m.cfg.MasterDBURLs()[strings.TrimSpace(key)]); dsn != "" {
		return m.getNamedDB("master", key, m.cfg.MasterDBDefaultKey(), m.cfg.MasterDBURLs(), m.masterDBs)
	}
	// Same fallback as GetShadowDB — single goopay_dest instance owns
	// every dw_<binding>.* in the split layout.
	return m.reg.GetDB(database.RoleDestination)
}

func (m *ConnectionManager) ShadowKeys() []string {
	return sortedDBKeys(m.cfg.ShadowDBURLs())
}

func (m *ConnectionManager) MasterKeys() []string {
	return sortedDBKeys(m.cfg.MasterDBURLs())
}

// isDefaultKey reports whether the caller asked for the default
// key (empty or matching cfg.*DBDefaultKey). Default keys are
// served by the physical-instance registry; only true multi-tenant
// keys fall through to the legacy URL-map cache.
func (m *ConnectionManager) isDefaultKey(key, defaultKey string) bool {
	k := strings.TrimSpace(key)
	if k == "" {
		return true
	}
	return k == strings.TrimSpace(defaultKey)
}

func (m *ConnectionManager) getNamedDB(
	role, key, defaultKey string,
	urls map[string]string,
	cache map[string]*gorm.DB,
) (*gorm.DB, error) {
	selectedKey := strings.TrimSpace(key)
	if selectedKey == "" {
		selectedKey = strings.TrimSpace(defaultKey)
	}
	if selectedKey == "" {
		selectedKey = "default"
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if db, ok := cache[selectedKey]; ok && db != nil {
		return db, nil
	}

	dsn, ok := urls[selectedKey]
	if !ok || strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("%s db key %q not configured", role, selectedKey)
	}

	db, err := database.NewPostgresConnectionByDSN(m.cfg, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s db %q: %w", role, selectedKey, err)
	}

	cache[selectedKey] = db
	m.logger.Info("named db connection initialized (multi-tenant)",
		zap.String("role", role),
		zap.String("key", selectedKey),
	)
	return db, nil
}

func sortedDBKeys(items map[string]string) []string {
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
