package database

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"centralized-data-service/config"

	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Phase 01 split E2E (T-C3) — physical-instance database registry.
// Phase B5.5c (2026-05-05) — shadow promoted to first-class role,
// resolved from cfg.ShadowDB.URLs[default-key]. Falls back to control
// plane DSN when shadow is unconfigured (single-PG-pair dev).
//
// Three roles are exposed:
//
//	"cdc"    → control plane DSN (gpay-postgres-cdc / cdc_dw).
//	           Owns cdc_system.* registry tables.
//	"shadow" → data-lake DSN (gpay-postgres-shadow / cdc_shadow when
//	           split; falls back to "cdc" when not configured).
//	           Owns shadow_<src>.* writes per shadow_binding.
//	"dest"   → destination DSN (gpay-postgres-dest / goopay_dest).
//	           Owns master + dw_<binding>.* — Worker's Swap step (Step 11)
//	           lands here.
//
// The registry is goroutine-safe and creates exactly ONE pool per
// (role, driver) — pgx and GORM each have an independent pool, so
// callers that need raw pgx (e.g. CopyFrom) and callers that need
// GORM share connections within their own driver but never leak
// connections across roles.

// RoleControlPlane is the registry role used to look up the cdc_dw
// pool (control plane + shadow).
const RoleControlPlane = "cdc"

// RoleShadow is the registry role used to look up the shadow / data-lake
// pool. Reads cfg.ShadowDB.URLs[default-key]; falls back to the control
// plane DSN when no shadow DSN is configured (legacy collocated layout).
const RoleShadow = "shadow"

// RoleDestination is the registry role used to look up the goopay_dest
// pool (master + dw_<binding>).
const RoleDestination = "dest"

// Registry caches GORM and pgx pools per role. The zero value is not
// usable — always construct via NewRegistry.
type Registry struct {
	cfg *config.AppConfig

	mu       sync.RWMutex
	gormDBs  map[string]*gorm.DB
	pgxPools map[string]*pgxpool.Pool

	// initOnce gates the eager Init() call so concurrent callers
	// don't double-build pools.
	initOnce sync.Once
	initErr  error
}

// NewRegistry returns a Registry bound to cfg. It does not open
// connections — call Init or one of the Get* methods to materialize
// pools lazily.
func NewRegistry(cfg *config.AppConfig) *Registry {
	return &Registry{
		cfg:      cfg,
		gormDBs:  make(map[string]*gorm.DB),
		pgxPools: make(map[string]*pgxpool.Pool),
	}
}

// Init eagerly opens both control-plane and destination pools so a
// failure to connect surfaces at boot rather than on first use. Safe
// to call concurrently — only the first call dials; subsequent calls
// return the cached error (if any).
func (r *Registry) Init(ctx context.Context) error {
	r.initOnce.Do(func() {
		if _, err := r.GetDB(RoleControlPlane); err != nil {
			r.initErr = fmt.Errorf("init %s: %w", RoleControlPlane, err)
			return
		}
		if _, err := r.GetDB(RoleDestination); err != nil {
			r.initErr = fmt.Errorf("init %s: %w", RoleDestination, err)
			return
		}
		if _, err := r.GetPgxPool(ctx, RoleControlPlane); err != nil {
			r.initErr = fmt.Errorf("init pgx %s: %w", RoleControlPlane, err)
			return
		}
		if _, err := r.GetPgxPool(ctx, RoleDestination); err != nil {
			r.initErr = fmt.Errorf("init pgx %s: %w", RoleDestination, err)
			return
		}
	})
	return r.initErr
}

// GetDB returns a cached GORM connection for the given role. Roles
// are RoleControlPlane ("cdc") and RoleDestination ("dest"). Calls
// are thread-safe; the underlying pool is built once per role.
func (r *Registry) GetDB(role string) (*gorm.DB, error) {
	role = strings.TrimSpace(role)
	if role == "" {
		return nil, fmt.Errorf("multi.GetDB: role is empty")
	}

	r.mu.RLock()
	if db, ok := r.gormDBs[role]; ok && db != nil {
		r.mu.RUnlock()
		return db, nil
	}
	r.mu.RUnlock()

	dsn, err := r.dsnForRole(role)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check after acquiring write lock — another goroutine may
	// have populated the cache in the meantime.
	if db, ok := r.gormDBs[role]; ok && db != nil {
		return db, nil
	}

	db, err := r.openGorm(dsn)
	if err != nil {
		return nil, fmt.Errorf("multi.GetDB(%s): %w", role, err)
	}
	r.gormDBs[role] = db
	return db, nil
}

// GetPgxPool returns a cached pgxpool.Pool for the given role. Used
// by high-throughput paths (CopyFrom, Batch). Pools are independent
// per role — a saturated cdc pool will not starve dest writes.
func (r *Registry) GetPgxPool(ctx context.Context, role string) (*pgxpool.Pool, error) {
	role = strings.TrimSpace(role)
	if role == "" {
		return nil, fmt.Errorf("multi.GetPgxPool: role is empty")
	}

	r.mu.RLock()
	if p, ok := r.pgxPools[role]; ok && p != nil {
		r.mu.RUnlock()
		return p, nil
	}
	r.mu.RUnlock()

	dsn, err := r.dsnForRole(role)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.pgxPools[role]; ok && p != nil {
		return p, nil
	}

	pool, err := r.openPgx(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("multi.GetPgxPool(%s): %w", role, err)
	}
	r.pgxPools[role] = pool
	return pool, nil
}

// Close drains every pool the registry has opened. Idempotent. Errors
// from individual pools are logged via the closing pool itself; this
// method always returns nil so callers can defer it.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for role, pool := range r.pgxPools {
		if pool != nil {
			pool.Close()
		}
		delete(r.pgxPools, role)
	}
	for role, db := range r.gormDBs {
		if db != nil {
			if sqlDB, err := db.DB(); err == nil {
				_ = sqlDB.Close()
			}
		}
		delete(r.gormDBs, role)
	}
}

func (r *Registry) dsnForRole(role string) (string, error) {
	switch role {
	case RoleControlPlane:
		dsn := r.cfg.ControlPlaneURL()
		if dsn == "" {
			return "", fmt.Errorf("multi: control plane DSN is empty (set controlPlane.url or CDC_CONTROL_PLANE_URL)")
		}
		return dsn, nil
	case RoleDestination:
		// Architect ruling (Track D Hardening Q1=a): RoleDestination
		// is the physical pool for masterDb.default. Read straight
		// from MasterDB.URLs to keep the single source of truth — no
		// indirection through a separate destination block.
		key := strings.TrimSpace(r.cfg.MasterDB.DefaultKey)
		if key == "" {
			key = "default"
		}
		dsn := ""
		if r.cfg.MasterDB.URLs != nil {
			dsn = strings.TrimSpace(r.cfg.MasterDB.URLs[key])
		}
		if dsn == "" {
			return "", fmt.Errorf("multi: destination DSN is empty (set masterDb.urls.%s or CDC_MASTER_DB_URL / CDC_DESTINATION_URL)", key)
		}
		return dsn, nil
	case RoleShadow:
		// Phase B5.5c — shadow as first-class role. Resolves from
		// cfg.ShadowDB.URLs[default-key]; if missing or matches control
		// plane DSN, fall through to RoleControlPlane (legacy collocated
		// layout where shadow_*.* lives inside cdc_dw).
		key := strings.TrimSpace(r.cfg.ShadowDB.DefaultKey)
		if key == "" {
			key = "default"
		}
		if r.cfg.ShadowDB.URLs != nil {
			if dsn := strings.TrimSpace(r.cfg.ShadowDB.URLs[key]); dsn != "" {
				return dsn, nil
			}
		}
		// Backwards compat fallback — shadow not split yet.
		return r.dsnForRole(RoleControlPlane)
	default:
		return "", fmt.Errorf("multi: unknown role %q (want %q, %q, or %q)", role, RoleControlPlane, RoleShadow, RoleDestination)
	}
}

func (r *Registry) openGorm(dsn string) (*gorm.DB, error) {
	logLevel := logger.Warn
	if r.cfg.Server.Mode == "debug" {
		logLevel = logger.Info
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("gorm open: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(r.cfg.DB.MaxOpenConn)
	sqlDB.SetMaxIdleConns(r.cfg.DB.MaxIdleConn)
	if r.cfg.DB.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(r.cfg.DB.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(5 * time.Minute)
	}

	return db, nil
}

func (r *Registry) openPgx(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool parse: %w", err)
	}
	poolCfg.MaxConns = int32(r.cfg.DB.MaxOpenConn)
	poolCfg.MinConns = int32(r.cfg.DB.MaxIdleConn)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool connect: %w", err)
	}
	return pool, nil
}
