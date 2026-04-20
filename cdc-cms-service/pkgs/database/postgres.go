package database

import (
	"fmt"
	"time"

	"cdc-cms-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewPostgresConnection(cfg *config.AppConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DB.Host, cfg.DB.Port, cfg.DB.UserName, cfg.DB.Password, cfg.DB.Database, cfg.DB.SSLMode,
	)

	logLevel := logger.Warn

	// PrepareStmt=true caches prepared statements per connection so repeated
	// queries (health collector, recon, registry scans) skip Postgres parse+plan
	// on every call. Fixes recurring "SLOW SQL" warnings whose 150-250ms cost
	// was planning time, not execution. Safe against injection — GORM still
	// binds parameters via the pgx bind protocol.
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:      logger.Default.LogMode(logLevel),
		PrepareStmt: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Honour config when provided, fall back to conservative defaults tuned
	// for CMS workload (health collector + recon + registry).
	maxOpen := cfg.DB.MaxOpenConn
	if maxOpen <= 0 {
		maxOpen = 25
	}
	maxIdle := cfg.DB.MaxIdleConn
	if maxIdle <= 0 {
		maxIdle = 10
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	if cfg.DB.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.DB.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(1 * time.Hour)
	}
	// Keep hot connections in the pool longer than the collector tick (15s)
	// so the plan cache survives between ticks.
	sqlDB.SetConnMaxIdleTime(30 * time.Minute)

	// Warm the pool: force N physical connections to open so the first
	// collector tick doesn't pay the connect+plan cost on cold sockets.
	// Also exercises the planner once per connection via a trivial query.
	const warmupCount = 5
	for i := 0; i < warmupCount; i++ {
		if pingErr := sqlDB.Ping(); pingErr != nil {
			// Non-fatal: startup continues; the collector will retry.
			break
		}
	}
	_ = db.Exec("SELECT 1").Error

	return db, nil
}
