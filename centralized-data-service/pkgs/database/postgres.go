package database

import (
	"fmt"
	"time"

	"centralized-data-service/config"

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
	if cfg.Server.Mode == "debug" {
		logLevel = logger.Info
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	sqlDB.SetMaxOpenConns(cfg.DB.MaxOpenConn)
	sqlDB.SetMaxIdleConns(cfg.DB.MaxIdleConn)
	if cfg.DB.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.DB.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(5 * time.Minute)
	}

	return db, nil
}

// NewPostgresReadReplica opens a second pool pointed at
// `cfg.DB.ReadReplicaDSN`. Returns (nil, nil) when no replica is
// configured — callers must treat that as "reuse primary".
//
// The replica pool uses a smaller size by default since it is only
// used for Recon reads which don't need the full primary fan-out.
func NewPostgresReadReplica(cfg *config.AppConfig) (*gorm.DB, error) {
	dsn := cfg.DB.ReadReplicaDSN
	if dsn == "" {
		return nil, nil
	}

	logLevel := logger.Warn
	if cfg.Server.Mode == "debug" {
		logLevel = logger.Info
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect postgres replica: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB (replica): %w", err)
	}

	// Replica pool sized at half the primary — Recon traffic is smaller
	// than live CDC write throughput.
	maxOpen := cfg.DB.MaxOpenConn / 2
	if maxOpen < 5 {
		maxOpen = 5
	}
	maxIdle := cfg.DB.MaxIdleConn / 2
	if maxIdle < 2 {
		maxIdle = 2
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	if cfg.DB.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.DB.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(5 * time.Minute)
	}

	return db, nil
}
