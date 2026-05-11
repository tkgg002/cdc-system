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
	sqlDB.SetConnMaxIdleTime(30 * time.Minute)
	const warmupCount = 5
	for i := 0; i < warmupCount; i++ {
		if pingErr := sqlDB.Ping(); pingErr != nil {
			break
		}
	}
	_ = db.Exec("SELECT 1").Error

	return db, nil
}
