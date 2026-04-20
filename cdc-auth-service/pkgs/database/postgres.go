package database

import (
	"fmt"
	"time"

	"cdc-auth-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewPostgresConnection(cfg *config.AppConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.DB.Host, cfg.DB.Port, cfg.DB.UserName, cfg.DB.Password, cfg.DB.Database, cfg.DB.SSLMode,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
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
