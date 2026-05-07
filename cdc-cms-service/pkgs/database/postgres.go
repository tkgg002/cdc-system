package database

import (
	"fmt"
	"time"

	"cdc-cms-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewPostgresConnection(dbCfg config.DBConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		dbCfg.Host, dbCfg.Port, dbCfg.UserName, dbCfg.Password, dbCfg.Database, dbCfg.SSLMode,
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

	maxOpen := dbCfg.MaxOpenConn
	if maxOpen <= 0 {
		maxOpen = 25
	}
	maxIdle := dbCfg.MaxIdleConn
	if maxIdle <= 0 {
		maxIdle = 10
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	if dbCfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(dbCfg.ConnMaxLifetime)
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
