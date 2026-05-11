package database

import (
	"fmt"
	"log"
	"os"
	"time"

	"cdc-cms-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewPostgresConnection(dbCfg config.DBConfig) (*gorm.DB, error) {
	// search_path=cdc_system,public lets GORM models with bare TableName
	// (e.g. "failed_sync_logs", "cdc_activity_log", "cdc_table_registry")
	// resolve to the cdc_system schema where the migrations create them.
	// Session-scoped via DSN — does NOT touch role search_path (role-level
	// search_path breaks migrations that create cross-schema partitioned
	// parents; see lessons.md "search_path role persistence trap").
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s search_path=cdc_system,public",
		dbCfg.Host, dbCfg.Port, dbCfg.UserName, dbCfg.Password, dbCfg.Database, dbCfg.SSLMode,
	)

	// IgnoreRecordNotFoundError=true: ErrRecordNotFound là branch logic
	// hợp lệ ở nhiều repo (alert dedup, upsert lookup, idempotent guards).
	// Default GORM logger in nó dưới dạng error đỏ → noise. Caller vẫn
	// nhận err qua Result.Error để switch — chỉ tắt LOG, không tắt err.
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:      gormLogger,
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
