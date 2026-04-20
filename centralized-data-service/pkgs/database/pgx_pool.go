package database

import (
	"context"
	"fmt"

	"centralized-data-service/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPgxPool creates a dedicated pgx connection pool for high-throughput operations
// (CopyFrom, Batch). Runs alongside GORM for regular CRUD.
func NewPgxPool(ctx context.Context, cfg *config.AppConfig) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.DB.UserName, cfg.DB.Password, cfg.DB.Host, cfg.DB.Port, cfg.DB.Database, cfg.DB.SSLMode,
	)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool parse config: %w", err)
	}

	poolCfg.MaxConns = int32(cfg.DB.MaxOpenConn)
	poolCfg.MinConns = int32(cfg.DB.MaxIdleConn)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool connect: %w", err)
	}

	return pool, nil
}
