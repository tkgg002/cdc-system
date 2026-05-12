// Package bootstrap holds startup-time runtime seeds — idempotent rows
// that the V2 registry needs to function but that the embedded SQL
// migrations can't carry because the values depend on per-deployment
// config (host/port/database vary across environments).
package bootstrap

import (
	"context"
	"fmt"

	"cdc-cms-service/config"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// EnsureDefaultShadowConnection inserts (idempotent) the default shadow
// connection row into cdc_system.connection_registry. Without it the
// post-Register V2 sync silently drops every shadow_binding because
// resolveShadowConnectionID returns no row, and the /shadow UI list
// stays empty even after a successful Register.
//
// connection_code is fixed to "default_shadow" so re-runs are no-ops;
// host/port/database come from CMS_SHADOW_DB_* env vars (cfg.ShadowDB).
// secret_ref points at the env var that holds the password so a future
// secret-store rotation only rewrites this column.
func EnsureDefaultShadowConnection(
	ctx context.Context, db *gorm.DB, cfg config.DBConfig, logger *zap.Logger,
) error {
	if cfg.Host == "" {
		logger.Warn("shadow connection bootstrap skipped — cfg.ShadowDB.Host empty")
		return nil
	}
	err := db.WithContext(ctx).Exec(`
		INSERT INTO cdc_system.connection_registry
			(connection_code, display_name, role_type, engine_type,
			 host, port, default_database, secret_ref, status)
		VALUES ('default_shadow', 'Default Shadow Connection',
		        'shadow', 'postgresql',
		        ?, ?, ?, 'env:CMS_SHADOW_DB_PASSWORD', 'active')
		ON CONFLICT (connection_code) DO UPDATE
		   SET host             = EXCLUDED.host,
		       port             = EXCLUDED.port,
		       default_database = EXCLUDED.default_database,
		       status           = 'active',
		       updated_at       = NOW()
	`,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	).Error
	if err != nil {
		return fmt.Errorf("seed default_shadow connection: %w", err)
	}
	logger.Info("shadow connection seeded",
		zap.String("connection_code", "default_shadow"),
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database))
	return nil
}
