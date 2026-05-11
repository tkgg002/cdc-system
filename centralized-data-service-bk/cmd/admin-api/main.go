package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"centralized-data-service/config"
	"centralized-data-service/internal/admin"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	// DB — dùng ControlPlane DSN (cdc_dw) vì registry sống ở đó
	dbDSN := cfg.ControlPlaneURL()
	if dbDSN == "" {
		dbDSN = cfg.SystemDBURL()
	}
	// Env override cho standalone run
	if v := os.Getenv("ADMIN_DB_URL"); v != "" {
		dbDSN = v
	}

	db, err := gorm.Open(postgres.Open(dbDSN), &gorm.Config{})
	if err != nil {
		logger.Fatal("open control-plane db", zap.Error(err), zap.String("dsn_prefix", dbDSN[:min(len(dbDSN), 30)]))
	}

	// NATS
	natsURL := cfg.Nats.URL
	if v := os.Getenv("NATS_URL"); v != "" {
		natsURL = v
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatal("nats connect", zap.Error(err), zap.String("url", natsURL))
	}
	defer nc.Drain() //nolint:errcheck

	// Debezium + Schema Registry URLs (với env override)
	debeziumURL := getEnvOr("DEBEZIUM_URL", cfg.Debezium.KafkaConnectURL)
	if debeziumURL == "" {
		debeziumURL = "http://localhost:18083" // local docker default
	}
	schemaRegistryURL := getEnvOr("SCHEMA_REGISTRY_URL", cfg.Kafka.SchemaRegistryURL)
	if schemaRegistryURL == "" {
		schemaRegistryURL = "http://localhost:18081" // local docker default
	}

	addr := getEnvOr("ADMIN_API_LISTEN_ADDR", "127.0.0.1:8090")
	token := os.Getenv("ADMIN_API_TOKEN")
	devMode := os.Getenv("ADMIN_API_DEV") == "true"

	if token == "" && !devMode {
		logger.Fatal("ADMIN_API_TOKEN is empty and ADMIN_API_DEV != 'true' — refusing to start without auth. " +
			"Set ADMIN_API_TOKEN to a strong secret, or set ADMIN_API_DEV=true to explicitly opt into dev mode.")
	}
	if token == "" {
		logger.Warn("ADMIN_API_DEV=true — running without authentication. NEVER use this in production.")
	}

	srv := admin.NewServer(admin.Deps{
		DB:                db,
		NATS:              nc,
		DebeziumBaseURL:   debeziumURL,
		SchemaRegistryURL: schemaRegistryURL,
		AuthToken:         token,
		Logger:            logger,
	})

	logger.Info("cdc-admin-api starting",
		zap.String("addr", addr),
		zap.String("debezium", debeziumURL),
		zap.String("schema_registry", schemaRegistryURL),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx, addr); err != nil {
		logger.Error("admin-api stopped", zap.Error(err))
	}
}

func getEnvOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
