package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"cdc-cms-service/config"
	_ "cdc-cms-service/docs" // Swagger generated docs
	"cdc-cms-service/internal/server"
	"cdc-cms-service/pkgs/observability"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// @title           CDC CMS Service API
// @version         1.0
// @description     CMS Service for CDC Integration - Schema change approval, table registry, mapping rules management.
// @host            localhost:8080
// @BasePath        /
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Enter "Bearer {token}" to authenticate
func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting CMS Service",
		zap.String("service", cfg.Server.Name),
		zap.String("port", cfg.Server.Port),
	)

	// Initialize OpenTelemetry (traces + logs)
	otelShutdown, err := observability.InitOtel(observability.OtelConfig{
		Enabled:     cfg.Otel.Enabled,
		ServiceName: cfg.Otel.ServiceName,
		Endpoint:    cfg.Otel.Endpoint,
		SampleRatio: cfg.Otel.SampleRatio,
	}, logger)
	if err != nil {
		logger.Warn("OpenTelemetry init failed, tracing disabled", zap.Error(err))
	}
	defer otelShutdown()

	// Wire OTel zap bridge: logs go to both console AND SigNoz
	if lp := observability.LogProvider(); lp != nil {
		otelCore := otelzap.NewCore("cdc-cms", otelzap.WithLoggerProvider(lp))
		logger = zap.New(zapcore.NewTee(logger.Core(), otelCore))
		logger.Info("OTel zap bridge active — logs forwarding to SigNoz")
	}

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Fatal("failed to initialize server", zap.Error(err))
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		srv.Shutdown()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		logger.Fatal("server failed", zap.Error(err))
	}
}
