package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"cdc-auth-service/config"
	_ "cdc-auth-service/docs" // Swagger generated docs
	"cdc-auth-service/internal/server"

	"go.uber.org/zap"
)

// @title           CDC Auth Service API
// @version         1.0
// @description     Authentication service for CDC Integration — Login, Register, JWT token management.
// @host            localhost:8081
// @BasePath        /
func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting Auth Service",
		zap.String("service", cfg.Server.Name),
		zap.String("port", cfg.Server.Port),
	)

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
