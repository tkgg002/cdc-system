package main

import (
	"context"
	"log"

	"centralized-data-service/config"
	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	logger, _ := zap.NewProduction()
	db, _ := gorm.Open(postgres.Open(cfg.ControlPlaneURL()), &gorm.Config{})
	nc, _ := nats.Connect(cfg.Nats.URL)
	defer nc.Close()

	orch := service.NewProvisioningOrchestrator(db, nc, logger)
	err := orch.Advance(context.Background(), 44, "manual-trigger")
	if err != nil {
		log.Fatalf("Advance failed: %v", err)
	}
	log.Println("Advance triggered for src 44")
}
