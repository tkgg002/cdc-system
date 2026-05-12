package main

import (
	"context"
	"log"

	"cdc-cms-service/config"
	"cdc-cms-service/internal/bootstrap"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/pkgs/database"

	"go.uber.org/zap"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger, _ := zap.NewProduction()
	db, err := database.NewPostgresConnection(cfg.DB)
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	// 1. Sync connectors
	err = bootstrap.SyncExistingConnectors(context.Background(), db, logger)
	if err != nil {
		log.Fatalf("connector sync failed: %v", err)
	}

	// 2. Sync table registries to V2
	v2Svc := persistence.NewSourceObjectV2SyncService(db, logger)
	var registries []model.TableRegistry
	if err := db.Find(&registries).Error; err != nil {
		log.Fatalf("list registries failed: %v", err)
	}

	for _, reg := range registries {
		err := v2Svc.SyncFromLegacy(context.Background(), &reg)
		if err != nil {
			log.Printf("V2 sync failed for registry %d (%s): %v", reg.ID, reg.TargetTable, err)
			continue
		}
		log.Printf("V2 sync successful for registry %d (%s)", reg.ID, reg.TargetTable)
	}

	log.Println("All syncs successful")
}
