package main

import (
	"context"
	"log"

	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=gpay_admin password=gpay_pass dbname=cdc_dw port=5433 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	logger, _ := zap.NewDevelopment()
	svc := persistence.NewSourceObjectV2SyncService(db, logger)

	var entries []model.TableRegistry
	if err := db.Find(&entries).Error; err != nil {
		log.Fatal(err)
	}

	log.Printf("Found %d legacy entries", len(entries))

	for _, entry := range entries {
		log.Printf("Syncing %s...", entry.TargetTable)
		if err := svc.SyncFromLegacy(context.Background(), &entry); err != nil {
			log.Printf("Failed to sync %s: %v", entry.TargetTable, err)
		} else {
			log.Printf("Synced %s", entry.TargetTable)
		}
	}
}
