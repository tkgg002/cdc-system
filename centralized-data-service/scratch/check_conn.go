package main

import (
	"fmt"

	"centralized-data-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	db, _ := gorm.Open(postgres.Open(cfg.ControlPlaneURL()), &gorm.Config{})

	var rows []struct {
		ID             int64  `gorm:"column:id"`
		ConnectionCode string `gorm:"column:connection_code"`
	}
	db.Table("cdc_system.connection_registry").Scan(&rows)
	fmt.Println("Connections:")
	for _, r := range rows {
		fmt.Printf("  - %d: %s\n", r.ID, r.ConnectionCode)
	}

	var src struct {
		SourceConnectionID int64 `gorm:"column:source_connection_id"`
	}
	db.Table("cdc_system.source_object_registry").Where("id = ?", 44).Scan(&src)
	fmt.Printf("Source 44 uses connection ID: %d\n", src.SourceConnectionID)
}
