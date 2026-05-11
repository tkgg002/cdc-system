package main

import (
	"fmt"

	"centralized-data-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	// Connect to shadow DB
	shadowURL := cfg.ShadowDB.URLs["default"]
	fmt.Println("Connecting to Shadow DB:", shadowURL)
	db, _ := gorm.Open(postgres.Open(shadowURL), &gorm.Config{})

	var tables []string
	db.Raw("SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')").Scan(&tables)
	fmt.Println("All tables in Shadow DB:")
	for _, t := range tables {
		fmt.Println("  -", t)
	}
}
