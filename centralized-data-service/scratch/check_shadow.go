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
	db.Raw("SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_name LIKE '%refund%'").Scan(&tables)
	fmt.Println("Tables matching 'refund':")
	for _, t := range tables {
		fmt.Println("  -", t)
	}

	var columns []string
	db.Raw("SELECT column_name FROM information_schema.columns WHERE table_name = 'refund_requests' AND table_schema = 'shadow_gpay_order'").Scan(&columns)
	fmt.Println("Columns in shadow_gpay_order.refund_requests:")
	for _, c := range columns {
		fmt.Println("  -", c)
	}
}
