package main

import (
	"fmt"

	"centralized-data-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	db, _ := gorm.Open(postgres.Open(cfg.ShadowDB.URLs["default"]), &gorm.Config{})

	var exists bool
	db.Raw(`SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = 'shadow_payment_bill_service' AND table_name = 'refund-requests'
	)`).Scan(&exists)
	fmt.Printf("Table 'shadow_payment_bill_service.refund-requests' exists: %v\n", exists)
}
