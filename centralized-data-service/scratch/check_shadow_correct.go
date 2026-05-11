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
		WHERE table_schema = 'shadow_payment_bill_service' AND table_name = 'refund_requests'
	)`).Scan(&exists)
	fmt.Printf("Table 'shadow_payment_bill_service.refund_requests' exists: %v\n", exists)

	var columns []string
	db.Raw("SELECT column_name FROM information_schema.columns WHERE table_name = 'refund_requests' AND table_schema = 'shadow_payment_bill_service'").Scan(&columns)
	fmt.Println("Columns:")
	for _, c := range columns {
		fmt.Println("  -", c)
	}
}
