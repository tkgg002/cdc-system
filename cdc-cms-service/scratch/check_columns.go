package main

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=gpay_admin password=gpay_pass dbname=cdc_dw port=5433 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	var columns []string
	db.Raw("SELECT column_name FROM information_schema.columns WHERE table_name = 'cdc_table_registry'").Scan(&columns)
	fmt.Printf("Columns: %v\n", columns)
}
