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

	tables := []string{"cdc_system.cdc_table_registry", "cdc_system.source_object_registry", "cdc_system.shadow_binding"}
	for _, table := range tables {
		fmt.Printf("\n--- %s ---\n", table)
		var results []map[string]interface{}
		db.Table(table).Find(&results)
		if len(results) == 0 {
			fmt.Println("EMPTY")
			continue
		}
		for _, r := range results {
			fmt.Printf("%+v\n", r)
		}
	}
}
