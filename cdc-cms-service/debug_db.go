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

	var results []map[string]interface{}
	var tables []string
	db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = 'cdc_system'").Scan(&tables)
	fmt.Println("--- tables in cdc_system ---")
	for _, t := range tables {
		fmt.Println(t)
	}

	db.Table("cdc_system.source_object_registry").Find(&results)
	fmt.Println("--- source_object_registry ---")
	for _, r := range results {
		fmt.Printf("ID: %v, SyncEngine: %v, ObjectCode: %v\n", r["id"], r["sync_engine"], r["object_code"])
	}

    db.Table("cdc_system.shadow_binding").Find(&results)
    fmt.Println("\n--- shadow_binding ---")
    for _, r := range results {
        fmt.Printf("ID: %v, SOID: %v, Table: %v\n", r["id"], r["source_object_id"], r["shadow_table"])
    }
}
