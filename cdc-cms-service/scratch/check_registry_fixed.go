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
	db.Raw("SELECT id, target_table, source_db, source_table FROM cdc_table_registry WHERE target_table = 'sd_export_jobs'").Scan(&results)
	for _, r := range results {
		fmt.Printf("ID: %v, Target: %v, SourceDB: %v, SourceTable: %v\n", r["id"], r["target_table"], r["source_db"], r["source_table"])
	}
}
