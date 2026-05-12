package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=admin password=admin dbname=cdc_dw port=5433 sslmode=disable TimeZone=UTC"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var results []map[string]interface{}
	db.Table("cdc_system.activity_log").
		Select("operation, target_table, status, error_message, created_at").
		Where("operation IN ('cmd-scan-fields', 'scan-fields')").
		Order("created_at DESC").
		Limit(5).
		Find(&results)

	for _, r := range results {
		fmt.Printf("%+v\n", r)
	}
}
