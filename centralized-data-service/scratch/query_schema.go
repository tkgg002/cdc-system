package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=gpay_admin password=gpay_pass dbname=cdc_dw port=5433 sslmode=disable TimeZone=UTC"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var results []map[string]interface{}
	db.Table("cdc_system.shadow_binding").
		Select("shadow_table, shadow_schema, source_object_id").
		Find(&results)

	for _, r := range results {
		fmt.Printf("%+v\n", r)
	}
}
