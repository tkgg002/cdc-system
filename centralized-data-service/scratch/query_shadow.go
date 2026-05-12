package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=gpay_admin password=gpay_pass dbname=cdc_shadow port=5436 sslmode=disable TimeZone=UTC"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var results []map[string]interface{}
	db.Raw("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'sd_export_jobs'").Find(&results)

	for _, r := range results {
		fmt.Printf("%+v\n", r)
	}
}
