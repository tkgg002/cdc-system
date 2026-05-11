package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "postgres://gpay_admin:gpay_pass@localhost:5433/cdc_dw?sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var results []map[string]interface{}
	db.Table("cdc_system.shadow_binding").Find(&results)
	for _, r := range results {
		fmt.Printf("Binding: %+v\n", r)
	}
}
