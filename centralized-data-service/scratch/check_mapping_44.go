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

	var count int64
	db.Table("cdc_system.mapping_rule_v2").Where("source_object_id = ?", 44).Count(&count)
	fmt.Printf("Source 44 Mapping Rules: %d\n", count)
}
