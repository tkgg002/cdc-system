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

	var state string
	db.Table("cdc_system.source_object_registry").Select("provisioning_state").Where("id = ?", 44).Scan(&state)
	fmt.Printf("Source 44 State: %s\n", state)
}
