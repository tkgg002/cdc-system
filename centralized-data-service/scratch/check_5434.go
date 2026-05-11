package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "postgres://gpay_admin:gpay_pass@localhost:5434/postgres?sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var schemas []string
	db.Raw("SELECT schema_name FROM information_schema.schemata").Scan(&schemas)
	fmt.Println("Schemas in 5434:", schemas)
}
