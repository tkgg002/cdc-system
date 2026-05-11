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

	var schemas []string
	db.Raw("SELECT schema_name FROM information_schema.schemata").Scan(&schemas)
	fmt.Println("Schemas:", schemas)

	var tables []struct {
		Schema string `gorm:"column:table_schema"`
		Name   string `gorm:"column:table_name"`
	}
	db.Raw("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE '%refund%'").Scan(&tables)
	fmt.Println("Refund Tables:", tables)
}
