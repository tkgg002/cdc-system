package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "postgres://gpay_admin:gpay_pass@localhost:5436/cdc_dw?sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	var tables []string
	db.Raw("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'shadow_mongo_payment_bill_default'").Scan(&tables)
	fmt.Println("Tables in shadow_mongo_payment_bill_default (SHADOW DB 5436):", tables)
}
