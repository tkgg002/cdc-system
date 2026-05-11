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

	err = db.Exec(`CREATE TABLE IF NOT EXISTS "shadow_mongo_payment_bill_default"."refund_requests" (id text primary key)`).Error
	if err != nil {
		fmt.Println("Error creating underscore table:", err)
	} else {
		fmt.Println("Underscore table created (or exists).")
	}

	var tables []string
	db.Raw("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'shadow_mongo_payment_bill_default'").Scan(&tables)
	fmt.Println("Tables now:", tables)
}
