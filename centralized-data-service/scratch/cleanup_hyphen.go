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

	err = db.Exec(`DROP TABLE IF EXISTS "shadow_mongo_payment_bill_default"."refund-requests"`).Error
	if err != nil {
		fmt.Println("Error dropping hyphen table:", err)
	} else {
		fmt.Println("Hyphen table dropped.")
	}
}
