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

	var tables []string
	db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = 'shadow_mongo_payment_bill_default'").Scan(&tables)
	fmt.Println("Tables in shadow_mongo_payment_bill_default:", tables)
}
