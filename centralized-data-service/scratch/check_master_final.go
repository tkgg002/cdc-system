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

	var b struct {
		MasterTable       string `gorm:"column:master_table"`
		PhysicalTableFQN  string `gorm:"column:physical_table_fqn"`
	}
	db.Table("cdc_system.master_binding").Where("source_object_id = ?", 44).Order("id DESC").Limit(1).Scan(&b)
	fmt.Printf("Table: %s, FQN: %s\n", b.MasterTable, b.PhysicalTableFQN)
}
