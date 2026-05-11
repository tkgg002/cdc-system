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
		SchemaName string `gorm:"column:schema_name"`
		TableName  string `gorm:"column:table_name"`
	}
	db.Table("cdc_system.shadow_binding").Where("source_object_id = ?", 44).Order("id DESC").Limit(1).Scan(&b)
	fmt.Printf("ShadowBinding -> Schema: %s, Table: %s\n", b.SchemaName, b.TableName)
}
