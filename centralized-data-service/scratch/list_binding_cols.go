package main

import (
	"fmt"

	"centralized-data-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	db, _ := gorm.Open(postgres.Open(cfg.ControlPlaneURL()), &gorm.Config{})

	var columns []struct {
		ColumnName string `gorm:"column:column_name"`
	}
	db.Raw("SELECT column_name FROM information_schema.columns WHERE table_name = 'shadow_binding' AND table_schema = 'cdc_system'").Scan(&columns)
	fmt.Println("Columns in cdc_system.shadow_binding:")
	for _, c := range columns {
		fmt.Println("  -", c.ColumnName)
	}
}
