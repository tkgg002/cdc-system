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

	var binding struct {
		Schema     string `gorm:"column:shadow_schema"`
		Table      string `gorm:"column:shadow_table"`
		BindingCode string `gorm:"column:binding_code"`
	}
	db.Table("cdc_system.shadow_binding").Where("source_object_id = ?", 44).Scan(&binding)
	fmt.Printf("Shadow Binding for src 44: %+v\n", binding)
}
