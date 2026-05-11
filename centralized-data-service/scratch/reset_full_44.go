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

	fmt.Println("Cleaning up bindings for src 44...")
	db.Exec("DELETE FROM cdc_system.shadow_binding WHERE source_object_id = ?", 44)
	db.Exec("DELETE FROM cdc_system.master_binding WHERE source_object_id = ?", 44)

	fmt.Println("Resetting state to draft for src 44...")
	db.Table("cdc_system.source_object_registry").Where("id = ?", 44).Updates(map[string]interface{}{
		"provisioning_state": "draft",
		"provisioning_mode":  "auto",
		"updated_at":         gorm.Expr("NOW()"),
	})
}
