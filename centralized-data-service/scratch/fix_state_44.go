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

	var row struct {
		ID                int64  `gorm:"column:id"`
		ProvisioningState string `gorm:"column:provisioning_state"`
		ProvisioningMode  string `gorm:"column:provisioning_mode"`
		LastStepError     string `gorm:"column:last_step_error"`
	}
	db.Table("cdc_system.source_object_registry").Where("id = ?", 44).Scan(&row)
	fmt.Printf("Current State for src 44: %+v\n", row)
}
