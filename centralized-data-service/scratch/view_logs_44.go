package main

import (
	"encoding/json"
	"fmt"

	"centralized-data-service/config"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, _ := config.NewConfig()
	db, _ := gorm.Open(postgres.Open(cfg.ControlPlaneURL()), &gorm.Config{})

	var logRaw string
	db.Raw("SELECT provisioning_step_log FROM cdc_system.source_object_registry WHERE id = ?", 44).Scan(&logRaw)
	
	var logs []map[string]any
	json.Unmarshal([]byte(logRaw), &logs)

	fmt.Println("Step Logs for src 44:")
	for _, l := range logs {
		fmt.Printf("  - %s: %v (Success: %v, Err: %v)\n", l["step"], l["to_state"], l["success"], l["error"])
	}
}
