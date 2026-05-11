package main

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type MappingRule struct {
	ID           uint   `gorm:"primaryKey"`
	SourceTable  string `gorm:"column:source_table"`
	SourceField  string `gorm:"column:source_field"`
	TargetColumn string `gorm:"column:target_column"`
	IsActive     bool   `gorm:"column:is_active"`
}

func (MappingRule) TableName() string { return "cdc_system.cdc_mapping_rules" }

func main() {
	dsn := "postgres://gpay_admin:gpay_pass@localhost:5433/cdc_dw?sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	var rules []MappingRule
	db.Where("source_table = ?", "export-jobs").Find(&rules)

	if len(rules) == 0 {
		fmt.Println("No rules found in cdc_mapping_rules")
	} else {
		fmt.Printf("Found %d rules in cdc_mapping_rules:\n", len(rules))
		for _, r := range rules {
			fmt.Printf("- %s -> %s (IsActive: %v)\n", r.SourceField, r.TargetColumn, r.IsActive)
		}
	}
}
