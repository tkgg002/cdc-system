package main

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type MappingRuleV2 struct {
	ID              int64  `gorm:"primaryKey"`
	SourceObjectID  int64  `gorm:"column:source_object_id"`
	SourceField     string `gorm:"column:source_field"`
	TargetColumn    string `gorm:"column:target_column"`
	IsActive        bool   `gorm:"column:is_active"`
	Status          string `gorm:"column:status"`
}

func (MappingRuleV2) TableName() string {
	return "cdc_system.mapping_rule_v2"
}

func main() {
	dsn := "postgres://gpay_admin:gpay_pass@localhost:5433/cdc_dw?sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	var rules []MappingRuleV2
	db.Where("source_object_id = ?", 35).Find(&rules)

	if len(rules) == 0 {
		fmt.Println("No rules found for source_object_id 35")
	} else {
		fmt.Printf("Found %d rules:\n", len(rules))
		for _, r := range rules {
			fmt.Printf("- %s -> %s (IsActive: %v, Status: %s)\n", r.SourceField, r.TargetColumn, r.IsActive, r.Status)
		}
	}
}
