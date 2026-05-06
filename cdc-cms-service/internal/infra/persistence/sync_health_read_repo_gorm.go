// Package persistence — GORM concrete adapters for ports.* interfaces
// and the in-package read ports declared in `internal/app/queries/`.
//
// This file implements queries.SyncHealthReader. Pure aggregate counts
// against cdc_table_registry + cdc_mapping_rules. The 5 counts are the
// wire surface of GET /api/sync/health (legacy handler:
// internal/api/registry_handler.go::SyncHealth).
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type syncHealthReadRepoGorm struct {
	db *gorm.DB
}

// NewSyncHealthReadRepo constructs the GORM-backed adapter for
// queries.SyncHealthReader.
func NewSyncHealthReadRepo(db *gorm.DB) queries.SyncHealthReader {
	return &syncHealthReadRepoGorm{db: db}
}

func (r *syncHealthReadRepoGorm) GetSyncHealth(ctx context.Context) (queries.SyncHealthSnapshot, error) {
	var snap queries.SyncHealthSnapshot
	d := r.db.WithContext(ctx)

	if err := d.Model(&model.TableRegistry{}).Count(&snap.TotalRegistryCMS).Error; err != nil {
		return snap, err
	}
	if err := d.Model(&model.TableRegistry{}).Where("is_active = ?", true).Count(&snap.ActiveTables).Error; err != nil {
		return snap, err
	}
	if err := d.Model(&model.TableRegistry{}).Where("is_table_created = ?", true).Count(&snap.TablesCreated).Error; err != nil {
		return snap, err
	}
	if err := d.Table("cdc_mapping_rules").Where("status = ?", "pending").Count(&snap.PendingMappingRules).Error; err != nil {
		return snap, err
	}
	if err := d.Table("cdc_mapping_rules").Where("status = ?", "approved").Count(&snap.ApprovedMappingRules).Error; err != nil {
		return snap, err
	}
	return snap, nil
}
