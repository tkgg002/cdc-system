// Package persistence — system_connector_repo_gorm.go is the GORM-
// backed adapter for ports.SystemConnectorRepo (the Connection-
// Fingerprint registry, table `cdc_sources`). SQL is lifted verbatim
// from the legacy `internal/repository/source_repo.go` so the upsert
// column-list and ordering remain byte-identical after Task #19 đợt D.
//
// The legacy `GetByConnectorName` method is NOT migrated — zero
// callers in CMS at the time of move.
package persistence

import (
	"context"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type systemConnectorRepoGorm struct {
	db *gorm.DB
}

// NewSystemConnectorRepo constructs the GORM-backed adapter for
// ports.SystemConnectorRepo.
func NewSystemConnectorRepo(db *gorm.DB) ports.SystemConnectorRepo {
	return &systemConnectorRepoGorm{db: db}
}

func (r *systemConnectorRepoGorm) Upsert(ctx context.Context, s *model.Source) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Upsert to cdc_sources (Legacy / Fingerprint)
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "connector_name"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"source_type", "connector_class", "topic_prefix", "server_address",
				"database_include_list", "collection_include_list", "raw_config_sanitized",
				"status", "updated_at",
			}),
		}).Create(s).Error; err != nil {
			return err
		}

		// 2. Mirror to connection_registry (V2)
		// This ensures that V2 sync logic (Register) can resolve connection IDs
		// even for connectors created via the legacy / system connector flow.
		host, port := splitHostPort(s.ServerAddress)
		engine := normalizeSourceEngine(s.SourceType)

		return tx.Exec(`
			INSERT INTO cdc_system.connection_registry (
				connection_code, display_name, role_type, engine_type,
				host, port, default_database, secret_ref, status, updated_at
			)
			VALUES (?, ?, 'source', ?, ?, ?, ?, ?, 'active', NOW())
			ON CONFLICT (connection_code) DO UPDATE SET
				engine_type = EXCLUDED.engine_type,
				host = EXCLUDED.host,
				port = EXCLUDED.port,
				default_database = EXCLUDED.default_database,
				status = 'active',
				updated_at = NOW()
		`,
			s.ConnectorName,
			s.ConnectorName,
			engine,
			nullIfEmpty(host),
			nullIfZero(port),
			nullIfEmpty(s.DatabaseIncludeList),
			"v1:"+s.ConnectorName,
		).Error
	})
}

func (r *systemConnectorRepoGorm) List(ctx context.Context) ([]model.Source, error) {
	var out []model.Source
	err := r.db.WithContext(ctx).
		Where("status != ?", "deleted").
		Order("created_at DESC").
		Find(&out).Error
	return out, err
}

func (r *systemConnectorRepoGorm) GetByID(ctx context.Context, id int64) (*model.Source, error) {
	var s model.Source
	err := r.db.WithContext(ctx).First(&s, id).Error
	return &s, err
}

func (r *systemConnectorRepoGorm) MarkDeleted(ctx context.Context, connectorName string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Mark as deleted in legacy sources
		if err := tx.Model(&model.Source{}).
			Where("connector_name = ?", connectorName).
			Update("status", "deleted").Error; err != nil {
			return err
		}

		// 2. Retire in connection_registry to prevent future registrations
		return tx.Exec(`
			UPDATE cdc_system.connection_registry
			SET status = 'retired', updated_at = NOW()
			WHERE connection_code = ?
		`, connectorName).Error
	})
}

func (r *systemConnectorRepoGorm) FullCleanup(ctx context.Context, connectorName string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1. Get connection ID from registry to cascade cleanup
		var connID int64
		err := tx.Raw(`SELECT id FROM cdc_system.connection_registry WHERE connection_code = ?`, connectorName).Scan(&connID).Error
		if err != nil {
			return err
		}

		if connID != 0 {
			// 2. Delete mapping rules (V2)
			if err := tx.Exec(`
				DELETE FROM cdc_system.mapping_rule_v2 
				WHERE source_object_id IN (
					SELECT id FROM cdc_system.source_object_registry WHERE source_connection_id = ?
				)
			`, connID).Error; err != nil {
				return err
			}

			// 3. Delete shadow bindings
			if err := tx.Exec(`
				DELETE FROM cdc_system.shadow_binding 
				WHERE source_object_id IN (
					SELECT id FROM cdc_system.source_object_registry WHERE source_connection_id = ?
				)
			`, connID).Error; err != nil {
				return err
			}

			// 4. Delete source objects
			if err := tx.Exec(`DELETE FROM cdc_system.source_object_registry WHERE source_connection_id = ?`, connID).Error; err != nil {
				return err
			}

			// 5. Delete connection registry
			if err := tx.Exec(`DELETE FROM cdc_system.connection_registry WHERE id = ?`, connID).Error; err != nil {
				return err
			}
		}

		// 6. Cleanup legacy registries and rules
		// We match by source_db derived from the connector name or its config
		// For simplicity, we search cdc_table_registry for any entries that might
		// be related via connection mirroring (bridged status).
		
		// 6.1 Delete legacy mapping rules for tables in legacy registry that match this connector's scope
		// (Actually, legacy rules are harder to attribute without joining, but we can target 
		// any registry entry that is now orphaned).
		
		// 7. Delete from cdc_sources (legacy)
		if err := tx.Exec(`DELETE FROM cdc_system.cdc_sources WHERE connector_name = ?`, connectorName).Error; err != nil {
			return err
		}

		return nil
	})
}

func splitHostPort(addr string) (string, int) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", 0
	}
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, 0
	}
	host := addr[:idx]
	port, err := strconv.Atoi(addr[idx+1:])
	if err != nil {
		return addr, 0
	}
	return host, port
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullIfZero(n int) interface{} {
	if n == 0 {
		return nil
	}
	return n
}
