package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// MasterSwap runs an atomic RENAME swap of a master table. Used by
// Wizard step 11: after a v2 master has been populated and validated,
// swap it into place under the canonical name, retaining the old row as
// <name>_old_<ts> for rollback.
type MasterSwap struct {
	db     *gorm.DB
	logger *zap.Logger
}

func NewMasterSwap(db *gorm.DB, logger *zap.Logger) *MasterSwap {
	return &MasterSwap{db: db, logger: logger}
}

// Swap atomically renames public.<masterName> -> <masterName>_old_<ts>
// and public.<newTableName> -> public.<masterName>, inside one Postgres
// TX. SET LOCAL lock_timeout='3s' bounds the blocking window so a
// long-held ACCESS SHARE can't hang the API; a timeout surfaces as 409.
func (s *MasterSwap) Swap(ctx context.Context, masterName, newTableName, reason string) error {
	if err := validateIdent(masterName); err != nil {
		return fmt.Errorf("invalid master_name: %w", err)
	}
	if err := validateIdent(newTableName); err != nil {
		return fmt.Errorf("invalid new_table_name: %w", err)
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '3s'").Error; err != nil {
			return fmt.Errorf("set lock_timeout: %w", err)
		}

		ts := time.Now().Unix()
		oldName := fmt.Sprintf("%s_old_%d", masterName, ts)

		renameCur := fmt.Sprintf(`ALTER TABLE public."%s" RENAME TO "%s"`, masterName, oldName)
		if err := tx.Exec(renameCur).Error; err != nil {
			return fmt.Errorf("rename current: %w", err)
		}
		renameNew := fmt.Sprintf(`ALTER TABLE public."%s" RENAME TO "%s"`, newTableName, masterName)
		if err := tx.Exec(renameNew).Error; err != nil {
			return fmt.Errorf("rename new: %w", err)
		}

		details, _ := json.Marshal(map[string]string{
			"old_table": oldName,
			"new_table": newTableName,
			"reason":    reason,
		})
		return tx.Exec(
			`INSERT INTO cdc_activity_log
			    (operation, target_table, status, details, triggered_by, started_at, completed_at)
			 VALUES ('master_swap', ?, 'success', ?::jsonb, 'manual', NOW(), NOW())`,
			masterName, string(details),
		).Error
	})
}
