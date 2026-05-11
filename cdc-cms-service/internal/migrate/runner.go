// Package migrate applies the embedded SQL files in cdc-cms-service/migrations
// against the control-plane Postgres at service startup.
//
// Why in-process instead of a separate `make migrate`:
//   - Developers running `make run` (and operators rolling out the binary)
//     get a self-bootstrapping service: empty cdc_system → fully populated
//     without manual psql steps.
//   - Production safety: a pg_advisory_lock pinned to a dedicated
//     connection guards against parallel migrators when multiple replicas
//     roll at once.
//   - Tracked in `cdc_system.schema_migrations` (filename minus `.sql`)
//     so re-applying is a no-op.
//
// search_path subtlety:
//   The runtime DSN sets `search_path=cdc_system,public` so GORM models
//   with bare TableName() resolve to cdc_system. But the migrations were
//   authored under the default `public, "$user"` search_path — files
//   006/008 create unqualified `cdc_activity_log`/`failed_sync_logs`
//   expecting them in `public`, then 010 creates the partitioned
//   `cdc_system.*` counterparts. With the runtime DSN's search_path, those
//   unqualified tables would land in `cdc_system` and collide with 010.
//   Inside each migration transaction we therefore SET LOCAL search_path
//   back to public+$user so the SQL behaves exactly as it would under a
//   default-configured psql session.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"cdc-cms-service/migrations"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Picked from /dev/urandom + masked to fit int64. Namespacing the advisory
// lock keeps unrelated callers from blocking us.
const advisoryLockKey int64 = 0x4344_4D49_4752_8042

const trackerTable = "cdc_system.schema_migrations"

// Migration files may begin with BEGIN; and end with COMMIT; — strip them
// so we can wrap (body + tracker INSERT) inside one atomic transaction.
var (
	reLeadingBegin   = regexp.MustCompile(`(?is)\A\s*BEGIN\s*;\s*`)
	reTrailingCommit = regexp.MustCompile(`(?is)\s*COMMIT\s*;\s*\z`)
)

// Run applies every embedded migration that hasn't been recorded yet.
// Idempotent: a second call is a no-op (advisory lock + tracker table).
func Run(gdb *gorm.DB, logger *zap.Logger) error {
	sqlDB, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("migrate: get *sql.DB: %w", err)
	}

	// Pin a single connection so (a) the advisory lock and the migration
	// transactions land on the same backend, and (b) SET LOCAL semantics
	// behave predictably regardless of pool churn.
	ctx := context.Background()
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migrate: pin conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("migrate: acquire advisory lock: %w", err)
	}
	defer func() {
		if _, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockKey); unlockErr != nil {
			logger.Warn("migrate: release advisory lock", zap.Error(unlockErr))
		}
	}()

	if err := ensureTracker(ctx, conn); err != nil {
		return err
	}

	applied, err := loadApplied(ctx, conn)
	if err != nil {
		return err
	}

	files, err := listMigrationFiles()
	if err != nil {
		return err
	}

	pending := 0
	for _, name := range files {
		version := strings.TrimSuffix(name, ".sql")
		if applied[version] {
			continue
		}
		body, err := fs.ReadFile(migrations.Files, name)
		if err != nil {
			return fmt.Errorf("migrate: read %s: %w", name, err)
		}
		if err := applyOne(ctx, conn, version, string(body), logger); err != nil {
			return err
		}
		pending++
	}

	logger.Info("migrations done",
		zap.Int("total_files", len(files)),
		zap.Int("applied_now", pending),
		zap.Int("already_applied", len(files)-pending),
	)
	return nil
}

func ensureTracker(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS cdc_system`); err != nil {
		return fmt.Errorf("migrate: ensure cdc_system schema: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+trackerTable+` (
			version    VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		)`); err != nil {
		return fmt.Errorf("migrate: create tracker table: %w", err)
	}
	return nil
}

func loadApplied(ctx context.Context, conn *sql.Conn) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version FROM "+trackerTable)
	if err != nil {
		return nil, fmt.Errorf("migrate: read tracker: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("migrate: scan tracker: %w", err)
		}
		out[v] = true
	}
	return out, rows.Err()
}

func listMigrationFiles() ([]string, error) {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("migrate: read embedded fs: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

func applyOne(ctx context.Context, conn *sql.Conn, version, body string, logger *zap.Logger) error {
	body = reLeadingBegin.ReplaceAllString(body, "")
	body = reTrailingCommit.ReplaceAllString(body, "")

	logger.Info("applying migration", zap.String("version", version))

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: begin %s: %w", version, err)
	}
	// Migrations were authored under the Postgres default search_path
	// (public, "$user"). Override the connection-level search_path
	// (set by the DSN for GORM runtime) for the duration of this
	// transaction so unqualified DDL lands in `public` as intended.
	if _, err := tx.ExecContext(ctx, `SET LOCAL search_path TO public, "$user"`); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("migrate: SET LOCAL search_path %s: %w", version, err)
	}
	if _, err := tx.ExecContext(ctx, body); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("migrate: apply %s: %w", version, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+trackerTable+" (version) VALUES ($1)", version); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("migrate: record %s: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: commit %s: %w", version, err)
	}
	logger.Info("migration applied", zap.String("version", version))
	return nil
}
